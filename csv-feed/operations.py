"""
Copyright start
MIT License
Copyright (c) 2024 Fortinet Inc
Copyright end
"""
import csv
import io
from os.path import join

import polars as pl
import requests
from connectors.core.connector import get_logger, ConnectorError
from connectors.cyops_utilities.builtins import download_file_from_cyops
from integrations.crudhub import make_request

try:
    from integrations.crudhub import trigger_ingest_playbook
except:
    # ignore. lower FSR version
    pass

logger = get_logger('csv-feed')


class CSVFeed:
    def __init__(self, config):
        self.server_url = config.get('server_url')
        if not (self.server_url.startswith('https://') or self.server_url.startswith('http://')):
            self.server_url = 'https://' + self.server_url
        self.server_url = self.server_url.strip('/')
        self.verify_ssl = config.get('verify_ssl')

    def make_request_api(self, endpoint, method='GET', data=None, params=None, files=None):
        try:
            url = self.server_url + endpoint
            logger.info('Executing url {}'.format(url))

            # CURL UTILS CODE
            try:
                from connectors.debug_utils.curl_script import make_curl
                make_curl(method, endpoint, params=params, data=data, verify_ssl=self.verify_ssl)
            except Exception as err:
                logger.error(f"Error in curl utils: {str(err)}")

            response = requests.request(method, url, params=params, files=files, data=data,
                                        verify=self.verify_ssl)
            if response.ok:
                logger.info('Successfully got response for url {}'.format(url))
                if method.upper() == 'DELETE':
                    return response
                else:
                    return response
            elif response.status_code == 400:
                error_response = response.json()
                raise ConnectorError(error_response)
            elif response.status_code == 401:
                error_response = response.json()
                raise ConnectorError(error_response)
            elif response.status_code == 404:
                error_response = response.json()
                raise ConnectorError(error_response)
            else:
                logger.error(response.json())
        except requests.exceptions.SSLError:
            logger.error('SSL certificate validation failed')
            raise ConnectorError('SSL certificate validation failed')
        except requests.exceptions.ConnectTimeout:
            logger.error('The request timed out while trying to connect to the server')
            raise ConnectorError('The request timed out while trying to connect to the server')
        except requests.exceptions.ReadTimeout:
            logger.error('The server did not send any data in the allotted amount of time')
            raise ConnectorError('The server did not send any data in the allotted amount of time')
        except requests.exceptions.ConnectionError:
            logger.error('Invalid endpoint or credentials')
            raise ConnectorError('Invalid endpoint or credentials')
        except Exception as err:
            logger.error(str(err))
            raise ConnectorError(str(err))
        raise ConnectorError(response.text)


def _check_health(config):
    try:
        if config.get('input') == "Server URL":
            cf = CSVFeed(config)
            response = cf.make_request_api(endpoint="")
            return response.ok
        else:
            return True
    except Exception as err:
        logger.error(str(err))
        raise ConnectorError(str(err))


def get_feeds_from_url(config: dict, params: dict, **kwargs):
    try:
        params = _build_payload(params)
        input_type = config.get('input')

        if input_type == "Server URL":
            return _process_csv_from_server_url(config, params)
        else:
            raise ConnectorError(
                "Server URL not provided in Configuration. This action doesn't work with Attachment/File IRI option in configuration")

    except Exception as err:
        logger.error(str(err))
        raise ConnectorError(str(err))


def get_feeds_from_attachment(config: dict, params: dict, **kwargs):
    try:
        params = _build_payload(params)
        input_type = config.get('input')
        process_response_as = params.get('process_response_as')

        if input_type == "Server URL":
            raise ConnectorError(
                "Attachment/File IRI not provided in Configuration. This action doesn't work with Server URL option in configuration")
        else:
            list_feeds = _process_csv_from_attachment_file(config, params)
            if process_response_as == "Return as JSON":
                return list_feeds
            else:
                logger.info(f"{list_feeds}")
                trigger_ingest_playbook(list_feeds, params.get("create_pb_id"), parent_env=kwargs.get('env', {}),
                                        batch_size=2000)
                return {"message": "Successfully triggered playbooks to create feed records"}

    except Exception as err:
        logger.error(str(err))
        raise ConnectorError(str(err))


def _process_csv_from_server_url(config: dict, params: dict):
    cf = CSVFeed(config)
    response = cf.make_request_api(endpoint="", method='GET', params=params)
    csv_data = response.text

    expected_columns = params.pop('col_name')
    delimiter = params.pop('delimiter', ',')
    lines = csv_data.splitlines()

    header_found = False
    header_line = None
    data_lines = []

    for line in lines:
        if line.startswith('#'):
            if not header_found and all(col in line for col in expected_columns):
                header_line = line.lstrip('#').strip()
                header_found = True
        else:
            if not line.startswith('#'):
                data_lines.append(line)

    if not header_found and data_lines:
        potential_header = data_lines[0].strip('"').split(f'"{delimiter}"')
        if all(col in potential_header for col in expected_columns):
            header_line = f'{delimiter}'.join(potential_header)
            data_lines = data_lines[1:]

    data_str = '\n'.join(data_lines)
    data_io = io.StringIO(data_str)

    if header_line:
        header_io = io.StringIO(header_line + '\n' + data_str)
        df = pl.read_csv(header_io, separator=delimiter, n_rows=params.get('n_rows', 100), columns=expected_columns,
                         ignore_errors=True)
    else:
        df = pl.read_csv(data_io, separator=delimiter, has_header=False, n_rows=params.get('n_rows', 100),
                         ignore_errors=True)

    return _format_result(df)


def _process_csv_from_attachment_file(config: dict, params: dict):
    file_iri = __handle_params(params, params.get('value'))
    file_path = join('/tmp', download_file_from_cyops(file_iri)['cyops_file_path'])
    delimiter = params.pop('delimiter', ',')
    n_rows = params.get('n_rows', 100)
    expected_columns = params.pop('col_name', "")

    has_headers = __check_if_csv_has_header(file_path)

    if expected_columns:
        df = pl.read_csv(file_path, separator=delimiter, n_rows=n_rows, columns=expected_columns,
                         has_header=has_headers)
        logger.info(f"Inside expected_columns")
    else:
        logger.info(f"Inside Not expected_columns")
        df = pl.read_csv(file_path, separator=delimiter, n_rows=n_rows, has_header=has_headers)

    return _format_result(df)


def _format_result(df) -> list:
    rows_as_dicts = [row for row in df.iter_rows(named=True)]
    return rows_as_dicts


def __check_if_csv_has_header(filepath) -> bool:
    sniffer = csv.Sniffer()
    res_whole = sniffer.has_header(open(filepath).read(2048))
    return res_whole


def __handle_params(params, file_param):
    value = str(file_param)
    input_type = params.get('input')
    try:
        if isinstance(value, bytes):
            value = value.decode('utf-8')
        if input_type == 'Attachment IRI':
            if not value.startswith('/api/3/attachments/'):
                value = '/api/3/attachments/{0}'.format(value)
            attachment_data = make_request(value, 'GET')
            file_iri = attachment_data['file']['@id']
            file_name = attachment_data['file']['filename']
            logger.info('file id = {0}, file_name = {1}'.format(file_iri, file_name))
            return file_iri
        elif input_type == 'File IRI':
            if value.startswith('/api/3/files/'):
                return value
            else:
                raise ConnectorError('Invalid File IRI {0}'.format(value))
    except Exception as err:
        logger.info('handle_params(): Exception occurred {0}'.format(err))
        raise ConnectorError(
            'Requested resource could not be found with input type "{0}" and value "{1}"'.format(input_type,
                                                                                                 value.replace(
                                                                                                     '/api/3/attachments/',
                                                                                                     '')))


def _build_payload(params: dict, options_dict: dict = {}) -> dict:
    if params.get('other_fields'):
        params.update(params.pop('other_fields'))

    if params.get('col_name') and isinstance(params.get('col_name'), str):
        col_name = params.pop('col_name')
        col_list = [col.strip() for col in col_name.split(',')]
        params.update({'col_name': col_list})

    return {key: options_dict.get(val, val) if isinstance(val, str) else val for key, val in params.items() if
            isinstance(val, (bool, int)) or val}


operations = {
    "get_feeds_from_url": get_feeds_from_url,
    "get_feeds_from_attachment": get_feeds_from_attachment
}