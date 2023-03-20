import time

import nipyapi
import json
import os

def _get_pg_id():
    obj = nipyapi.nifi.apis.resources_api.ResourcesApi(api_client=None)
    response = obj.get_resources()
    for i in range(len(response.resources)):
        if response.resources[i].name == "sftp_ingestion":
            identifier = response.resources[i].identifier
            return identifier[16:52]

def _get_processor_id(pg_id):
    obj = nipyapi.nifi.apis.process_groups_api.ProcessGroupsApi(api_client=None)
    response = obj.get_connections(id=pg_id)

    for i in range(len(response.connections)):
        if response.connections[i].component.source.name == "FetchSFTP":
            processor_id = response.connections[i].component.source.id
            return processor_id

def _get_processor_configs(processor_id):
    obj3 = nipyapi.nifi.apis.processors_api.ProcessorsApi(api_client=None)
    response = obj3.get_processor(id=processor_id)
    version = response.revision.version
    return version

def _update_run_status_processor(file_path, processor_id, version):

    with open(file_path, 'r') as file:
        data = json.load(file)

    file_dir = os.path.dirname(os.path.abspath(__file__))
    sftp_template_path = os.path.join(file_dir, '..', 'properties_templates',
                                       'update_sftp_processor_config.json')
    with open(sftp_template_path) as file:
        sftp_config = json.load(file)

    sftp_config["component"]["id"] = processor_id
    sftp_config["revision"]["version"] = version
    sftp_config["component"]["config"]["properties"]["Password"] = "${file_ingestion_password}"

    if "Private_Key_Passphrase" in data["source"]["sftp"].keys():
        sftp_config["component"]["config"]["properties"]["Private Key Passphrase"] = data["source"]["sftp"][
            "Private_Key_Passphrase"]

    obj = nipyapi.nifi.apis.processors_api.ProcessorsApi(api_client=None)
    obj.update_processor(id=processor_id, body=sftp_config)

def _stop_processor(processor_id , version):
    file_dir = os.path.dirname(os.path.abspath(__file__))
    sftp_template_path = os.path.join(file_dir, '..', 'properties_templates',
                                      'stop_referencing component.json')
    with open(sftp_template_path) as file:
        sftp_config = json.load(file)

    sftp_config["component"]["id"] = processor_id
    sftp_config["revision"]["version"] = version


    obj = nipyapi.nifi.apis.processors_api.ProcessorsApi(api_client=None)
    obj.update_processor(id=processor_id, body=sftp_config)


def _get_state(processor_id):
    obj3 = nipyapi.nifi.apis.processors_api.ProcessorsApi(api_client=None)
    response = obj3.get_processor(id=processor_id)
    state = response.component.state
    return state


def set_configs(file_path):
    pg_id = _get_pg_id()
    processor_id = _get_processor_id(pg_id)
    state = _get_state(processor_id)
    version = _get_processor_configs(processor_id)

    if state == "STOPPED":
        _update_run_status_processor(file_path, processor_id, version)

    else:
        _stop_processor(processor_id, version)
        state = _get_state(processor_id)

        while True:
            if state == "STOPPING":
                time.sleep(5)

            else:
                version = _get_processor_configs(processor_id)
                _update_run_status_processor(file_path,processor_id, version)
                break


