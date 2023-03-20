"""Running the ingestion and checking the status of the flow"""
import json
import importlib
import logging
import os
import nipyapi
import argparse
from nifi.environment import config

def flatten_json(nested_json):
    out = {}

    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x

    flatten(nested_json)
    return out

def _data_validation(file_path):
    empty_values = []
    flag = 0
    with open(file_path, 'r') as file:
        data = json.load(file)
    flatten_data = flatten_json(data)
    for value in flatten_data:
        if not flatten_data[value]:
            empty_values.append(value)

    if len(empty_values) > 0:
        flag = 1
        logging.error(f"Enter values for {empty_values}")
    return flag

def _get_pg_id():
    """retrieving processor group id"""
    obj = nipyapi.nifi.apis.resources_api.ResourcesApi(api_client=None)
    response=obj.get_resources()
    for i in range (len(response.resources)):
        if(response.resources[i].name)== "User_input_for_db_ingestion":
            identifier = response.resources[i].identifier
    return identifier[16:52]

def _get_processor_id(pg_id):
    """retrieving GenerateFlowFile processor id"""
    obj = nipyapi.nifi.apis.process_groups_api.ProcessGroupsApi(api_client=None)
    response=obj.get_connections(id=pg_id)
    for i in range(len(response.connections)):
        if(response.connections[i].component.source.name) == "GenerateFlowFile":
            processor_id = response.connections[i].component.source.id
    return processor_id

def _get_processor_configs(processor_id):
    """retrieving processor configs"""
    obj = nipyapi.nifi.apis.processors_api.ProcessorsApi(api_client=None)
    response = obj.get_processor(id=processor_id)
    version = response.revision.version
    state = response.component.state
    processor_config_list = [version, state]
    return processor_config_list

def _get_flow_status():

    if (_update_run_status_processor.destination == "s3"
            or _update_run_status_processor.destination == "S3"):

        module = importlib.import_module("nifi.environment.data_ingestion.rdbms_to_s3_flow_status",
                                         "environment")
        func = getattr(module, 'get_flow_status')
        func()
    else:
        module = importlib.import_module("nifi.environment.data_ingestion.rdbms_to_hdfs_flow_status",
                                         "environment")
        func = getattr(module, 'get_flow_status')
        func()


def _update_run_status_processor(file_path, processor_id, processor_config_list):
    """Updating the run status of a processor"""
    version = processor_config_list[0]
    with open(file_path, "r") as file:
        file_data = json.load(file)
    _update_run_status_processor.destination = file_data['storage'][
        'destination_type']

    if file_data["source"]["source_type"] == "RDBMS":
        source = file_data['source']['database']['db_type']
    else:
        source = file_data["source"]["source_type"]

    print(f"starting the Data ingestion from {source} to"
          f" {_update_run_status_processor.destination}")
    file_dir = os.path.dirname(os.path.abspath(__file__))
    config_template_path = os.path.join(file_dir, 'run_templates', 'initialize_processor.json')
    with open(config_template_path, "r") as file:
        data = json.load(file)
    data["revision"]["version"] = version
    data["component"]["id"] = processor_id
    data["component"]["config"]["properties"]["generate-ff-custom-text"] = json.dumps(file_data)
    obj = nipyapi.nifi.apis.processors_api.ProcessorsApi(api_client=None)
    obj.update_processor(id=processor_id, body=data)

def _stop_processor(processor_id, processor_config_list):
    """Stopping the processor """
    version = processor_config_list[0]
    file_dir = os.path.dirname(os.path.abspath(__file__))
    config_template_path = os.path.join(file_dir, 'run_templates', 'stop_prcessor.json')
    with open(config_template_path, "r") as file:
        data = json.load(file)
    data["revision"]["version"] = version
    data["component"]["id"] = processor_id
    obj = nipyapi.nifi.apis.processors_api.ProcessorsApi(api_client=None)
    obj.update_processor(id=processor_id, body=data)

def _get_source(file_path):

    with open(file_path, 'r') as file:
        data = json.load(file)
    return data["source"]["source_type"]

def _set_ftp_sftp_config(source, path):

    if source == "SFTP":
        module = importlib.import_module("nifi.environment.data_ingestion.sftp",
                                         "environment")
        func = getattr(module, 'set_configs')
        func(path)
    else:
        module = importlib.import_module("nifi.environment.data_ingestion.ftp",
                                         "environment")
        func = getattr(module, 'set_configs')
        func()

def main():
    """calling all the functions"""
    parser = argparse.ArgumentParser()
    parser.add_argument('--f', type=str, required=True)
    parser.add_argument('--c', type=str, required=True)
    args = parser.parse_args()
    path = args.f
    config_path = args.c
    config.configuration(config_path)
    flag = _data_validation(path)

    if flag == 0:
        source = _get_source(path)
        _set_ftp_sftp_config(source, path)
        pg_id = _get_pg_id()
        processor_id = _get_processor_id(pg_id)
        processor_config_list = _get_processor_configs(processor_id)
        state = processor_config_list[1]
        if state == "STOPPED":
            _update_run_status_processor(path, processor_id, processor_config_list)
            _get_flow_status()
        else:
            _stop_processor(processor_id, processor_config_list)
            processor_config_list = _get_processor_configs(processor_id)
            _update_run_status_processor(path, processor_id, processor_config_list)
            _get_flow_status()

if __name__ == "__main__":

    main()
