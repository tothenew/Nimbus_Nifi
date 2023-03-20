"""utility file"""
import json
import logging
import os
import nipyapi

def check_controller_service_state(controller_services_id):

    """ This function will Check the status of controller services
    :param int controller_services_id: Id of a controller service"""
    
    obj = nipyapi.nifi.apis.controller_services_api.ControllerServicesApi(api_client=None)
    response = obj.get_controller_service(id=controller_services_id)
    error = response.component.validation_errors
    state = response.component.validation_status
    run_status = response.status.run_status
    state_list = [error, state, run_status]
    return state_list

def get_controller_service_referencing_component(controller_services_id):

    """ This function will Retrieve the controller services referencing components
    :param int controller_services_id: Id of a controller service"""
    
    referencing_component_list = []
    obj = nipyapi.nifi.apis.controller_services_api.ControllerServicesApi(api_client=None)
    resp = obj.get_controller_service_references(id=controller_services_id)
    for values in range(len(resp.controller_service_referencing_components)):
        referencing_component_list.append(resp.controller_service_referencing_components[values].
                                          revision.version)
        referencing_component_list.append(resp.controller_service_referencing_components[values].
                                          component.id)
    return referencing_component_list

def start_referencing_components(referencing_component_list):

    """ This function will Start the referencing component
    :param list referencing_component_list: list of ids of referencing processors"""
    
    obj = nipyapi.nifi.apis.processors_api.ProcessorsApi(api_client=None)
    file_dir = os.path.dirname(os.path.abspath(__file__))
    config_template_path = os.path.join(file_dir, 'properties_templates',
                                        'start_referencing_component.json')
    with open(config_template_path, 'r') as file:
        data = json.load(file)
    for value in referencing_component_list:
        if isinstance(value, int):
            data["revision"]["version"] = value
        else:
            data["component"]["id"] = value
            obj.update_processor(id=value, body=data)

def stop_referencing_components(referencing_component_list):

    """This function will Stop the referencing component
    :param list referencing_component_list: list of ids of referencing processors"""
    
    obj = nipyapi.nifi.apis.processors_api.ProcessorsApi(api_client=None)
    file_dir = os.path.dirname(os.path.abspath(__file__))
    config_template_path = os.path.join(file_dir, 'properties_templates',
                                        'stop_referencing component.json')
    with open(config_template_path, 'r') as file:
        data = json.load(file)
    for value in referencing_component_list:
        if isinstance(value, int):
            data["revision"]["version"] = value
        else:
            data["component"]["id"] = value
            obj.update_processor(id=value, body=data)

def stop_controller_service(controller_services_id, version):

    """ This function will Stop the controller service 
    :param int controller_services_id: id of a controller service 
    :param int version: latest version of controller service"""
    
    file_dir = os.path.dirname(os.path.abspath(__file__))
    config_template_path = os.path.join(file_dir,'properties_templates',
                                        'stop_controllerservices.json')
    with open(config_template_path, 'r') as file:
        data = json.load(file)
    data['component']['id'] = controller_services_id
    data['revision']['version'] = version
    obj = nipyapi.nifi.apis.controller_services_api.ControllerServicesApi(api_client=None)
    obj.update_controller_service(id=controller_services_id, body=data)

def get_controller_services_version(controller_services_id):

    """ This function will Retrieve controller service version 
    :param int controller_services_id: id of a controller service """
    
    obj = nipyapi.nifi.apis.controller_services_api.ControllerServicesApi(api_client=None)
    response = obj.get_controller_service(id=controller_services_id)

    return response.revision.version

def update_rdbms_controller_services_properties(version, controller_services_id,
                                                postgre_config, config_template_path):
                                                
    """ This function will Set the values for rdbms controller service
    :param int version: latest version of controller service
    :param int controller_services_id: id of a controller service
    :param dict postgre_config: configuration for rdbms controller service
    :param str config_template_path: config file path """
    
    with open(config_template_path, "r") as file:
        data = json.load(file)
    data["revision"]["version"] = version
    data["component"]["id"] = controller_services_id
    data["component"]["properties"]["Database Connection URL"] = \
        postgre_config['Database Connection URL']
    data["component"]["properties"]["Database Driver Class Name"] = \
        postgre_config['Database Driver Class Name']
    data["component"]["properties"]["database-driver-locations"] = \
        postgre_config['database-driver-locations']
    data["component"]["properties"]["Database User"] = \
        postgre_config['Database User']
    data["component"]["properties"]["Password"] = \
        postgre_config['Password']
    obj = nipyapi.nifi.apis.controller_services_api.ControllerServicesApi(api_client=None)
    obj.update_controller_service(id=controller_services_id, body=data)
    state_list = check_controller_service_state(controller_services_id)
    error = state_list[0]
    state = state_list[1]
    if state == "INVALID" or state == "invalid":
        logging.error(error)
        controller_services_version =\
            get_controller_services_version(controller_services_id)
        referencing_component_list = get_controller_service_referencing_component\
            (controller_services_id)
        stop_referencing_components(referencing_component_list)
        stop_controller_service(controller_services_id, controller_services_version)
        logging.error("Check values you have entered in setup.json file")
    else:
        print("Enabled Successfully")

def update_aws_controller_services_properties(version, controller_services_id, s3_configs,
                                              s3_template_path):
                                              
    """ This function will Set the values for AWS controller service
    :param int version: latest version of controller service
    :param int controller_services_id: id of a controller service
    :param dict s3_configs: configuration for aws controller service
    :param str s3_template_path: config file path """
    
    with open(s3_template_path, "r") as file:
        data = json.load(file)
    data["revision"]["version"] = version
    data["component"]["id"] = controller_services_id
    data["component"]["properties"]["Access Key"] = s3_configs['Access Key']
    data["component"]["properties"]["Secret Key"] = s3_configs['Secret Key']
    obj = nipyapi.nifi.apis.controller_services_api.ControllerServicesApi(api_client=None)
    obj.update_controller_service(id=controller_services_id, body=data)
    state_list = check_controller_service_state(controller_services_id)
    error = state_list[0]
    state = state_list[1]
    if state == "INVALID" or state == "invalid":
        logging.error(error)
        controller_services_version = \
            get_controller_services_version(controller_services_id)
        referencing_component_list = get_controller_service_referencing_component\
            (controller_services_id)
        stop_referencing_components(referencing_component_list)
        stop_controller_service(controller_services_id, controller_services_version)
        logging.error("Check values you have entered in setup.json file")
    else:
        print("Enabled Successfully")

def get_variable_referencing_component_id():

    """ This function will Retrieve the referencing component processor id"""
    
    processor_id_list = []
    obj = nipyapi.nifi.apis.resources_api.ResourcesApi(api_client=None)
    response = obj.get_resources()
    for i in range(len(response.resources)):
        if (response.resources[i].name) == "PutHDFS":
            identifier = response.resources[i].identifier
            ids = identifier[12:]
            processor_id_list.append(ids)
    return processor_id_list

def get_referencing_component_run_state(referencing_component_list):

    """ This function will Retrieve referencing component processor state
    :param list referencing_component_list: list of ids of referencing processors """
    
    referencing_component_last_refreshed = []
    obj = nipyapi.nifi.apis.processors_api.ProcessorsApi(api_client=None)
    for value in referencing_component_list:
        if isinstance(value, str):
            response = obj.get_processor(id=value)
            referencing_component_last_refreshed.append(response.status.stats_last_refreshed)
    return referencing_component_last_refreshed

def get_referencing_component_run_status(referencing_component_list):
    """ This function will Retrieve referencing component processor status
    :param list referencing_component_list: list of ids of referencing processors """
    
    referencing_component_status_list = []
    obj = nipyapi.nifi.apis.processors_api.ProcessorsApi(api_client=None)
    for value in referencing_component_list:
        if isinstance(value, str):
            response = obj.get_processor(id=value)
            referencing_component_status_list.append(response.component.validation_errors)
            referencing_component_status_list.append(response.component.validation_status)
            referencing_component_status_list.append(response.component.state)
    return referencing_component_status_list

