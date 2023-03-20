"""Setting aws configuration in controller service"""
import json
import os
import logging
import nipyapi
from nifi.environment import utils

region_list = ["us-east-1", "us-east-2", "us-west-1", "us-west-2",
               "af-south-1", "ap-east-1", "ap-southeast-3",
               "ap-south-1", "ap-northeast-3", "ap-northeast-2",
               "ap-southeast-1", "ap-southeast-2", "ap-northeast-1",
               "ca-central-1", "eu-central-1", "eu-west-1",
               "eu-west-2", "eu-south-1", "eu-west-3", "eu-north-1",
               "me-south-1", "sa-east-1"]


def _data_validation(mysql_config):

    """This function will Validate json data
    :param dict mysql_config: configurations of aws s3"""
    
    validation_flag = 0
    for data in mysql_config:
        if not mysql_config[data]:
            logging.error(f"INVALID SETUP.JSON FILE \n Enter value for {data}")
            validation_flag = 1
    return validation_flag


def _get_aws_controller_service_id():

    """ This function will retrieve aws controller service Id"""
    
    obj = nipyapi.nifi.apis.resources_api.ResourcesApi(api_client=None)
    response = obj.get_resources()
    for i in range(len(response.resources)):
        if (response.resources[i].name) == "Nimbus_AWSCredentialsProviderControllerService":
            identifier_aws = response.resources[i].identifier

    return identifier_aws[21:57]


def _get_s3_pg_id():

    """This function will retrieve processor group id"""
    
    obj = nipyapi.nifi.apis.resources_api.ResourcesApi(api_client=None)
    response = obj.get_resources()
    for i in range(len(response.resources)):
        if (response.resources[i].name) == "Put S3":
            identifier = response.resources[i].identifier

    return identifier[16:52]


def _get_s3_processor_id(pg_id):

    """ This function will retrieve s3 processor id
    :param int pg_id: processor group id"""
    
    processor_id_list = []
    list2 = []
    obj = nipyapi.nifi.apis.process_groups_api.ProcessGroupsApi(api_client=None)
    response = obj.get_connections(id=pg_id)
    for i in range(len(response.connections)):
        if (response.connections[i].component.source.name) == "PutS3Object":
            ids = response.connections[i].component.source.id
            list2.append(ids)
            for processor_id in list2:
                if processor_id not in processor_id_list:
                    processor_id_list.append(processor_id)
    return processor_id_list


def _get_processor_version(processor_id_list):

    """This function will retrieve the s3 processor version
    :param list processor_id_list: list of processor id's"""
  
    id1 = processor_id_list[0]
    id2 = processor_id_list[1]
    obj = nipyapi.nifi.apis.processors_api.ProcessorsApi(api_client=None)
    response = obj.get_processor(id=id1)
    version1 = response.revision.version
    response = obj.get_processor(id=id2)
    version2 = response.revision.version
    version_list = [version1, version2]

    return version_list


def _stop_refrencing_components(version_list, processor_id_list):

    """This function will stop the s3 refrencing processor 
    :param list version_list: list of processor versions
    :param list processor_id_list: list of processor id's"""

    stop_refrencing_components_list = [version_list[0], processor_id_list[0],
                                       version_list[1], processor_id_list[1]]
    obj = nipyapi.nifi.apis.processors_api.ProcessorsApi(api_client=None)
    file_dir = os.path.dirname(os.path.abspath(__file__))
    config_template_path = os.path.join(file_dir, '..', 'properties_templates',
                                        'stop_referencing component.json')
    with open(config_template_path, 'r') as file:
        data = json.load(file)
    for value in stop_refrencing_components_list:
        if isinstance(value, int):
            data["revision"]["version"] = value
        else:
            data["component"]["id"] = value
            obj.update_processor(id=value, body=data)


def _update_region(version_list, processor_id_list, region):

    """ This function will update the region-property in  processor
    :param list version_list: list of processor versions
    :param list processor_id_list: list of processor id's
    :param list region: aws region"""
    
    version1 = version_list[0]
    version2 = version_list[1]
    id1 = processor_id_list[0]
    id2 = processor_id_list[1]

    file_dir = os.path.dirname(os.path.abspath(__file__))
    config_template_path = os.path.join(file_dir, '..',
                                        'properties_templates', 's3_properties.json')
    with open(config_template_path, "r") as file:
        data = json.load(file)

    data["revision"]["version"] = version1
    data["component"]["id"] = id1
    data["component"]["config"]["properties"]["Region"] = region
    obj = nipyapi.nifi.apis.processors_api.ProcessorsApi(api_client=None)
    obj.update_processor(id=id1, body=data)

    data["revision"]["version"] = version2
    data["component"]["id"] = id2
    data["component"]["config"]["properties"]["Region"] = region
    obj = nipyapi.nifi.apis.processors_api.ProcessorsApi(api_client=None)
    obj.update_processor(id=id2, body=data)
    print("Region Updated successfully")


def _check_region(s3_config):
	""" This function will check if the region is correct 
	:param dict s3_config: configurations of aws s3"""
	
    flag = 0
    for i in range(len(region_list)):
        if region_list[i] == s3_config["Region"]:
            flag = 1
    return flag


def set_properties(s3_config):

    """This function will Call all the functions all the functions"""
    
    flag = _check_region(s3_config)
    file_dir = os.path.dirname(os.path.abspath(__file__))
    s3_template_path = os.path.join(file_dir, '..', 'properties_templates',
                                    'aws_controller_services_properties.json')

    if flag == 0:
        logging.error(f"please enter a valid Region in setup.json file, refer to the List \n "
                      f"{region_list}")
    else:
        region = s3_config["Region"]
        validation_flag = _data_validation(s3_config)
        if validation_flag == 0:
            print("Enabling AWS controller service and setting S3 region")
            pg_id = _get_s3_pg_id()
            processor_id_list = _get_s3_processor_id(pg_id)
            version_list = _get_processor_version(processor_id_list)
            _stop_refrencing_components(version_list, processor_id_list)
            version_list = _get_processor_version(processor_id_list)
            _update_region(version_list, processor_id_list, region)
            controller_services_id = _get_aws_controller_service_id()
            state_list = utils.check_controller_service_state(controller_services_id)
            run_status = state_list[2]
            if run_status == "ENABLED" or run_status == "ENABLING":
                referencing_component_list = utils.get_controller_service_referencing_component \
                    (controller_services_id)
                utils.stop_referencing_components(referencing_component_list)
                version = utils.get_controller_services_version(controller_services_id)
                utils.stop_controller_service(controller_services_id, version)
                version = utils.get_controller_services_version(controller_services_id)
                utils.update_aws_controller_services_properties(version, controller_services_id,
                                                                s3_config, s3_template_path)
                utils.check_controller_service_state(controller_services_id)
                referencing_component_list = utils.get_controller_service_referencing_component \
                    (controller_services_id)
                utils.start_referencing_components(referencing_component_list)

            else:
                state_list = utils.check_controller_service_state(controller_services_id)
                previous_state = state_list[1]
                version = utils.get_controller_services_version(controller_services_id)
                if previous_state == "VALID":
                    utils.update_aws_controller_services_properties(version, controller_services_id,
                                                                    s3_config, s3_template_path)
                    referencing_component_list = utils.get_controller_service_referencing_component \
                        (controller_services_id)
                    utils.start_referencing_components(referencing_component_list)
                else:
                    utils.update_aws_controller_services_properties(version, controller_services_id,
                                                                    s3_config, s3_template_path)

