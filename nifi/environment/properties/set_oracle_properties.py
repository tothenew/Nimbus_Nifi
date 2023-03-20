"""Setting oracle configuration in controller service"""
import os
import logging
import nipyapi
from nifi.environment import utils

def _data_validation(oracle_config):

    """ This function will Validate json data
    :param dict oracle_config: configurations of oracle """
    
    flag = 0
    for data in oracle_config:
        if not oracle_config[data]:
            logging.error(f"INVALID SETUP.JSON FILE \n Enter value for {data}")
            flag = 1
    return flag

def _get_oracle_controller_service_id():

    """ This function will retrieve oracle controller service Id"""
    
    obj = nipyapi.nifi.apis.resources_api.ResourcesApi(api_client=None)
    response = obj.get_resources()
    for i in range(len(response.resources)):
        if (response.resources[i].name) == "Nimbus_oracle_DBCPConnectionPool":
            identifier_sql = response.resources[i].identifier

    return identifier_sql[21:57]

def set_properties(oracle_config):

    """This function will Call all the functions"""
    
    file_dir = os.path.dirname(os.path.abspath(__file__))
    oracle_template_path = os.path.join(file_dir, '..', 'properties_templates',
                                       'oracle_controller_service_properties.json')
    flag = _data_validation(oracle_config)
    if flag == 0:
        print("Enabling ORACLE controller services")
        controller_services_id = _get_oracle_controller_service_id()
        state_list = utils.check_controller_service_state(controller_services_id)
        run_status = state_list[2]
        if run_status == "ENABLED" or run_status == "ENABLING":
            referencing_component_list = utils.get_controller_service_referencing_component\
                (controller_services_id)
            utils.stop_referencing_components(referencing_component_list)
            version = utils.get_controller_services_version(controller_services_id)
            utils.stop_controller_service(controller_services_id, version)
            version = utils.get_controller_services_version(controller_services_id)
            utils.update_rdbms_controller_services_properties(version, controller_services_id,
                                                              oracle_config, oracle_template_path)
            utils.check_controller_service_state(controller_services_id)
            referencing_component_list = utils.get_controller_service_referencing_component\
                (controller_services_id)
            utils.start_referencing_components(referencing_component_list)

        else:
            state_list = utils.check_controller_service_state(controller_services_id)
            previous_state = state_list[1]
            version = utils.get_controller_services_version(controller_services_id)
            if previous_state == "VALID":
                utils.update_rdbms_controller_services_properties(version, controller_services_id,
                                                                  oracle_config, oracle_template_path)
                referencing_component_list = utils.get_controller_service_referencing_component\
                    (controller_services_id)
                utils.start_referencing_components(referencing_component_list)
            else:
                utils.update_rdbms_controller_services_properties(version, controller_services_id,
                                                                  oracle_config, oracle_template_path)

