"""Setting HDFS configuration as variables in nifi flow"""
import json
import os
import logging
import time
import nipyapi
from nifi.environment import utils

PROCESSOR_GROUP = "Nimbus_V"
VARIABLE_NAME = "hdfs_site"


def _get_pg_id():

    """ This function will Retrieve process group id"""
    
    obj = nipyapi.nifi.apis.resources_api.ResourcesApi(api_client=None)
    response = obj.get_resources()
    for i in range(len(response.resources)):
        if (response.resources[i].name) == PROCESSOR_GROUP:
            identifier = response.resources[i].identifier
    return identifier[16:52]


def _get_pg_version(pg_id):

    """ This function will Retrieve process group version
    :param int pg_id: processor group id"""
    
    obj = nipyapi.nifi.apis.process_groups_api.ProcessGroupsApi(api_client=None)
    response = obj.get_variable_registry(id=pg_id)
    return response.process_group_revision.version


def _get_variable_referencing_component_version(processor_id):

    """ This function will Retrieve variable referencing component processor version
    :param int processor_id: Id of a processor"""

    obj = nipyapi.nifi.apis.processors_api.ProcessorsApi(api_client=None)
    response = obj.get_processor(id=processor_id)
    referencing_component_version = response.revision.version
    return referencing_component_version


def _update_pg_hdfs_variables(version, pg_id, hdfs_config):

    """This function will Set the variables in process group
    :param int version: latest version of variables
    :param int pg_id: processor group id
    :param dict hdfs_config: configurations for setting variables"""
    
    file_dir = os.path.dirname(os.path.abspath(__file__))
    config_template_path = os.path.join(file_dir, '..', 'properties_templates',
                                        'hdfs_properties.json')
    with open(config_template_path, "r") as file:
        data = json.load(file)
    data["processGroupRevision"]["version"] = version
    data["variableRegistry"]["processGroupId"] = pg_id
    data["variableRegistry"]["variables"][0]["variable"]["value"] = hdfs_config['hdfs-site']
    data["variableRegistry"]["variables"][1]["variable"]["value"] = hdfs_config['core-site']
    data["variableRegistry"]["variables"][2]["variable"]["value"] = hdfs_config['hdfs-path']
    obj = nipyapi.nifi.apis.process_groups_api.ProcessGroupsApi(api_client=None)
    obj.update_variable_registry(id=pg_id, body=data)

def _check_component_status(referencing_component_status_list, component_list):

"""This function will check the referencing process group status
    :param list referencing_component_status_list: list of referencing component id's
    :param list component_list: processor group id list of components"""

    for status in referencing_component_status_list:
        if status == "RUNNING":
            utils.stop_referencing_components(component_list)


def _check_component_state(referencing_component_status_list1, referencing_component_status_list1, processor_id_list,
                           pg_id, hdfs_config):
                           
    """ This function will check the referencing process group state
    :param list referencing_component_status_list1: list of referencing component id's
    :param list referencing_component_status_list2: list of referencing component id's
    :param list processor_id_list: list of processors id's
    :param int pg_id: processor group id 
    :param dict hdfs_config: configurations for getting component status """
                       
    last_refreshed_state = 0
    for i, j in zip(referencing_component_status_list1, referencing_component_status_list2):
        if i == "STOPPED" and j == "STOPPED":
            last_refreshed_state = utils.get_referencing_component_run_state(processor_id_list)
            pg_version = _get_pg_version(pg_id)
            _update_pg_hdfs_variables(pg_version, pg_id, hdfs_config)
    return last_refreshed_state


def _check_component_error(referencing_component_status_list1, referencing_component_status_list2):

	""" This function will check the errors
    :param list referencing_component_status_list1: list of referencing component id's
    :param list referencing_component_status_list2: list of referencing component id's"""

    break_flag = 0
    for error1, error2 in zip(referencing_component_status_list1,
                              referencing_component_status_list2):
        if error1 != "INVALID" and error1 != "STOPPED" and error1 != "RUNNING" and \
                error2 != "INVALID" and error2 != "STOPPED" and error2 != "RUNNING":
            if error1 == error2:
                logging.error(error1)
                break_flag = 1
                break
            else:
                logging.error(error1, "\n", error2)
                break_flag = 1
                break
    return break_flag


def _set_hdfs_variables(updated_referencing_component_status_list1, updated_referencing_component_status_list2,
                        processor_id1, processor_id2):
    running_flag = 0
    for i, j in zip(updated_referencing_component_status_list1,
                    updated_referencing_component_status_list2):
        if i != "RUNNING" and j != "RUNNING":
            try:
                version1 = _get_variable_referencing_component_version(processor_id1)
                version2 = _get_variable_referencing_component_version(processor_id2)
                component_list = [version1, processor_id1, version2, processor_id2]
                utils.start_referencing_components(component_list)
            except Exception:
                time.sleep(5)
        if i == "RUNNING" and j == "RUNNING":
            running_flag = 1
            break
    return running_flag

def _check_component_processor_flag(referencing_component_status_list1, referencing_component_status_list2):

	""" This function will get the flag values
    :param list referencing_component_status_list1: list of referencing component id's
    :param list referencing_component_status_list2: list of referencing component id's"""
    
    run_processor_flag = 0
    for status1, status2 in zip(referencing_component_status_list1,
                                referencing_component_status_list2):
        if status1 and status2 == "INVALID":
            run_processor_flag = 1
            break
        if status1 and status2 == "VALID":
            run_processor_flag = 0
            break
    return run_processor_flag


def set_properties(hdfs_config):
    """ This function will Call all the functions"""
    
    run_processor_flag = 0

    print("Setting hdfs variables")
    pg_id = _get_pg_id()
    processor_id_list = utils.get_variable_referencing_component_id()
    processor_id1 = processor_id_list[0]
    processor_id2 = processor_id_list[1]
    referencing_component_version1 = _get_variable_referencing_component_version(processor_id1)
    referencing_component_version2 = _get_variable_referencing_component_version(processor_id2)
    component_list1 = [referencing_component_version1, processor_id1]
    component_list2 = [referencing_component_version2, processor_id2]
    referencing_component_status_list1 = \
        utils.get_referencing_component_run_status(component_list1)
    referencing_component_status_list2 = \
        utils.get_referencing_component_run_status(component_list2)

    _check_component_status(referencing_component_status_list1, component_list1)
    _check_component_status(referencing_component_status_list2, component_list2)

    referencing_component_status_list1 = \
        utils.get_referencing_component_run_status(component_list1)
    referencing_component_status_list2 = \
        utils.get_referencing_component_run_status(component_list2)

    last_refreshed_state = _check_component_state(referencing_component_status_list1,
                                                  referencing_component_status_list2, processor_id_list, pg_id,
                                                  hdfs_config)
    while True:
        updated_referencing_component_status_list1 = \
            utils.get_referencing_component_run_status(component_list1)
        updated_referencing_component_status_list2 = \
            utils.get_referencing_component_run_status(component_list2)
        current_refreshed_state = utils.get_referencing_component_run_state(processor_id_list)

        for (last_refreshed, current_refreshed) in \
                zip(last_refreshed_state, current_refreshed_state):
            if last_refreshed == current_refreshed:
                time.sleep(5)
            if last_refreshed != current_refreshed:
                referencing_component_status_list1 = \
                    utils.get_referencing_component_run_status(component_list1)
                referencing_component_status_list2 = \
                    utils.get_referencing_component_run_status(component_list2)
                run_processor_flag = _check_component_processor_flag(referencing_component_status_list1,
                                                                     referencing_component_status_list2)

        if run_processor_flag == 1:
            referencing_component_status_list1 = \
                utils.get_referencing_component_run_status(component_list1)
            referencing_component_status_list2 = \
                utils.get_referencing_component_run_status(component_list2)
            break_flag = _check_component_error(referencing_component_status_list1,
                                                referencing_component_status_list2)
            if break_flag == 1:
                break

        if run_processor_flag == 0:
            running_flag = _set_hdfs_variables(updated_referencing_component_status_list1,
                                               updated_referencing_component_status_list2, processor_id1,
                                               processor_id2)
            if running_flag == 1:
                print("Variables are set succesfully")
                break

