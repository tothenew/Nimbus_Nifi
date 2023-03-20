"""Running the processor group"""
import json
import os
import nipyapi

def _get_pg_id():

    """This function will Retrieve process group id"""
    obj = nipyapi.nifi.apis.resources_api.ResourcesApi(api_client=None)
    response = obj.get_resources()
    for i in range(len(response.resources)):
        if (response.resources[i].name) == "Nimbus_V":
            identifier = response.resources[i].identifier
    return identifier[16:52]

def _check_processor_group_status(pg_id):

    """This function will check if the process group is running or stopped
    :param int pg_id: processor group id """
    
    obj = nipyapi.nifi.apis.flow_api.FlowApi(api_client=None)
    response = obj.get_process_group_status(id=pg_id)
    for group_id in range (len(response.process_group_status.aggregate_snapshot.
                                       processor_status_snapshots)):
        if response.process_group_status.aggregate_snapshot.processor_status_snapshots[group_id].\
                processor_status_snapshot.group_id == pg_id:
            run_status = response.process_group_status.aggregate_snapshot.processor_status_snapshots[group_id].\
                processor_status_snapshot.run_status
    return run_status

def _start_processor_group(pg_id):

    """ This function will Strat the process group
    :param int pg_id: processor group id"""
    
    file_dir = os.path.dirname(os.path.abspath(__file__))
    config_template_path = os.path.join(file_dir, '..', 'properties_templates',
                                        'start_processor_group.json')
    with open(config_template_path, "r") as file:
        data = json.load(file)
    data['id'] = pg_id
    obj = nipyapi.nifi.apis.flow_api.FlowApi(api_client=None)
    obj.schedule_components(id=pg_id, body=data)

def update_processor_group_run_status():

    """This function will Call the above functions"""
    
    pg_id = _get_pg_id()
    run_status = _check_processor_group_status(pg_id)
    if run_status.lower() == "stopped":
        print("starting the process group")
        _start_processor_group(pg_id)
        print("process group started")

