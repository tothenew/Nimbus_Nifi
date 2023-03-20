"""Tracking the over all flow status for sending data to hdfs"""
import time
import nipyapi

def _get_hdfs_flow_details():

    """ This function will Retrieve flow details for hdfs"""
    
    obj = nipyapi.nifi.apis.resources_api.ResourcesApi(api_client=None)
    response = obj.get_resources()
    for i in range(len(response.resources)):
        if (response.resources[i].name) == "Put_hdfs_pg":
            identifier = response.resources[i].identifier
            pg_id_hdfs = (identifier[16:52])
    obj = nipyapi.nifi.apis.process_groups_api.ProcessGroupsApi(api_client=None)
    response = obj.get_connections(id=pg_id_hdfs)
    for i in range(len(response.connections)):
        if (response.connections[i].status.name == "success" and response.connections[
            i].status.source_name == "PutHDFS" and response.connections
                [i].status.destination_name == "UpdateAttribute"):
            success_id_hdfs = response.connections[i].status.destination_id

        if (response.connections[i].status.name == "failure" and response.connections
            [i].status.source_name == "PutHDFS" and response.connections
                [i].status.destination_name == "UpdateAttribute"):
            failure_id_hdfs = response.connections[i].status.destination_id

    processor_ids_list = [success_id_hdfs, failure_id_hdfs]
    return processor_ids_list

def _track_hdfs_flow_status(processor_ids_list):

    """ This function will Track the flow status for hdfs processor
    :param list processor_ids_list: list of processors ids """
    
    success_id = processor_ids_list[0]
    failure_id = processor_ids_list[1]

    obj = nipyapi.nifi.apis.processors_api.ProcessorsApi(api_client=None)
    response = obj.get_state(id=success_id)
    value_success_hdfs = response.component_state.local_state.state[0].value

    response = obj.get_state(id=failure_id)
    value_failure_hdfs = response.component_state.local_state.state[0].value

    while True:
        obj = nipyapi.nifi.apis.processors_api.ProcessorsApi(api_client=None)
        response = obj.get_state(id=success_id)
        value_current_success = response.component_state.local_state.state[0].value
        if value_success_hdfs == value_current_success:
            print("Data Ingestion In progress")
            time.sleep(5)
        else:
            print("Data Ingested Successfully")
            break

        response = obj.get_state(id=failure_id)
        value_current_failure = response.component_state.local_state.state[0].value

        if value_failure_hdfs == value_current_failure:
            print("Data Ingestion In progress")
            time.sleep(10)
        else:
            print("Data Ingestion Failed")
            break

def get_flow_status():

    """ This function will Call the above functions """
    
    processor_ids_list = _get_hdfs_flow_details()
    _track_hdfs_flow_status(processor_ids_list)

