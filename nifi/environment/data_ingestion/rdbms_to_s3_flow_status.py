"""tracking the over all flow status for sending data to S3"""
import time
import nipyapi

def _get_s3_flow_details():

    """This function will Retrieve the details for flow"""
    
    obj = nipyapi.nifi.apis.resources_api.ResourcesApi(api_client=None)
    response = obj.get_resources()
    for i in range(len(response.resources)):
        if (response.resources[i].name) == "Put S3":
            identifier = response.resources[i].identifier
            pg_id_s3 = (identifier[16:52])
    obj = nipyapi.nifi.apis.process_groups_api.ProcessGroupsApi(api_client=None)
    response = obj.get_connections(id=pg_id_s3)
    for i in range(len(response.connections)):
        if (response.connections[i].status.name == "success" and response.connections
            [i].status.source_name == "PutS3Object" and response.connections
                [i].status.destination_name == "UpdateAttribute"):
            success_id_s3 = response.connections[i].status.destination_id

        if (response.connections[i].status.name == "failure" and response.connections[
            i].status.source_name == "PutS3Object" and response.connections[
                i].status.destination_name == "UpdateAttribute"):
            failure_id_s3 = response.connections[i].status.destination_id
    processor_ids_list_s3 = [success_id_s3, failure_id_s3]
    return processor_ids_list_s3

def _track_s3_flow_status(processor_ids_list):

    """This function will Track the flow status for s3 processor
    :param list processor_ids_list: list of processors ids"""
    
    success_id = processor_ids_list[0]
    failure_id = processor_ids_list[1]

    obj = nipyapi.nifi.apis.processors_api.ProcessorsApi(api_client=None)
    response = obj.get_state(id=success_id)
    value_success_s3 = response.component_state.local_state.state[0].value
    response = obj.get_state(id=failure_id)
    value_failure_s3 = response.component_state.local_state.state[0].value

    while True:
        obj = nipyapi.nifi.apis.processors_api.ProcessorsApi(api_client=None)
        response = obj.get_state(id=success_id)
        value_current_success = response.component_state.local_state.state[0].value
        if value_success_s3 == value_current_success:
            print("Data Ingestion In progress")
            time.sleep(5)

        else:
            print("Data Ingested Successfully")
            break

        response = obj.get_state(id=failure_id)
        value_current_failure = response.component_state.local_state.state[0].value

        if value_failure_s3 == value_current_failure:
            print("Data Ingestion In progress")
            time.sleep(5)

        else:
            print("Data Ingestion Failed")
            break

def get_flow_status():

    """ This function will Call the above functions """
    
    processor_ids_list = _get_s3_flow_details()
    _track_s3_flow_status(processor_ids_list)

