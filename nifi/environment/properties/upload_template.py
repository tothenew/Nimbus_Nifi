"""uploading and instantiating the template"""
import json
import os
import nipyapi


def _get_pg_id():

    """ This function will retrieve process group Id"""
    
    obj_resources = nipyapi.nifi.apis.resources_api.ResourcesApi(api_client=None)
    response = obj_resources.get_resources()
    for i in range(len(response.resources)):
        if (response.resources[i].name) == "NiFi Flow":
            identifier = response.resources[i].identifier

    return identifier[16:52]

def _check_template_status():

    """ This function will check whether template already exist"""
    
    flag = 0
    obj = nipyapi.nifi.apis.flow_api.FlowApi(api_client=None)
    response = obj.get_templates()
    for i in range(len(response.templates)):
        if (response.templates[i].template.name) == "Nimbus_data_ingestion":
            flag = 1
            break
    return flag

def _upload_template(pg_id, template_file_path):
    """ This function will upload the template
    :param int pg_id: processor group id"""
    obj_pg = nipyapi.nifi.apis.process_groups_api.ProcessGroupsApi(api_client=None)
    obj_pg.upload_template(id=pg_id, template=template_file_path)

def _import_template(pg_id):

    """This function will instantiating the template
    :param int pg_id: processor group id"""
    
    obj_pg = nipyapi.nifi.apis.process_groups_api.ProcessGroupsApi(api_client=None)
    file_dir = os.path.dirname(os.path.abspath(__file__))
    config_template_path = os.path.join(file_dir, '..', 'properties_templates',
                                        'upload_template.json')
    with open(config_template_path, "r") as file:
        data = json.load(file)
    obj = nipyapi.nifi.apis.flow_api.FlowApi(api_client=None)
    response = obj.get_templates()
    for i in range(len(response.templates)):
        if (response.templates[i].template.name) == "Nimbus_data_ingestion":
            template_id = response.templates[i].id

    data['templateId'] = template_id
    obj_pg.instantiate_template(id=pg_id, body=data)

def set_template_on_nifi_canvas(template_file_path):

    """ This function will call all the functions"""
    
    pg_id = _get_pg_id()
    flag = _check_template_status()
    if flag == 0:
        print("Uploading Template...")
        _upload_template(pg_id, template_file_path)
        print("Importing Template on NIFI CANVAS...")
        _import_template(pg_id)
        print("Imported Successfully")

