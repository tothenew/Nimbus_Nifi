"""Importing the common controller services"""
import json
import os
import time
import nipyapi

def _get_postgre_controller_service_configs(response, obj_cs):

    """ This function will retrieve postgre controller service configs
    :param dict response: response returned by api
    :param obj obj_cs: object of a nipyapi class"""
    
    for j in range(len(response.resources)):
        if (response.resources[j].name) == "execute_sql_query_postgre":
            identifier = response.resources[j].identifier
            pg_id_postgre = identifier[16:52]
            response_postgre = obj_cs.get_controller_services_from_group(id=pg_id_postgre)
            for j in range(len(response_postgre.controller_services)):
                if response_postgre.controller_services[j].component.name == \
                        "nimbus_postgre_CSVRecordSetWriter":
                    name = response_postgre.controller_services[j].component.name
                    version = response_postgre.controller_services[j].revision.version
                    id_postgre = response_postgre.controller_services[j].component.id
    postgre_controller_service_list = [name, version, id_postgre]
    return postgre_controller_service_list

def _update_postgre_controller_service(postgre_controller_service_list, data, object_cs):

    """ This function will update run status for postgre controller service
    :param list postgre_controller_service_list: list of id's of postgre controller service
    :param dict data: configuration to update postgre controller service
    :param obj object_cs: object of a nipyapi class """
    
    name = postgre_controller_service_list[0]
    version = postgre_controller_service_list[1]
    id_postgre = postgre_controller_service_list[2]
    data["revision"]["version"] = version
    data["component"]["id"] = id_postgre
    data["component"]["name"] = name
    object_cs.update_controller_service(id=id_postgre, body=data)

def _get_mysql_controller_service_configs(response, obj_cs):

    """ This function will retrieve mysql controller service configs
    :param dict response: response returned by api
    :param obj obj_cs: object of a nipyapi class"""
    
    for j in range(len(response.resources)):
        if (response.resources[j].name) == "Executing_Sql_Query":
            identifier = response.resources[j].identifier
            pg_id_sql = identifier[16:52]
            response_sql = obj_cs.get_controller_services_from_group(id=pg_id_sql)
            for j in range(len(response_sql.controller_services)):
                if response_sql.controller_services[j].component.name == \
                        "nimbus_CSVRecordSetWriter":
                    name = response_sql.controller_services[j].component.name
                    version = response_sql.controller_services[j].revision.version
                    id_mysql = response_sql.controller_services[j].component.id
    mysql_controller_service_list = [name, version, id_mysql]
    return mysql_controller_service_list

def _update_mysql_controller_service(mysql_controller_service_list, data,object_cs):

    """ This function will update run status for mysql controller service
    :param list mysql_controller_service_list: list of id's of mysql controller service
    :param dict data: configuration to update mysql controller service
    :param obj object_cs: object of a nipyapi class """
    
    name = mysql_controller_service_list[0]
    version = mysql_controller_service_list[1]
    id_sql = mysql_controller_service_list[2]
    data["revision"]["version"] = version
    data["component"]["id"] = id_sql
    data["component"]["name"] = name
    object_cs.update_controller_service(id=id_sql, body=data)

def _get_pg_id(response):

    """ This function will retrieve the file fomat handler processor group id
    :param dict response: response returned by api"""
    
    for i in range(len(response.resources)):
        if (response.resources[i].name) == "FileFormatHandler":
            identifier = response.resources[i].identifier
    return identifier[16:52]

def _get_avro_writer_controller_service_configs(obj_cs, pg_id):

    """ This function will retrieve the avro writer controller service configs
    :param obj obj_cs: object of a nipyapi class
    :param int pg_id: processor group id """
    
    response_file = obj_cs.get_controller_services_from_group(id=pg_id)
    for i in range(len(response_file.controller_services)):
        if response_file.controller_services[i].component.name == "Nimbus_AvroRecordSetWriter":
            name = response_file.controller_services[i].component.name
            version = response_file.controller_services[i].revision.version
            avro_writer_id = response_file.controller_services[i].component.id

    avro_writer_controller_service_list = [name, version, avro_writer_id]
    return avro_writer_controller_service_list

def _update_avro_writer_controller_service(avro_writer_controller_service_list, data, object_cs):

    """ This function will update the run status for avro writer controller service
    :param list avro_writer_controller_service_list: list of id's of avro writer controller service
    :param dict data: configuration to update avro writer controller service
    :param obj object_cs: object of a nipyapi class"""
    
    name = avro_writer_controller_service_list[0]
    version = avro_writer_controller_service_list[1]
    avro_writer_id = avro_writer_controller_service_list[2]
    data["revision"]["version"] = version
    data["component"]["id"] = avro_writer_id
    data["component"]["name"] = name
    object_cs.update_controller_service(id=avro_writer_id, body=data)

def _get_csv_tsv_controller_service_configs(obj_cs, pg_id):

    """ This function will retrieve the csv reader for tsv files controller service configs
    :param obj obj_cs: object of a nipyapi class
    :param int pg_id: processor group id"""
    
    response_file = obj_cs.get_controller_services_from_group(id=pg_id)
    for i in range(len(response_file.controller_services)):
        if response_file.controller_services[i].component.name == "Nimbus_CSVReader_for_tsv_files":
            name = response_file.controller_services[i].component.name
            version = response_file.controller_services[i].revision.version
            csv_tsv_id = response_file.controller_services[i].component.id

    csv_tsv_controller_service_list = [name, version,csv_tsv_id]
    return csv_tsv_controller_service_list

def _update_csv_tsv_controller_service(csv_tsv_controller_service_list,  data, object_cs):

    """ This function will update run status for  csv reader for tsv files controller service
    :param list csv_tsv_controller_service_list: list of id's of csv tsv controller service
    :param dict data: configuration to update csv tsv controller service
    :param obj object_cs: object of a nipyapi class """
    
    name = csv_tsv_controller_service_list[0]
    version = csv_tsv_controller_service_list[1]
    csv_tsv_id = csv_tsv_controller_service_list[2]
    data["revision"]["version"] = version
    data["component"]["id"] = csv_tsv_id
    data["component"]["name"] = name
    object_cs.update_controller_service(id=csv_tsv_id, body=data)

def _get_csv_reader_controller_service_configs(obj_cs, pg_id):

    """ This function will retrieve csv reader controller service configs
    :param obj obj_cs: object of a nipyapi class
    :param int pg_id: processor group id"""
    
    response_file = obj_cs.get_controller_services_from_group(id=pg_id)
    for i in range(len(response_file.controller_services)):
        if response_file.controller_services[i].component.name == "Nimbus_CSVReader":
            name = response_file.controller_services[i].component.name
            version = response_file.controller_services[i].revision.version
            csv_reader_id = response_file.controller_services[i].component.id

    csv_reader_controller_service_list = [name, version, csv_reader_id]
    return csv_reader_controller_service_list

def _update_csv_reader_controller_service(csv_reader_controller_service_list, data, object_cs):

    """ This function will update run status for csv reader controller service
    :param list csv_reader_controller_service_list: list of id's of csv reader controller service
    :param dict data: configuration to update csv reader controller service
    :param obj object_cs: object of a nipyapi class"""
    
    name = csv_reader_controller_service_list[0]
    version = csv_reader_controller_service_list[1]
    csv_reader_id = csv_reader_controller_service_list[2]
    data["revision"]["version"] = version
    data["component"]["id"] = csv_reader_id
    data["component"]["name"] = name
    object_cs.update_controller_service(id=csv_reader_id, body=data)

def _get_json_writer_controller_service_configs(obj_cs, pg_id):

    """ This function will retrieve json writer controller service configs
    :param obj obj_cs: object of a nipyapi class
    :param int pg_id: processor group id"""
    
    response_file = obj_cs.get_controller_services_from_group(id=pg_id)
    for i in range(len(response_file.controller_services)):
        if response_file.controller_services[i].component.name == "Nimbus_JsonRecordSetWriter":
            name = response_file.controller_services[i].component.name
            version = response_file.controller_services[i].revision.version
            json_writer_id = response_file.controller_services[i].component.id
    json_writer_controller_service_list= [name, version, json_writer_id]
    return json_writer_controller_service_list

def _update_json_writer_controller_service(json_writer_controller_service_list, data, object_cs):

    """ This function will update run status for json writer controller service
    :param list json_writer_controller_service_list: list of id's of json writer controller service
    :param dict data: configuration to update json writer controller service
    :param obj object_cs: object of a nipyapi class"""
    
    name =json_writer_controller_service_list[0]
    version = json_writer_controller_service_list[1]
    json_writer_id = json_writer_controller_service_list[2]
    data["revision"]["version"] = version
    data["component"]["id"] = json_writer_id
    data["component"]["name"] = name
    object_cs.update_controller_service(id=json_writer_id, body=data)

def _get_json_tree_controller_service_configs(obj_cs, pg_id):

    """ This function will retrieve json tree controller service configs
    :param obj obj_cs: object of a nipyapi class
    :param int pg_id: processor group id"""
    
    response_file = obj_cs.get_controller_services_from_group(id=pg_id)
    for i in range(len(response_file.controller_services)):
        if response_file.controller_services[i].component.name == "Nimbus_JsonTreeReader":
            name = response_file.controller_services[i].component.name
            version = response_file.controller_services[i].revision.version
            json_tree_id = response_file.controller_services[i].component.id

    json_writer_controller_service_list = [name, version, json_tree_id]
    return json_writer_controller_service_list

def _update_json_tree_controller_service(json_writer_controller_service_list, data, object_cs):

    """ This function will update run status for json tree controller service
    :param list json_writer_controller_service_list: list of id's of json tree controller service
    :param dict data: configuration to update json tree controller service
    :param obj object_cs: object of a nipyapi class"""
    
    name = json_writer_controller_service_list[0]
    version = json_writer_controller_service_list[1]
    json_writer_id = json_writer_controller_service_list[2]
    data["revision"]["version"] = version
    data["component"]["id"] = json_writer_id
    data["component"]["name"] = name
    object_cs.update_controller_service(id=json_writer_id, body=data)


def _postgre_csv_controller_service(controller_service_id):

	""" This function will check the status of postgre controller service 
	:param int controller_service_id: postgre controller service id """

    check_status_object = \
        nipyapi.nifi.apis.controller_services_api.ControllerServicesApi(api_client=None)
    while True:
        status_response = check_status_object.get_controller_service \
            (id=controller_service_id)
        run_status = status_response.status.run_status
        if run_status == "ENABLING" or run_status == "DISABLED":
            time.sleep(5)
        else:
            print("postgre common controller service is Enabled")
            break

def _csv_record_set_writer_controller_service(controller_service_id):

""" This function will check the status of csv record set writer controller service 
	:param int controller_service_id: csv record set writer controller service id """

    check_status_object = \
        nipyapi.nifi.apis.controller_services_api.ControllerServicesApi(api_client=None)
    while True:
        status_response = check_status_object.get_controller_service \
            (id=controller_service_id)
        run_status = status_response.status.run_status
        if run_status == "ENABLING" or run_status == "DISABLED":
            time.sleep(5)
        else:
            print("mysql common controller service is Enabled")
            break

def _avro_record_set_writer_controller_service(controller_service_id):

""" This function will check the status of avro record set writer controller service 
	:param int controller_service_id: avro record set writer controller service id """

    check_status_object = \
        nipyapi.nifi.apis.controller_services_api.ControllerServicesApi(api_client=None)
    while True:
        status_response = check_status_object.get_controller_service \
            (id=controller_service_id)
        run_status = status_response.status.run_status
        if run_status == "ENABLING" or run_status == "DISABLED":
            time.sleep(5)
        else:
            print("AvroRecordSetWriter common controller service is Enabled")
            break

def _csv_reader_for_tsv_files_controller_service(controller_service_id):

""" This function will check the status of csv reader for tsv controller service 
	:param int controller_service_id: csv reader for tsv controller service id """

    check_status_object = \
        nipyapi.nifi.apis.controller_services_api.ControllerServicesApi(api_client=None)
    while True:
        status_response = check_status_object.get_controller_service \
            (id=controller_service_id)
        run_status = status_response.status.run_status
        if run_status == "ENABLING" or run_status == "DISABLED":
            time.sleep(5)
        else:
            print("CSVReader_for_tsv_files common controller service is Enabled")
            break

def _csv_reader_controller_service(controller_service_id):

""" This function will check the status of csv reader controller service 
	:param int controller_service_id: csv reader controller service id """

    check_status_object = \
        nipyapi.nifi.apis.controller_services_api.ControllerServicesApi(api_client=None)
    while True:
        status_response = check_status_object.get_controller_service \
            (id=controller_service_id)
        run_status = status_response.status.run_status
        if run_status == "ENABLING" or run_status == "DISABLED":
            time.sleep(5)
        else:
            print("CSVReader common controller service is Enabled")
            break

def _json_record_set_writer_controller_service(controller_service_id):

""" This function will check the status of json record set writer controller service 
	:param int controller_service_id: json record set writer controller service id """

    check_status_object = \
        nipyapi.nifi.apis.controller_services_api.ControllerServicesApi(api_client=None)
    while True:
        status_response = check_status_object.get_controller_service \
            (id=controller_service_id)
        run_status = status_response.status.run_status
        if run_status == "ENABLING" or run_status == "DISABLED":
            time.sleep(5)
        else:
            print("JsonRecordSetWriter common controller service is Enabled")
            break

def _json_tree_reader_controller_service(controller_service_id):

""" This function will check the status of json tree reader controller service 
	:param int controller_service_id: json tree reader controller service id """

    check_status_object = \
        nipyapi.nifi.apis.controller_services_api.ControllerServicesApi(api_client=None)
    while True:
        status_response = check_status_object.get_controller_service \
            (id=controller_service_id)
        run_status = status_response.status.run_status
        if run_status == "ENABLING" or run_status == "DISABLED":
            time.sleep(5)
        else:
            print("JsonTreeReader common controller service is Enabled")
            break

    
def _update_controller_service_status(controller_services_id_list,
                                      disabled_controller_services_name_list,
                                      data, object_cs, postgre_config_list, mysql_config_list,
                                      avro_writer_config_list, csv_tsv_config_list,
                                      csv_reader_config_list, json_writer_config_list,
                                      json_tree_config_list):
                                      
        """ This function will update the status of controller service 
	:param list controller_services_id_list: list of controller service id
	:param list disabled_controller_services_name_list: name of disabled controller service
	:param dict data: configuration to update the status of controller service
	:param obj object_cs: object of nipyapi 
	:param list postgre_config_list: configuration for postgre controller service
	:param list mysql_config_list: configuration for mysql controller service
	:param list avro_writer_config_list: configuration for avro writer controller service
	:param list csv_tsv_config_list: configuration for csv tsv controller service
	:param list csv_reader_config_list: configuration for csv reader controller service
	:param list json_writer_config_list: configuration for json writer controller service
	:param list json_tree_config_list: configuration for json tree controller service """

    for i in range(len(disabled_controller_services_name_list)):
        if disabled_controller_services_name_list[i] == "nimbus_postgre_CSVRecordSetWriter":
            _update_postgre_controller_service(postgre_config_list, data, object_cs)
            _postgre_csv_controller_service(controller_services_id_list[0])

        if disabled_controller_services_name_list[i] == "nimbus_CSVRecordSetWriter":
            _update_mysql_controller_service(mysql_config_list, data, object_cs)
            _csv_record_set_writer_controller_service(controller_services_id_list[1])

        if disabled_controller_services_name_list[i] == "Nimbus_AvroRecordSetWriter":
            _update_avro_writer_controller_service(avro_writer_config_list, data, object_cs)
            _avro_record_set_writer_controller_service(controller_services_id_list[2])
            
        if disabled_controller_services_name_list[i] == "Nimbus_CSVReader_for_tsv_files":
            _update_csv_tsv_controller_service(csv_tsv_config_list, data, object_cs)
            _csv_reader_for_tsv_files_controller_service(controller_services_id_list[3])

        if disabled_controller_services_name_list[i] == "Nimbus_CSVReader":
            _update_csv_reader_controller_service(csv_reader_config_list, data, object_cs)
            _csv_reader_controller_service(controller_services_id_list[4])
            
        if disabled_controller_services_name_list[i] == "Nimbus_JsonRecordSetWriter":
            _update_json_writer_controller_service(json_writer_config_list, data, object_cs)
            _json_record_set_writer_controller_service(controller_services_id_list[5])
                    
        if disabled_controller_services_name_list[i] == "Nimbus_JsonTreeReader":
            _update_json_tree_controller_service(json_tree_config_list, data, object_cs)
            _json_tree_reader_controller_service(controller_services_id_list[6])

def update_controller_services():

    """ This function will call all the functions"""
    
    file_dir = os.path.dirname(os.path.abspath(__file__))
    config_template_path = os.path.join(file_dir, '..', 'properties_templates',
                                        'controller_services_properties.json')
    with open(config_template_path, "r") as file:
        data = json.load(file)

    obj_cs = nipyapi.nifi.apis.flow_api.FlowApi(api_client=None)
    object_cs = nipyapi.nifi.apis.controller_services_api.ControllerServicesApi(api_client=None)
    obj = nipyapi.nifi.apis.resources_api.ResourcesApi(api_client=None)
    response = obj.get_resources()
    pg_id = _get_pg_id(response)
    postgre_config_list = _get_postgre_controller_service_configs(response, obj_cs)
    postgre_id = postgre_config_list[2]
    mysql_config_list = _get_mysql_controller_service_configs(response, obj_cs)
    mysql_id = mysql_config_list[2]
    avro_writer_config_list = _get_avro_writer_controller_service_configs(obj_cs, pg_id)
    avro_writer_id = avro_writer_config_list[2]
    csv_tsv_config_list = _get_csv_tsv_controller_service_configs(obj_cs, pg_id)
    csv_tsv_id = csv_tsv_config_list[2]
    csv_reader_config_list = _get_csv_reader_controller_service_configs(obj_cs, pg_id)
    csv_reader_id = csv_reader_config_list[2]
    json_writer_config_list = _get_json_writer_controller_service_configs(obj_cs, pg_id)
    json_writer_id = json_writer_config_list[2]
    json_tree_config_list = _get_json_tree_controller_service_configs(obj_cs, pg_id)
    json_tree_id = json_tree_config_list[2]
    controller_services_id_list = [postgre_id, mysql_id, avro_writer_id, csv_tsv_id,
                                   csv_reader_id, json_writer_id, json_tree_id]
    check_status_object = \
        nipyapi.nifi.apis.controller_services_api.ControllerServicesApi(api_client=None)
    disabled_controller_services_name_list = []
    for i in range(len(controller_services_id_list)):
        status_response = check_status_object.get_controller_service\
            (id=controller_services_id_list[i])
        run_status = status_response.status.run_status
        if run_status == "DISABLED":
            name = status_response.component.name
            disabled_controller_services_name_list.append(name)
    if len(disabled_controller_services_name_list) == 0:
        print("Common Controller services are already Enabled")
        print("Updating Other Resources")
    else:
        print("Enabling the Common Controller services")
        _update_controller_service_status(controller_services_id_list,
                                          disabled_controller_services_name_list,
                                          data, object_cs, postgre_config_list,
                                          mysql_config_list, avro_writer_config_list,
                                          csv_tsv_config_list, csv_reader_config_list,
                                          json_writer_config_list, json_tree_config_list)

