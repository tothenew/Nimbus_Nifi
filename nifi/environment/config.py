"""configuring the host and checking whether SSl is enabled """
import json
import nipyapi


def configuration(config_path):

    """ This function will retrieve the values from config.json and configuring the host
    :param str config_path: path to the config.json file"""
    
    with open(config_path, "r") as file:
        data = json.load(file)
        host = data['host']
        port = data['port']
        ssl = data['SSL-ENABLED']
    host = host + ":" + port
    nipyapi.config.nifi_config.host = 'https://%s/nifi-api' % host

    if ssl.lower() == "true":
        ca_file = data['ssl']['ca_file_path']
        client_cert_file = data['ssl']['client_cert_file']
        client_key_file = data['ssl']['client_key_file']

        nipyapi.security.set_service_ssl_context(service='nifi', ca_file=ca_file,
                                                 client_cert_file=client_cert_file,
                                                 client_key_file=client_key_file)
    else:
        nipyapi.config.registry_config.verify_ssl = False
        nipyapi.config.nifi_config.verify_ssl = False

