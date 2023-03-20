"""Setting up nifi environment"""
import importlib
import json
import logging
import argparse
import os
from nifi.environment import config
import nifi.environment.properties.update_controller_services as controller_services
import nifi.environment.properties.upload_template as template
import nifi.environment.properties.update_processor_group_properties as processor_group

def main():
""" This function will setup your NiFi 
Environment according to your source and destination """

    parser = argparse.ArgumentParser()
    parser.add_argument('--f', type=str, required=True)
    parser.add_argument('--c', type=str, required=True)
    args = parser.parse_args()
    path = args.f
    config_path = args.c
    config.configuration(config_path)
    file_dir = os.path.dirname(os.path.abspath(__file__))
    template_path = os.path.join(file_dir, 'ingestion_templates', 'Nimbus_data_ingestion.xml')
    template.set_template_on_nifi_canvas(template_path)
    controller_services.update_controller_services()
    with open(path, "r") as file:
        data = json.load(file)
    for value in data:
        FLAG = False
        for key in data[value]:
            if not data[value][key]:
                FLAG = True
                break
        if not FLAG:
            module_name = f'nifi.environment.properties.set_{value}_properties'
            module = importlib.import_module(module_name, 'environment')
            func = getattr(module, 'set_properties')
            func(data[value])
        if FLAG:
            logging.error(f"INVALID SETUP.JSON FILE \n Enter value for {data}")
    processor_group.update_processor_group_run_status()


if __name__ == "__main__":
    main()

