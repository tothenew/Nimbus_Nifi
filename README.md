![nimbus logo](/docs/images/Untitled.png)

# Nimbus-NiFi : No Code Data Ingestion Framework using NiFi
[Apache NiFi](https://github.com/apache/nifi) is one of the key tools in the area of Data Engineering and Big Data. It is primarily used for Data Ingestion and Orchestration.

Apache NiFi is a real time data ingestion platform, which can transfer and manage data transfer between different sources and destination systems.This supports a wide variety of data sources and protocols making this platform popular in many IT organizations.

## Basic Features

![flow diagram](/docs/images/Screenshot from 2023-03-09 15-18-24 (1).png)

Nimbus-NiFi enables users to ingest data from multiple sources into different destinations without the need of writing any script.
User just has to provide details of source and destination in easily configurable json files and Nimbus-NiFi will do the rest.

# Getting Started

## Prerequisite
* java (version 8)
* Python v3.8
* Nifi v1.15.3

## Cloning Nimbus-NiFi

```
git clone git@github.com:tothenew/nimbus-nifi.git
```

## Important Points to Consider before Configuring 
* Path for hdfs **hdfs://host:port/target-directory-name/**. 
* Target directory must be present in the hdfs.
* Target directory must have all the permissions.
* For postgresql provide table name with schema example **schema.table_name**
* Incoming file format supported are **XML,JSON,CSV,TSV**.
* Output File Format supported are **CSV,AVRO,PARQUET,ORC**
* For file based ingestion we need absolute path of file, it will not pick file recursively from folder.
* For file based ingestion only provide the destination information in setup.json. 


## Configuring Nimbus-NiFi

* Create the config.json file from config.jon.template and setup.json file from setup.json.template present in [ingestion_templates](nifi/ingestion_templates) folder.
* Configure setup.json according to your source and destination.
* Refer to the template files inside [ingestion_templates](nifi/ingestion_templates) folder and edit the file according to your source and destination. For example if your source is mysql and destination is hdfs, edit and configure the **mysql_to_hdfs.json.template** to create **mysql_to_hdfs.json** file.


## Running Nimbus-NiFi

* For running NiFi without securing it, edit **nifi.properties** file present in **conf/** folder inside NiFi directory  and set **nifi.security.allow.anonymous.authentication** property to **true**


```
python setup.py install

nimbus_env --f {path to your setup.json file} --c {path to your config.json file} to set up nifi environment.

run_ingestion --f {path to your ingestion json file} --c {path to your config.json file} to run the ingestion.
```

# Reporting bugs and contributing code

Want to report a bug or add a new feature ?

Please check out the [contributing Guide](contribution.md).


