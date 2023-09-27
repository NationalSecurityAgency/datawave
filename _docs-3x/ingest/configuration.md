---
title: DataWave Ingest Configuration
tags: [ingest, configuration]
devtags: [todo]
toc: true
---

{% include devtag_todo.html content="Need a section on edge configuration" %}
{% include devtag_todo.html content="Need a section on ingest-config.xml" %}
{% include devtag_todo.html content="Need a section on shard-ingest-config.xml, and some discussion of $NUM_SHARDS" %}
{% include devtag_todo.html content="Need a section on all-config.xml" %}

## Data Type Configuration

Here, *Data Type* denotes a distinct flow of data into the system in which all the raw input arrives in the same binary format
and conforms to some well-defined information schema (e.g., an XML dataset where all the input files conform to the same XSD)

### Configuration Files

* File Name: *{Data Type}-config.xml*
  - The only requirement for the file name is that it must end with "*-config.xml*"
  - Example file: [myjson-ingest-config.xml][dw_blob_myjson_config]

* Edge definitions for the data type, if any, should be defined in a distinct, global config file
  - Example file: [edge-definitions.xml][dw_blob_edge_config]

### Properties

In practice, the settings available to a given data type may originate from any number of specialized classes throughout the ingest API,
each class establishing its own set of configurable behaviors for various ingest-related purposes. Thus, the properties below are a relatively
small subset of all those possible, but they represent core settings that will be common across *most*, if not all, of your data types. 

{% assign props = site.data.datawave.configs.ingest['data-type'].properties %}
{% include configuration.html 
   properties=props
   caption="Data Type Properties" 
   sort_by_name=false %}
   
## Flag Maker Configuration

### Configuration Files

File Name: *flag-maker-{Flow Name}.xml*

This file contains configuration settings for a single Flag Maker process and its associated data types. The above file name format
is only a recommendation. The file name is not important and can be whatever you'd like.

Examples in the DataWave project include two Flag Maker configs and two sets of accompanying bash scripts. These demonstrate
**[bulk][dw_blob_flag_config_bulk]** ingest and **[live][dw_blob_flag_config_live]** ingest data flows respectively. However,
new configs and scripts can be created as needed. Generally speaking, there is no upper bound on the number of Flag Maker
processes that DataWave Ingest can support.

### Scripts

* *{Flow Name}-ingest-server.sh* -- regulates the number of jobs running and existing marker files for the flow, calls
  *{Flow Name}-execute.sh* if more jobs can be supported
* *{Flow Name}-execute.sh* -- runs the {Flow Name}-ingest.sh command from the first line in the flag file
* *{Flow Name}-ingest.sh* -- starts the mapreduce job

### Classes and Interfaces

* *FlagMaker.java*
* *FlagMakerConfig.java*
* *FlagDataTypeConfig.java*
* *FlagDistributor.java*

### Properties

{% assign props = site.data.datawave.configs.ingest['flag-maker'].parent-properties %}
{% include configuration.html 
   properties=props
   caption="Flag Maker Instance Properties" 
   sort_by_name=true %}

{% assign props = site.data.datawave.configs.ingest['flag-maker'].datatype-properties %}
{% include configuration.html 
   properties=props
   caption="Flag Maker Data Type Properties" 
   sort_by_name=true %}
   

## Bulk Loader Configuration

### Usage

Java class: *datawave.ingest.mapreduce.job.BulkIngestMapFileLoader*

```bash
*.BulkIngestMapFileLoader hdfsWorkDir jobDirPattern instanceName zooKeepers username password \
   [-sleepTime sleepTime] \
   [-majcThreshold threshold] \
   [-majcCheckInterval count] \
   [-majcDelay majcDelay] \
   [-seqFileHdfs seqFileSystemUri] \
   [-srcHdfs srcFileSystemURI] \
   [-destHdfs destFileSystemURI] \
   [-jt jobTracker] \
   [-shutdownPort portNum] \
   confFile [{confFile}]
```
### Properties

{% assign props = site.data.datawave.configs.ingest['bulk-loader'].properties %}
{% include configuration.html 
   properties=props
   caption="Bulk Loader Arguments" 
   sort_by_name=true %}

[dw_blob_flag_config_bulk]: https://github.com/NationalSecurityAgency/datawave/blob/{{ page.release_tag }}/warehouse/ingest-configuration/src/main/resources/config/flag-maker-bulk.xml
[dw_blob_flag_config_live]: https://github.com/NationalSecurityAgency/datawave/blob/{{ page.release_tag }}/warehouse/ingest-configuration/src/main/resources/config/flag-maker-live.xml

[dw_blob_myjson_config]: https://github.com/NationalSecurityAgency/datawave/blob/{{ page.release_tag }}/warehouse/ingest-configuration/src/main/resources/config/myjson-ingest-config.xml
[dw_blob_edge_config]: https://github.com/NationalSecurityAgency/datawave/blob/{{ page.release_tag }}/warehouse/ingest-configuration/src/main/resources/config/edge-definitions.xml