---
title: Ingest Data Flow
tags: [ingest]
toc: true
---

## Data Flow Overview   

{% include image.html url="/images/dw-data-flow-1.png" file="dw-data-flow-1.png" alt="Ingest Data Flow" %}

## Flag Maker

A given Flag Maker process will be configured and deployed to manage a single data flow within the system. There can be
any number of Flag Maker processes, each driving M/R processing for its own flow, and that flow may consist of one or more
distinct data types. Data types are registered via [configuration](configuration#data-type-configuration) files and are uniquely
identified by their respective **data.name** properties.

Thus, the configuration settings for a given Flag Maker are tailored for the characteristics of its managed flow and its
associated data types, characteristics such as data volume, individual file size, raw file format, etc.

### Overview

{% include image.html url="/images/dw-flag-maker-1.png" file="dw-flag-maker-1.png" alt="Flag Maker Overview" %}

### Configuration

Setup and configuration details are [here](configuration#flag-maker-configuration)

### Example Flag File

```bash
$DATAWAVE_INGEST_HOME/bin/ingest/live-ingest.sh /local/flags/1453374646.00_mydatatype_20160121103632_mymachine_16f803c3a4eff08c7.seq+784.flag.inprogress 150 -inputFormat datawave.ingest.input.reader.event.EventSequenceFileInputFormat -inputFileLists -inputFileListMarker ***FILE_LIST***
***FILE_LIST***
/data/flagged/mydatatype/2016/01/21/mydatatype_20160121103632_mymachine_16f803c3a4eff08c7.seq
/data/flagged/mydatatype/2016/01/21/mydatatype_20160121103455_mymachine_26d06d0502a022163.seq
/data/flagged/mydatatype/2016/01/21/mydatatype_20160121100507_mymachine_21732b5e75f12c859.seq
/data/flagged/mydatatype/2016/01/21/mydatatype_20160121105902_mymachine_32fb2a1590733dcfa.seq
/data/flagged/mydatatype/2016/01/21/mydatatype_20160121102012_mymachine_267877fd6f6f24357.seq
...
```

### Workflow

{% include image.html url="/images/dw-flag-maker-2.png" file="dw-flag-maker-2.png" alt="Flag File Lifecycle 1" %}

{% include image.html url="/images/dw-flag-maker-3.png" file="dw-flag-maker-3.png" alt="Flag File Lifecycle 2" %}

### Recovery

If a Flag Maker process happens to terminate abnormally for any reason...
1. Move all **flagging** files back to the HDFS base folder for the given data type
2. For all **flag.generating** files, move the flagged files to the base directory
3. Remove the **flag.generating** files
4. Investigate the root cause of the issue and restart the Flag Maker as needed

## MapReduce

### Ingest Job Overview

{% include image.html url="/images/dw-ingest-job-1.png" file="dw-ingest-job-1.png" alt="Ingest Job Overview" %}

## Bulk Loader

### Overview

{% include image.html url="/images/dw-bulk-loader-1.png" file="dw-bulk-loader-1.png" alt="Bulk Loader Overview" %}

### Configuration

Setup and configuration details are [here](configuration#bulk-loader-configuration)

### Workflow

{% include image.html url="/images/dw-bulk-loader-2.png" file="dw-bulk-loader-2.png" alt="Bulk Loader Workflow" %}


