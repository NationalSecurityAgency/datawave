---
title: "DataWave Tour: Ingest Basics"
layout: tour
tags: [getting_started, ingest]
summary: |
  The examples below will demonstrate DataWave Ingest usage. In order to follow along in your own DataWave
  environment, you should first complete the <a href="../getting-started/quickstart-install">Quickstart Installation</a>
---

## Configuration Basics

DataWave Ingest is largely driven by configuration. Below we'll use DataWave's example *tvmaze* data type (*myjson* config)
to examine the basic settings used to establish a data type within the ingest framework.

{% include data_dictionary_note.html %}

{% include tvmaze_note.html %}

<ul id="profileTabs" class="nav nav-tabs">
    <li class="active"><a class="noCrossRef" href="#define-type" data-toggle="tab"><b>1: Define the Data Type</b></a></li>
    <li><a class="noCrossRef" href="#register-type" data-toggle="tab"><b>2: Register the Data Type</b></a></li>
    <li><a class="noCrossRef" href="#other-configs" data-toggle="tab"><b>3: Additional Considerations</b></a></li>
</ul>
<div class="tab-content">
<div role="tabpanel" class="tab-pane active" id="define-type" markdown="1">
### Step 1: Define the Data Type

To ingest data in Step 5 below, we'll be utilizing the [myjson-ingest-config.xml][dw_blob_myjson_config] example file, which
defines our **tvmaze** data type and encompasses all of the configuration settings required to ingest the raw JSON from *tvmaze.com*

The settings below are a subset of these and represent the core settings for any data type. They are replicated here only for
demonstration purposes. Please see the documentation [here](../ingest/configuration#data-type-configuration) for full
descriptions of the properties. 

**Deploy directory**: $DATAWAVE_INGEST_HOME/config/

```xml
<configuration>

 <property>
     <name>data.name</name>
     <value>myjson</value>
 </property>

 <property>
     <name>myjson.output.name</name>
     <value>tvmaze</value>
 </property>
 
 <property>
     <name>myjson.data.category.date</name>
     <value>PREMIERED</value>
 </property>

 <property>
     <name>file.input.format</name>
     <value>datawave.ingest.json.mr.input.JsonInputFormat</value>
 </property>

 <property>
     <name>myjson.reader.class</name>
     <value>datawave.ingest.json.mr.input.JsonRecordReader</value>
 </property>

 <property>
     <name>myjson.ingest.helper.class</name>
     <value>datawave.ingest.json.config.helper.JsonIngestHelper</value>
 </property>

 <property>
     <name>myjson.handler.classes</name>
     <value>datawave.ingest.json.mr.handler.ContentJsonColumnBasedHandler</value>
 </property>
 
 <property>
     <name>myjson.data.category.marking.default</name>
     <value>PRIVATE|(BAR&amp;FOO)</value>
 </property>
 
 <property>
     <name>myjson.data.category.index</name>
     <value>NAME,ID,EMBEDDED_CAST_CHARACTER_NAME,...</value>
 </property>
 
 <property>
     <name>myjson.data.category.index.reverse</name>
     <value>NAME,NETWORK_NAME,OFFICIALSITE,URL</value>
 </property>
 ...
 <!-- 
   The remaining settings in the linked config file are beyond the scope of this example
 -->
 ...
</configuration>

```
</div>
<div role="tabpanel" class="tab-pane" id="register-type" markdown="1">
### Step 2: Register the Data Type

The config file, [ingest-config.xml][dw_blob_ingest_config], is used to register all of our data types and define a few
global settings. Properties of interest are replicated here only for demonstration purposes 

**Deploy directory**: $DATAWAVE_INGEST_HOME/config/

```xml
<configuration>
 
 <property>
   <name>ingest.data.types</name>
   <value>myjson,...</value>
   <description>
     Comma-delimited list of data types to be processed by the system. The
     {data.name} value for your data type MUST appear in this list in order for
     it to be processed 
   </description>
 </property>
 ...
 ...
 <property>
   <name>event.discard.interval</name>
   <value>0</value>
   <description>
     Per the DataWave data model, each data object (a.k.a. "event") has an
     associated date, and this date is used to determine the object's YYYYMMDD shard
     partition within the primary data table.
     
     With that in mind, the value of this property is defined in milliseconds and
     denotes that an object having a date prior to (NOW - event.discard.interval)
     should be discarded.
     
     E.g., use a value of (1000 x 60 x 60 x 24) to automatically discard
     objects more than 24 hrs old
       
     - A value of 0 disables this check
     - A data type can override this value with its own
       "{data.name}.event.discard.interval" property
   </description>
 </property>
 ...
 <!-- 
      The remaining settings in this file are beyond the scope of this example,
      and will be covered later 
 -->
 ...
</configuration>

```
</div>
<div role="tabpanel" class="tab-pane" id="other-configs" markdown="1">

### Step 3: Additional Considerations

#### 3.1: The 'all' Data Type

The [all-config.xml][dw_blob_all_config] file represents a globally-recognized data type that may be leveraged to configure
settings that we wish to have applied to all data types.

For example, the *all.handler.classes* property below will enable edge creation and date indexing for any data types that
are configured to take advantage of those behaviors. That is, these particular classes are designed such that they require
additional configuration in order to have any effect during ingest, whereas other handler classes may have
no such requirement.

**Deploy directory**: $DATAWAVE_INGEST_HOME/config/

```xml
<configuration>
 ...
 ...
 <property>
     <name>all.handler.classes</name>
     <value>
       datawave.ingest.mapreduce.handler.edge.ProtobufEdgeDataTypeHandler,
       datawave.ingest.mapreduce.handler.dateindex.DateIndexDataTypeHandler
     </value>
     <description>
       Comma-delimited list of data type handlers to be utilized by all
       registered types, in *addition* to any distinct handlers set by
       individual data types via their own *-config.xml files.
     </description>
 </property>
 ...
 ...
 <property>
     <name>all.ingest.policy.enforcer.class</name>
     <value>datawave.policy.IngestPolicyEnforcer$NoOpIngestPolicyEnforcer</value>
     <description>
        Name of the class to use for record-level (or per-data object) policy
        enforcement on all incoming records
     </description>
 </property>
 ...
 <!-- 
     The remaining settings in this file are beyond the scope of this example,
     and will be covered later 
 -->
 ...
</configuration>
```

#### 3.2: Edge Definitions

The example config, [edge-definitions.xml][dw_blob_edge_config], is used to define various [edges](../getting-started/data-model#edge-table),
all of which are based on our example data types. Note that several edge types are defined for our *tvmaze/myjson* data type.
Performing edge-related queries will be covered [later](edge-query) in the tour.

**Deploy directory**: $DATAWAVE_INGEST_HOME/config/

The *ProtobufEdgeDataTypeHandler* class, as mentioned above, is responsible for generating graph edges from incoming data objects.
Thus, it leverages this config file to determine how and when to create edge key/value pairs for a given data type.

{% include edge_dictionary_note.html %}

</div>
</div>

<button type="button" class="btn" data-toggle="collapse" data-target="#details123">Steps 1, 2, &amp; 3 - More Info</button>
<div id="details123" class="collapse" markdown="1">
In **Steps 1**, **2** and **3**, we looked at a few of the most important configuration properties for establishing a
data type within the DataWave Ingest framework

* **data.name**, and optionally **{data.name}.output.name**, will uniquely identify a data type and its associated raw
  data feed. These two properties can be leveraged to split up a single data type over multiple data feeds, if needed
  
* **{data.name}.reader.class**, **{data.name}.ingest.helper.class**, **{data.name}.handler.classes**, and
  **all.handler.classes** together define processing pipeline for a given data type, in order to transform raw input
  into [data objects](../getting-started/data-model#primary-data-table) comprised of Accumulo key/value pairs
  
* The **all-config.xml** file can be used to control various ingest behaviors for *all* registered data types

* Graph edges in DataWave are composed of {% include data_model_term.html term_id="NFV" display="name" %}
  pairs known to exist within the {% include data_model_term.html term_id="DO" display="name" %}s of DataWave's registered
  data types. Thus, edge types are defined via configuration on a per-data type basis. Given a single data object as input,
  DataWave Ingest may emit zero or more edges based on this configuration.
</div>

---

## Load Some New Data

Lastly, we'll fetch some new data via the TVMAZE-API service and we'll load it into DataWave.

<ul id="profileTabs" class="nav nav-tabs">
    <li class="active"><a class="noCrossRef" href="#get-data" data-toggle="tab"><b>4: Stage the Raw Data</b></a></li>
    <li><a class="noCrossRef" href="#run-job" data-toggle="tab"><b>5: Run the Ingest M/R Job</b></a></li>
</ul>
<div class="tab-content">
<div role="tabpanel" class="tab-pane active" id="get-data" markdown="1">
### Step 4: Stage the Raw Data

Here we use the quickstart's [ingest-tv-shows.sh][dw_blob_ingest_tv_shows] script to fetch additional TV shows along
with cast member information. If you wish, the default list of shows to download may be overridden by passing your
own comma-delimited list of show names via the `--shows` argument.

Use the `--help` option for more information.

The script simply iterates over the specified list of TV show names, invokes
<http://api.tvmaze.com/singlesearch/shows?q=SHOWNAME&embed=cast> for each show, and appends each search result to the
output file specified by `--outfile`

```bash

 $ cd DW_SOURCE/contrib/datawave-quickstart/bin/services/datawave/ingest-examples
 
 # We'll use the --download-only flag to write the data to a local file only,
 # to avoid ingesting the data automatically...

 $ ./ingest-tv-shows.sh --download-only --outfile ~/more-tv-shows.json
 
 [DW-INFO] - Writing json records to /home/me/more-tv-shows.json
 [DW-INFO] - Downloading show data: 'Veep'
 [DW-INFO] - Downloading show data: 'Game of Thrones'
 [DW-INFO] - Downloading show data: 'I Love Lucy'
 [DW-INFO] - Downloading show data: 'Breaking Bad'
 [DW-INFO] - Downloading show data: 'Malcom in the Middle'
 [DW-INFO] - Downloading show data: 'The Simpsons'
 [DW-INFO] - Downloading show data: 'Sneaky Pete'
 [DW-INFO] - Downloading show data: 'King of the Hill'
 [DW-INFO] - Downloading show data: 'Threes Company'
 [DW-INFO] - Downloading show data: 'The Andy Griffith Show'
 [DW-INFO] - Downloading show data: 'Matlock'
 [DW-INFO] - Downloading show data: 'North and South'
 [DW-INFO] - Downloading show data: 'MASH'
 [DW-INFO] - Data download is complete
 ...
 

```
</div>
<div role="tabpanel" class="tab-pane" id="run-job" markdown="1">
### Step 5: Run the Ingest M/R Job

All that's left now is to write the raw data to HDFS and then invoke DataWave's [IngestJob][dw_blob_ingest_job] class
with the appropriate parameters. To accomplish both, we'll simply invoke a quickstart utility function,
[datawaveIngestJson](../getting-started/quickstart-reference#datawave-ingest-functions)

This quickstart function also displays the actual bash commands required to ingest the file, as shown below.

They key thing to note here is that DataWave's [live-ingest.sh][dw_blob_live_ingest_sh] script is used, which passes
arguments to the IngestJob class in order to enable [Live Ingest](../ingest/overview#live-ingest) mode for our data.

{% include note.html content="Typically, MapReduce job submission for a particular data type is automated by a DataWave
   [Flag Maker](../ingest/data-flow#the-flag-maker) process. Configuring and running a flag maker is beyond the scope of
   this exercise" %}

```bash
 # Quickstart utility function...

 $ datawaveIngestJson ~/more-tv-shows.json

 [DW-INFO] - Initiating DataWave Ingest job for '~/more-tv-shows.json'
 [DW-INFO] - Loading raw data into HDFS:
 
   # Here's the command to write the raw data to HDFS...
   hdfs dfs -copyFromLocal ~/more-tv-shows.json /Ingest/myjson

 [DW-INFO] - Submitting M/R job:
 
   # And here's the command to submit the ingest job. The "live-ingest.sh" script
   # passes the parameters below and others to DataWave's IngestJob class...
   $DATAWAVE_INGEST_HOME/bin/ingest/live-ingest.sh /Ingest/myjson/more-tv-shows.json 1 \
      -inputFormat datawave.ingest.json.mr.input.JsonInputFormat \
      -data.name.override=myjson
 ...
 ...
 # Job log is written to the console and also to QUICKSTART_DIR/data/hadoop/yarn/log/
 ...
 ...
 18/04/01 19:45:43 INFO mapreduce.Job: Running job: job_1523209920746_0004
 18/04/01 19:45:48 INFO mapreduce.Job: Job job_1523209920746_0004 running in uber mode : false
 18/04/01 19:45:48 INFO mapreduce.Job:  map 0% reduce 0%
 18/04/01 19:45:55 INFO mapreduce.Job:  map 100% reduce 0%
 18/04/01 19:45:55 INFO mapreduce.Job: Job job_1523209920746_0004 completed successfully
 18/04/01 19:45:55 INFO mapreduce.Job: Counters: 43
 	File System Counters
 		FILE: Number of bytes read=0
 		FILE: Number of bytes written=188556
 		FILE: Number of read operations=0
 		FILE: Number of large read operations=0
 		FILE: Number of write operations=0
 		HDFS: Number of bytes read=394045
 		HDFS: Number of bytes written=0
 		HDFS: Number of read operations=3
 		HDFS: Number of large read operations=0
 		HDFS: Number of write operations=0
 	Job Counters 
 		Launched map tasks=1
 		Data-local map tasks=1
 		Total time spent by all maps in occupied slots (ms)=8842
 		Total time spent by all reduces in occupied slots (ms)=0
 		Total time spent by all map tasks (ms)=4421
 		Total vcore-seconds taken by all map tasks=4421
 		Total megabyte-seconds taken by all map tasks=9054208
 	Map-Reduce Framework
 		Map input records=13
 		Map output records=19584
 		Input split bytes=119
 		Spilled Records=0
 		Failed Shuffles=0
 		Merged Map outputs=0
 		GC time elapsed (ms)=128
 		CPU time spent (ms)=9650
 		Physical memory (bytes) snapshot=311042048
 		Virtual memory (bytes) snapshot=2908852224
 		Total committed heap usage (bytes)=253427712
 	Content Index Counters
 		Document synonyms processed=24
 		Document tokens processed=2083
 	EVENTS_PROCESSED
 		MYJSON=13
 	FILE_NAME
 		hdfs://localhost:9000/Ingest/myjson/more-tv-shows.json=1
 	LINE_BYTES
 		MAX=64184
 		MIN=4686
 		TOTAL=250260
 	ROWS_CREATED
 		ContentJsonColumnBasedHandler=12231
 		DateIndexDataTypeHandler=0
 		ProtobufEdgeDataTypeHandler=8672
 	datawave.ingest.metric.IngestOutput
 		DUPLICATE_VALUE=1563
 		MERGED_VALUE=9104
 		ROWS_CREATED=20903
 	File Input Format Counters 
 		Bytes Read=393926
 	File Output Format Counters 
 		Bytes Written=0
 18/04/01 19:45:55 INFO datawave.ingest: Counters: 43
 	File System Counters
 		FILE: Number of bytes read=0
 		FILE: Number of bytes written=188556
 		FILE: Number of read operations=0
 		FILE: Number of large read operations=0
 		FILE: Number of write operations=0
 		HDFS: Number of bytes read=394045
 		HDFS: Number of bytes written=0
 		HDFS: Number of read operations=3
 		HDFS: Number of large read operations=0
 		HDFS: Number of write operations=0
 	Job Counters 
 		Launched map tasks=1
 		Data-local map tasks=1
 		Total time spent by all maps in occupied slots (ms)=8842
 		Total time spent by all reduces in occupied slots (ms)=0
 		Total time spent by all map tasks (ms)=4421
 		Total vcore-seconds taken by all map tasks=4421
 		Total megabyte-seconds taken by all map tasks=9054208
 	Map-Reduce Framework
 		Map input records=13
 		Map output records=19584
 		Input split bytes=119
 		Spilled Records=0
 		Failed Shuffles=0
 		Merged Map outputs=0
 		GC time elapsed (ms)=128
 		CPU time spent (ms)=9650
 		Physical memory (bytes) snapshot=311042048
 		Virtual memory (bytes) snapshot=2908852224
 		Total committed heap usage (bytes)=253427712
 	Content Index Counters
 		Document synonyms processed=24
 		Document tokens processed=2083
 	EVENTS_PROCESSED
 		MYJSON=13
 	FILE_NAME
 		hdfs://localhost:9000/Ingest/myjson/more-tv-shows.json=1
 	LINE_BYTES
 		MAX=64184
 		MIN=4686
 		TOTAL=250260
 	ROWS_CREATED
 		ContentJsonColumnBasedHandler=12231
 		DateIndexDataTypeHandler=0
 		ProtobufEdgeDataTypeHandler=8672
 	datawave.ingest.metric.IngestOutput
 		DUPLICATE_VALUE=1563
 		MERGED_VALUE=9104
 		ROWS_CREATED=20903
 	File Input Format Counters 
 		Bytes Read=393926
 	File Output Format Counters
 		Bytes Written=0
 	...
 	...

 # Use the Accumulo shell to verify the new TV shows were loaded into
 # the primary data table (ie, the "shard" table in the quickstart env)...
 	
 $ ashell
 
   Shell - Apache Accumulo Interactive Shell
   -
   - version: 1.8.1
   - instance name: my-instance-01
   - instance id: cc3e8158-a94a-4f2e-af9e-d1014b5d1912 
   -
   - type 'help' for a list of available commands
   -
   root@my-instance-01> grep Veep -t datawave.shard

```
</div>
</div>

<button type="button" class="btn" data-toggle="collapse" data-target="#details45">Steps 4 &amp; 5 - More Info</button>
<div id="details45" class="collapse" markdown="1">
In **Steps 4** and **5**, we acquired some new raw data via the TVMAZE-API service and we ingested it via MapReduce

* We loaded the raw data into HDFS with a simple copy command...
  ```bash
    hdfs dfs -copyFromLocal ~/more-tv-shows.json /Ingest/myjson
  ```

* Finally, we used DataWave's [live-ingest.sh][dw_blob_live_ingest_sh] script to kick off the MapReduce job. This script passes
  the`-outputMutations` and `-mapOnly` flags to the [IngestJob][dw_blob_ingest_job] class to enable "live" mode...
  ```bash
   $DATAWAVE_INGEST_HOME/bin/ingest/live-ingest.sh \
     /Ingest/myjson/more-tv-shows.json \                           # Raw file in HDFS
     1 \                                                           # $NUM_SHARDS (i.e., number of reducers)
     -inputFormat datawave.ingest.json.mr.input.JsonInputFormat \  # Overrides IngestJob's default input format
     -data.name.override=myjson                                    # Forces the job to use our 'myjson' config
  ``` 
</div>
  
[dw_blob_myjson_config]: https://github.com/NationalSecurityAgency/datawave/blob/{{ page.release_tag }}/warehouse/ingest-configuration/src/main/resources/config/myjson-ingest-config.xml
[dw_blob_ingest_config]: https://github.com/NationalSecurityAgency/datawave/blob/{{ page.release_tag }}/warehouse/ingest-configuration/src/main/resources/config/ingest-config.xml
[dw_blob_all_config]: https://github.com/NationalSecurityAgency/datawave/blob/{{ page.release_tag }}/warehouse/ingest-configuration/src/main/resources/config/all-config.xml
[dw_blob_edge_config]: https://github.com/NationalSecurityAgency/datawave/blob/{{ page.release_tag }}/warehouse/ingest-configuration/src/main/resources/config/edge-definitions.xml
[dw_blob_ingest_tv_shows]: https://github.com/NationalSecurityAgency/datawave/blob/{{ page.release_tag }}/contrib/datawave-quickstart/bin/services/datawave/ingest-examples/ingest-tv-shows.sh
[dw_blob_ingest_job]: https://github.com/NationalSecurityAgency/datawave/blob/{{ page.release_tag }}/warehouse/ingest-core/src/main/java/datawave/ingest/mapreduce/job/IngestJob.java
[dw_blob_live_ingest_sh]: https://github.com/NationalSecurityAgency/datawave/blob/{{ page.release_tag }}/warehouse/ingest-scripts/src/main/resources/bin/ingest/live-ingest.sh
