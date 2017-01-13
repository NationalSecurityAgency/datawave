This project contains utilities that are designed to gather, persist, and report Datawave ingest metrics.

Metrics are received from the poller, ingester and loader in the form of Hadoop Counters objects. Counters can be considered fancy maps of type <String, long>, so it is possible to store time and count information along with tags. These counters objects implement the `Writable` interface and are easily consumed via MapReduce (other, more pressing inefficiencies also exist, which we will touch on later). These Counters objects are stored in HDFS and consumed by cron jobs that submit jobs to place them into one of three metrics tables inside of Accumulo: PollerMetrics, IngestMetrics and LoaderMetrics. There's also a DatawaveMetrics table that we'll talk about later.

There exists a logical correlation between entries in one table and entries in another. For example, output files in the PollerMetrics map to input files into the IngestMetrics table, and some of the job IDs (bulk loading jobs) map to directories in the LoaderMetrics table.n This chain of processing can be thought of as a flow, with up to five specific timing segments: Poller Duration (time spent being processed by the poller), Ingest Delay (time spent sitting on HDFS waiting to processed by the ingest job), Ingest Duration (time spent being processed by an ingest job), and optionally Loader Delay and Loader Duration.

So, imagine that ingest has been running for a while and these tables are filled up with metrics. How do we say, "It takes an event X amount of time to get loaded into Accumulo?" Using the analytic.sh script in $NW_HOME/bin/metrics, we can run a MapReduce job that scans the ingest table and looks to correlate a job with its input files and output directory. It then outputs mutations to the DatawaveMetrics table. Each entry in this table shows that, at a given point in time, it took an event of type T, X minutes (broken down into category, as explained in the timing information above) to get loaded into Accumulo.

In terms how the framework runs, there are a few phases:

1) Ingest jobs complete (poller, ingest or loader)
2) The metrics script ingests the artifacts left after each of the jobs into the metrics tables
3) The analytic comes through and correlates the information in each table

Now, the data is ready to be viewed.

The web portion is a simple client that gives a data type specific view of what Datawave Ingest has been doing for a specified period of time. It relies on some plain-jane servlets to fetch numbers and jQuery + jqPlot to graph and display data in a browser. The metrics server itself is an embedded Jetty application that outputs JSON objects, which makes them easily consumed by web apps.

The interface for the metrics web app is simple: select which ingest type you wish to view (Live or Bulk) and the start date of the metrics you wish to view (it automatically stops at the current time). If complete flows are available, a bar char showing, by type, the average latency for events will appear. Then a tab with data types will display the total events processed during that time, and a third graph will show average ingest rate of the system.



