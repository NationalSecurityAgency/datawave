package datawave.metrics.mapreduce;

import java.io.IOException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.hadoop.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.hadoop.mapreduce.AccumuloOutputFormat;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import datawave.metrics.config.MetricsConfig;
import datawave.metrics.config.MetricsOptions;
import datawave.metrics.mapreduce.error.ProcessingErrorsMapper;
import datawave.metrics.mapreduce.error.ProcessingErrorsReducer;
import datawave.metrics.util.Connections;
import datawave.util.time.DateHelper;

/**
 * This is a pretty generic wrapper class intended to allow users to specify what type of metrics they're processing at runtime. This simplifies maintenance, as
 * now we have 1 ingester that can work on all metrics. This is possible because type specific logic for metrics is centralized to the map task associated with
 * the metric type.
 * <p>
 * For example, to insert Loader metrics, this tool can be used by invoking the command:
 * <p>
 * MetricsIngester [Accumulo arguments] -inputDirectory /NewIngest/LoaderMetrics -outputTable LoaderMetrics
 * <p>
 * As of now, this utility only supports reading files from one directory and outputting to one table. In the future, it would be best to at least support
 * inputting from multiple directories because Hadoop's API already has support for that. Outputting to multiple tables is do-able, but I have not thought of a
 * good way of mapping logical paths to different tables via this interface yet.
 * <p>
 * Notes:
 * <p>
 * (1) This job is a map only job, because as of now, there's no need to run a reduce task because the metrics are already sorted on per file/job/folder bases.
 * <p>
 * (2) This job will create the table specified as an argument if it does not already exist.
 *
 */
public class MetricsIngester extends Configured implements Tool {
    private static final boolean createTables = true;

    private static final Logger log = Logger.getLogger(MetricsIngester.class);

    protected static final byte[] emptyBytes = {};
    protected static final Value emptyValue = new Value(emptyBytes);

    private static final int MAX_FILES = 2000;

    @Override
    public int run(String[] args) throws Exception {
        _configure(args);

        final Configuration conf = getConf();
        String type = conf.get(MetricsConfig.TYPE);

        /*
         * if the type is "errors", we want to process all of the errors from the metrics files first and then run the regular ingest metrics process
         */
        // MetricsServer.setServerConf(conf);
        // MetricsServer.initInstance();
        if ("errors".equals(type)) {
            try {
                launchErrorsJob(Job.getInstance(conf), conf);
            } catch (Exception e) {
                log.info("Failed to launch errors job", e);
            }
            type = "ingest";
            conf.set(MetricsConfig.TYPE, type);
        }

        /* Type logic so I can differeniate between loader and ingest metrics jobs */
        Class<? extends Mapper<?,?,?,?>> mapperClass;
        String outTable;

        Path inputDirectoryPath = new Path(conf.get(MetricsConfig.INPUT_DIRECTORY));
        FileSystem fs = FileSystem.get(inputDirectoryPath.toUri(), conf);
        FileStatus[] fstats = fs.listStatus(inputDirectoryPath);
        Path[] files = FileUtil.stat2Paths(fstats);
        Path[] fileBuffer = new Path[MAX_FILES];
        for (int i = 0; i < files.length;) {
            Job job = Job.getInstance(getConf());
            job.setJarByClass(this.getClass());

            job.getConfiguration().setInt("mapred.job.reuse.jvm.num.tasks", -1);

            if ("ingest".equalsIgnoreCase(type)) {
                mapperClass = IngestMetricsMapper.class;
                outTable = conf.get(MetricsConfig.INGEST_TABLE, MetricsConfig.DEFAULT_INGEST_TABLE);
                job.setInputFormatClass(SequenceFileInputFormat.class);
            } else if ("loader".equalsIgnoreCase(type)) {
                mapperClass = LoaderMetricsMapper.class;
                outTable = conf.get(MetricsConfig.LOADER_TABLE, MetricsConfig.DEFAULT_LOADER_TABLE);
                job.setInputFormatClass(SequenceFileInputFormat.class);
            } else if ("flagmaker".equalsIgnoreCase(type)) {
                mapperClass = FlagMakerMetricsMapper.class;
                outTable = conf.get(MetricsConfig.FLAGMAKER_TABLE, MetricsConfig.DEFAULT_FLAGMAKER_TABLE);
                job.setInputFormatClass(SequenceFileInputFormat.class);
            } else {
                log.error(type + " is not a valid job type. Please use <ingest|loader>.");
                return -1;
            }

            job.setJobName("MetricsIngester-" + type);

            if (files.length - i > MAX_FILES) {
                System.arraycopy(files, i, fileBuffer, 0, MAX_FILES);
                i += MAX_FILES;
            } else {
                fileBuffer = new Path[files.length - i];
                System.arraycopy(files, i, fileBuffer, 0, fileBuffer.length);
                i += files.length - i;
            }

            SequenceFileInputFormat.setInputPaths(job, fileBuffer);

            job.setMapperClass(mapperClass);

            job.setNumReduceTasks(0);

            job.setOutputFormatClass(AccumuloOutputFormat.class);
            final String instanceName = conf.get(MetricsConfig.INSTANCE);
            final String zookeepers = conf.get(MetricsConfig.ZOOKEEPERS);
            final String userName = conf.get(MetricsConfig.USER);
            final String password = conf.get(MetricsConfig.PASS);
            // @formatter:off
            Properties clientProps = Accumulo.newClientProperties()
                    .to(instanceName, zookeepers).as(userName, password)
                    .batchWriterConfig(new BatchWriterConfig().setMaxLatency(25, TimeUnit.MILLISECONDS))
                    .build();
            AccumuloOutputFormat.configure()
                    .clientProperties(clientProps)
                    .createTables(createTables)
                    .defaultTable(outTable)
                    .store(job);
            // @formatter:on
            log.info("zookeepers = " + zookeepers);
            log.info("instance = " + instanceName);
            log.info("clientConfiguration = " + clientProps);

            job.submit();

            job.waitForCompletion(true);

            if (job.isSuccessful()) {
                for (Path p : fileBuffer) {
                    fs.delete(p, true);
                }
            }
        }

        return 0;
    }

    protected int launchErrorsJob(Job job, Configuration conf)
                    throws IOException, InterruptedException, ClassNotFoundException, AccumuloException, AccumuloSecurityException, TableNotFoundException {
        job.setJobName("ErrorMetricsIngest");

        job.setJarByClass(this.getClass());

        String outTable = conf.get(MetricsConfig.METRICS_TABLE, MetricsConfig.DEFAULT_METRICS_TABLE);

        /*
         * This block allows for us to read from a virtual "snapshot" of a directory and remove only files we process.
         */
        Path inputDirectoryPath = new Path(conf.get(MetricsConfig.INPUT_DIRECTORY));
        FileSystem fs = FileSystem.get(inputDirectoryPath.toUri(), conf);
        FileStatus[] fstats = fs.listStatus(inputDirectoryPath);
        Path[] inPaths = {};
        if (fstats != null && fstats.length > 0) {
            inPaths = FileUtil.stat2Paths(fstats);
            FileInputFormat.setInputPaths(job, inPaths);
        }

        Collection<Range> ranges = new ArrayList<>();

        BatchWriterConfig bwConfig = new BatchWriterConfig().setMaxLatency(1000L, TimeUnit.MILLISECONDS).setMaxMemory(1024L).setMaxWriteThreads(4);

        // job name is in form
        // IngestJob_yyyyMMddHHmmss.553
        // 20120829134659.
        // 20120829135352
        SimpleDateFormat outFormat = new SimpleDateFormat("yyyyMMddHH");

        String jobNamePrefix = "IngestJob_";
        String date = null;
        Date dateObj;

        try (AccumuloClient client = Connections.warehouseClient(conf);
                        BatchWriter writer = client.createBatchWriter(conf.get(MetricsConfig.ERRORS_TABLE, MetricsConfig.DEFAULT_ERRORS_TABLE), bwConfig)) {

            Mutation m = new Mutation("metrics");
            for (Path path : inPaths) {

                String jobName = path.getName();
                if (jobName.contains(".metrics")) {
                    jobName = jobName.substring(0, jobName.indexOf(".metrics"));
                }

                if (jobName.startsWith((jobNamePrefix))) {
                    int end = jobName.lastIndexOf(".");
                    if (end < 0)
                        end = jobName.length();
                    date = jobName.substring(jobNamePrefix.length(), end);
                }

                m.put(new Text(date), new Text(""), emptyValue);

            }

            if (m.size() > 0) {
                writer.addMutation(m);
            }
        }

        Collection<Key> keysToRemove = new ArrayList<>();
        try (AccumuloClient client = Connections.metricsClient(conf);
                        BatchScanner scanner = client.createBatchScanner(conf.get(MetricsConfig.ERRORS_TABLE, MetricsConfig.DEFAULT_ERRORS_TABLE),
                                        Authorizations.EMPTY, 8)) {
            scanner.setRanges(Collections.singleton(new Range(new Text("metrics"))));

            Iterator<Entry<Key,Value>> iter = scanner.iterator();
            String cf;

            long oneDay = (24 * 60 * 60 * 1000);

            Calendar calendar = Calendar.getInstance();
            calendar.setTimeInMillis(System.currentTimeMillis() - oneDay);

            Key iterKey;
            while (iter.hasNext()) {
                iterKey = iter.next().getKey();
                cf = iterKey.getColumnFamily().toString();
                try {
                    dateObj = DateHelper.parseTimeExactToSeconds(cf);
                    Date dateObjNext = DateHelper.addHours(dateObj, 1);
                    if (calendar.getTime().compareTo(dateObj) > 0) {
                        // remove the entries older than 24 hrs. If we are restarting after a long pause,
                        // then those entries will be removed following successful access.
                        keysToRemove.add(iterKey);

                    }

                    ranges.add(new Range(new Key(new Text("IngestJob_" + outFormat.format(dateObj))),
                                    new Key(new Text("IngestJob_" + outFormat.format(dateObjNext)))));
                } catch (DateTimeParseException e) {
                    log.error(e);
                }

            }
        }

        Collection<IteratorSetting.Column> columns = new ArrayList<>();
        columns.add(new IteratorSetting.Column("e"));
        columns.add(new IteratorSetting.Column("info"));

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setMapperClass(ProcessingErrorsMapper.class);

        job.setReducerClass(ProcessingErrorsReducer.class);
        job.setNumReduceTasks(1);

        Properties clientProps = Accumulo.newClientProperties().to(conf.get(MetricsConfig.WAREHOUSE_INSTANCE), conf.get(MetricsConfig.WAREHOUSE_ZOOKEEPERS))
                        .as(conf.get(MetricsConfig.WAREHOUSE_USERNAME), conf.get(MetricsConfig.WAREHOUSE_PASSWORD, ""))
                        .batchWriterConfig(new BatchWriterConfig().setMaxLatency(25, TimeUnit.MILLISECONDS)).build();

        job.setInputFormatClass(AccumuloInputFormat.class);
        job.setOutputFormatClass(AccumuloOutputFormat.class);

        try (AccumuloClient client = Accumulo.newClient().from(clientProps).build()) {
            // @formatter;off
            AccumuloInputFormat.configure().clientProperties(clientProps).table(conf.get(MetricsConfig.ERRORS_TABLE, MetricsConfig.DEFAULT_ERRORS_TABLE))
                            .auths(client.securityOperations().getUserAuthorizations(client.whoami())).fetchColumns(columns).ranges(ranges).store(job);

            AccumuloOutputFormat.configure().clientProperties(clientProps).createTables(createTables).defaultTable(outTable).store(job);
            // @formatter:on
        }

        if (job.waitForCompletion(true)) {
            if (!keysToRemove.isEmpty()) {
                bwConfig = new BatchWriterConfig().setMaxLatency(1024L, TimeUnit.MILLISECONDS).setMaxMemory(1024L).setMaxWriteThreads(8);
                try (AccumuloClient client = Connections.metricsClient(conf);
                                BatchWriter writer = client.createBatchWriter(conf.get(MetricsConfig.ERRORS_TABLE, MetricsConfig.DEFAULT_ERRORS_TABLE),
                                                bwConfig)) {

                    Mutation m = new Mutation("metrics");
                    for (Key key : keysToRemove) {

                        m.putDelete(key.getColumnFamily(), key.getColumnQualifier());

                    }
                    writer.addMutation(m);
                }
            }
        }
        return 0;
    }

    /*
     * Goes through the arguments and attempts to add relevant values to the configuration
     */
    private void _configure(String[] args) {
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd;
        try {
            cmd = parser.parse(new MetricsOptions(), args);
        } catch (ParseException e) {
            log.warn("Could not parse command line options. Defaults from metrics.xml will be used.", e);
            return;
        }

        Configuration conf = getConf();
        URL metricsConfig = MetricsIngester.class.getClassLoader().getResource("metrics.xml");
        if (metricsConfig != null) {
            conf.addResource(metricsConfig);
        }
        for (Option opt : cmd.getOptions()) {
            conf.set(MetricsConfig.MTX + opt.getOpt(), opt.getValue());
        }
    }

    public static void main(String[] args) throws Exception {

        System.exit(ToolRunner.run(new MetricsIngester(), args));
    }

}
