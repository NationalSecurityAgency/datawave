package datawave.ingest.mapreduce.job.reindex;

import static datawave.ingest.mapreduce.job.SplitsFile.SPLIT_WORK_DIR;
import static datawave.ingest.mapreduce.job.reduce.BulkIngestKeyDedupeCombiner.CONTEXT_WRITER_CLASS;
import static datawave.ingest.mapreduce.job.reduce.BulkIngestKeyDedupeCombiner.USING_COMBINER;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.hadoop.mapreduce.AccumuloFileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.ZStandardCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.RootLogger;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.collect.Multimap;

import datawave.ingest.data.config.ingest.AccumuloHelper;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.mapreduce.job.DelegatingPartitioner;
import datawave.ingest.mapreduce.job.IngestJob;
import datawave.ingest.mapreduce.job.RFileInputFormat;
import datawave.ingest.mapreduce.job.SplitsFile;
import datawave.ingest.mapreduce.job.reduce.BulkIngestKeyDedupeCombiner;
import datawave.ingest.mapreduce.job.writer.AbstractContextWriter;
import datawave.ingest.mapreduce.job.writer.ContextWriter;
import datawave.ingest.mapreduce.partition.MultiTableRangePartitioner;
import datawave.util.StringUtils;

/**
 * Can be run against rfiles for a table that need to be repartitioned. Will optionally apply the configured combiners to map and reduce tasks
 */
public class RepartitionerJob implements Tool {
    private static final Logger log = Logger.getLogger(RepartitionerJob.class);

    private Configuration configuration;
    private final JobConfig jobConfig = new JobConfig();

    private void setupJobCache(Job j) throws IOException {
        String[] jarNames = StringUtils.trimAndRemoveEmptyStrings(jobConfig.cacheJars.split("\\s*,\\s*"));
        for (String jarName : jarNames) {
            File jar = new File(jarName);
            Path path = new Path(jobConfig.cacheDir, jar.getName());
            log.info("jar: " + jar);
            j.addFileToClassPath(path);
        }

        j.setJarByClass(this.getClass());
    }

    private void updateConfig() throws URISyntaxException, IOException {
        if (jobConfig.resources != null) {
            log.info("adding resources");
            String[] resources = StringUtils.trimAndRemoveEmptyStrings(jobConfig.resources.split("\\s*,\\s*"));
            for (String resource : resources) {
                log.info("Added resource: " + resource);
                configuration.addResource(resource);
            }
        }

        // configure the accumulo helper
        AccumuloHelper.setInstanceName(configuration, jobConfig.instance);
        AccumuloHelper.setPassword(configuration, getPassword().getBytes());
        AccumuloHelper.setUsername(configuration, jobConfig.username);
        AccumuloHelper.setZooKeepers(configuration, jobConfig.zookeepers);

        // set the work dir
        configuration.set(SPLIT_WORK_DIR, jobConfig.workDir);

        // required for MultiTableRangePartitioner
        configuration.set("ingest.work.dir.qualified", FileSystem.get(new URI(jobConfig.sourceHdfs), configuration).getUri().toString() + jobConfig.workDir);
        configuration.set("output.fs.uri", FileSystem.get(new URI(jobConfig.destHdfs), configuration).getUri().toString());

        // set the configured table
        configuration.set("table", jobConfig.table);
    }

    private Job setupJob() throws URISyntaxException, IOException, AccumuloException, TableNotFoundException, AccumuloSecurityException {
        updateConfig();

        Job j = Job.getInstance(getConf());
        Configuration config = j.getConfiguration();

        // setup and cache table from config
        SplitsFile.setupFile(j, config);
        Set<String> tableNames = IngestJob.setupAndCacheTables(config, false);
        config.setInt("splits.num.reduce", jobConfig.reducers);

        setupJobCache(j);

        DelegatingPartitioner.configurePartitioner(j, config, tableNames.toArray(new String[0]));

        RFileInputFormat.addInputPaths(j, jobConfig.inputDir);
        j.setInputFormatClass(RFileInputFormat.class);
        j.setMapperClass(BulkIngestKeyMapper.class);
        j.setMapOutputKeyClass(BulkIngestKey.class);
        j.setMapOutputValueClass(Value.class);

        j.setNumReduceTasks(jobConfig.reducers);
        j.setReducerClass(BulkIngestKeyReducer.class);
        j.setOutputKeyClass(Key.class);
        j.setOutputValueClass(Value.class);

        if (jobConfig.combiner) {
            config.setBoolean(USING_COMBINER, true);
            j.setCombinerClass(BulkIngestKeyDedupeCombiner.class);
        }

        AccumuloFileOutputFormat.setCompressOutput(j, true);
        AccumuloFileOutputFormat.setOutputCompressorClass(j, ZStandardCodec.class);
        AccumuloFileOutputFormat.setOutputPath(j, new Path(jobConfig.outputDir));
        j.setOutputFormatClass(AccumuloFileOutputFormat.class);

        return j;
    }

    private String getPassword() {
        if (jobConfig.password.toLowerCase().startsWith("env:")) {
            return System.getenv(jobConfig.password.substring(4));
        }

        return jobConfig.password;
    }

    public static void main(String[] args) throws Exception {
        log.info("Running job");

        System.exit(ToolRunner.run(null, new RepartitionerJob(), args));
    }

    @Override
    public int run(String[] args) throws Exception {
        JCommander cmd = JCommander.newBuilder().addObject(jobConfig).build();
        cmd.parse(args);

        Job j = setupJob();

        if (j.waitForCompletion(true)) {
            return 0;
        }

        return -1;
    }

    @Override
    public void setConf(Configuration configuration) {
        this.configuration = configuration;
    }

    @Override
    public Configuration getConf() {
        return configuration;
    }

    // wrap all input into BulkIngestKey applying the configured table for partitioning and combining
    private static class BulkIngestKeyMapper extends Mapper<Key,Value,BulkIngestKey,Value> {
        private Text table;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);

            table = new Text(context.getConfiguration().get("table"));
            log.info("Applying table: " + table);

            // wire up the partitioner to the context
            MultiTableRangePartitioner.setContext(context);

            // move logging to the warn level, too much noise created for these tasks
            RootLogger.getRootLogger().setLevel(Level.WARN);
        }

        @Override
        protected void map(Key key, Value value, Context context) throws IOException, InterruptedException {
            // wrap the key up as a bulk ingest key so it can be partitioned and combined
            context.write(new BulkIngestKey(table, key), value);
        }
    }

    // optionally apply combiner and strip off the BulkIngestKey
    private static class BulkIngestKeyReducer extends Reducer<BulkIngestKey,Value,Key,Value> {
        private BulkIngestKeyDedupeCombiner<Key,Value> combiner = null;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration config = context.getConfiguration();
            if (config.getBoolean(USING_COMBINER, false)) {
                combiner = new BulkIngestKeyDedupeCombiner<>();
                config.setClass(CONTEXT_WRITER_CLASS, BulkIngestKeyStrippingContextWriter.class, ContextWriter.class);
                combiner.setup(config);
                log.info("Using Combiner");
            }
        }

        @Override
        protected void reduce(BulkIngestKey key, Iterable<Value> values, Context context) throws IOException, InterruptedException {
            if (combiner != null) {
                combiner.doReduce(key, values, context);
            } else {
                // do not combine, pass values through
                for (Value value : values) {
                    context.write(key.getKey(), value);
                }
            }
        }
    }

    // strip off the BulkIngestKey applied for partitioning and combining
    public static class BulkIngestKeyStrippingContextWriter extends AbstractContextWriter<Key,Value> {
        @Override
        protected void flush(Multimap<BulkIngestKey,Value> entries, TaskInputOutputContext<?,?,Key,Value> context) throws IOException, InterruptedException {
            for (Map.Entry<BulkIngestKey,Value> entry : entries.entries()) {
                Key key = entry.getKey().getKey();
                context.write(key, entry.getValue());
            }
        }
    }

    private static class JobConfig {
        @Parameter(names = "--resources", description = "configuration resources to be added")
        private String resources;

        @Parameter(names = "--instance", description = "accumulo instance name", required = true)
        private String instance;

        @Parameter(names = "--zookeepers", description = "accumulo zookeepers", required = true)
        private String zookeepers;

        @Parameter(names = "--username", description = "accumulo username", required = true)
        private String username;

        @Parameter(names = "--password", description = "accumulo password", required = true)
        private String password;

        // support for cache jars
        @Parameter(names = "--cacheDir", description = "HDFS path to cache directory", required = true)
        private String cacheDir;

        @Parameter(names = "--cacheJars", description = "jars located in the cacheDir to add to the classpath and distributed cache", required = true)
        private String cacheJars;

        // work dir
        @Parameter(names = "--workDir", description = "Temporary work location in hdfs", required = true)
        private String workDir;

        @Parameter(names = "--sourceHdfs", description = "HDFS for --inputFiles", required = true)
        private String sourceHdfs;

        @Parameter(names = "--destHdfs", description = "HDFS for --outputDir", required = true)
        private String destHdfs;

        @Parameter(names = "--reducers", description = "number of reducers to use", required = true)
        private int reducers;

        @Parameter(names = "--inputDir", description = "Input files for the job. Should be comma delimited hdfs glob strings", required = true)
        private String inputDir;

        @Parameter(names = "--outputDir", description = "output directory that must not already exist", required = true)
        private String outputDir;

        @Parameter(names = "--table", description = "table this data should be partitioned for", required = true)
        private String table;

        @Parameter(names = "--combiner", description = "apply a datawave combiner")
        private boolean combiner = false;
    }
}
