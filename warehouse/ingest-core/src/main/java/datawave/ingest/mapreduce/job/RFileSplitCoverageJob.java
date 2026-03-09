package datawave.ingest.mapreduce.job;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import datawave.ingest.data.config.ingest.AccumuloHelper;

/**
 * MapReduce job that analyzes RFiles in a bulk import directory to determine how each RFile's key range maps to the current table splits in Accumulo.
 *
 * <p>
 * For each RFile, the job reports the number and percentage of table splits that the file's row range covers. This is useful for understanding how well bulk
 * import files are aligned with the table's tablet boundaries.
 *
 * <p>
 * Usage: {@code RFileSplitCoverageJob <inputDir> <tableName> <outputDir> [configFile ...]}
 *
 * <p>
 * Accumulo connection info is read from the configuration files via {@link AccumuloHelper}.
 */
public class RFileSplitCoverageJob extends Configured implements Tool {

    private static final Logger log = Logger.getLogger(RFileSplitCoverageJob.class);

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(null, new RFileSplitCoverageJob(), args));
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 3) {
            printUsage();
            return -1;
        }

        String inputDir = args[0];
        String tableName = args[1];
        String outputDir = args[2];

        Configuration conf = getConf();
        if (conf == null) {
            conf = new Configuration();
        }

        // load any additional config files
        for (int i = 3; i < args.length; i++) {
            log.info("Adding resource: " + args[i]);
            conf.addResource(args[i]);
        }

        // fetch splits from Accumulo
        AccumuloHelper accumuloHelper = new AccumuloHelper();
        accumuloHelper.setup(conf);

        List<Text> splits;
        try (AccumuloClient client = accumuloHelper.newClient()) {
            Collection<Text> splitCollection = client.tableOperations().listSplits(tableName);
            splits = new ArrayList<>(splitCollection);
        }
        log.info("Fetched " + splits.size() + " splits for table " + tableName);

        // serialize splits into config
        String serializedSplits = RFileSplitCoverageMapper.serializeSplits(splits);
        conf.set(RFileSplitCoverageMapper.SPLITS_CONFIG_KEY, serializedSplits);

        // set up the job
        Job job = Job.getInstance(conf, "RFile Split Coverage: " + tableName);
        job.setJarByClass(RFileSplitCoverageJob.class);

        // input
        FileInputFormat.addInputPath(job, new Path(inputDir));
        job.setInputFormatClass(RFileInputFormat.class);

        // mapper
        job.setMapperClass(RFileSplitCoverageMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        // no reducers - map only
        job.setNumReduceTasks(0);

        // output
        FileOutputFormat.setOutputPath(job, new Path(outputDir));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        boolean success = job.waitForCompletion(true);
        return success ? 0 : -1;
    }

    private void printUsage() {
        System.err.println("Usage: " + getClass().getSimpleName() + " <inputDir> <tableName> <outputDir> [configFile ...]");
        System.err.println();
        System.err.println("  inputDir    - HDFS path to the bulk import table directory containing .rf files");
        System.err.println("  tableName   - Accumulo table name to fetch current splits for");
        System.err.println("  outputDir   - HDFS output directory for results");
        System.err.println("  configFile  - optional Hadoop config files providing Accumulo connection info");
        System.err.println("                (accumulo.instance.name, accumulo.zookeepers, accumulo.username, accumulo.password)");
    }
}
