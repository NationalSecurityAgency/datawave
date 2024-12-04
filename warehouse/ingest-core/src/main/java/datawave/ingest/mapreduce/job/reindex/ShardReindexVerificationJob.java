package datawave.ingest.mapreduce.job.reindex;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.hadoop.mapreduce.AccumuloFileOutputFormat;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.ZStandardCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import datawave.ingest.mapreduce.job.util.AccumuloUtil;
import datawave.ingest.mapreduce.job.util.RFileUtil;
import datawave.ingest.mapreduce.job.util.SplittableRFileRangeInputFormat;

/**
 * Verify the equivalence of two inputs. Primarily used for verifying ShardReindexJob correctness
 */
public class ShardReindexVerificationJob implements Tool {
    private static final Logger log = Logger.getLogger(ShardReindexVerificationJob.class);

    private Configuration configuration;
    private JobConfig jobConfig = new JobConfig();
    private AccumuloClient accumuloClient;

    public static void main(String[] args) throws Exception {
        log.info("Running ShardReindexVerificationJob");
        System.exit(ToolRunner.run(null, new ShardReindexVerificationJob(), args));
    }

    @Override
    public int run(String[] args) throws Exception {
        JCommander cmd = JCommander.newBuilder().addObject(jobConfig).build();
        cmd.parse(args);

        // setup Job
        Job j = Job.getInstance(getConf());

        setupJob(j);

        if (j.waitForCompletion(true)) {
            return 0;
        }

        // failure
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

    private void setupJob(Job j) throws IOException {
        configureJobCache(j);

        configureSources(j);

        configureSplits(j);

        configureJob(j);

        cleanup();
    }

    private void configureSplits(Job j) {
        Configuration config = j.getConfiguration();
        if (jobConfig.source1Type == SourceType.FILE) {
            log.info("Selecting source1 for split files");
            SplittableRFileRangeInputFormat.setSplitFiles(j, config.get("source1.files"));
        } else if (jobConfig.source2Type == SourceType.FILE) {
            log.info("Selecting source2 for split files");
            SplittableRFileRangeInputFormat.setSplitFiles(j, config.get("source2.files"));
        } else {
            // use the source1 accumulo
            if (accumuloClient == null) {
                accumuloClient = AccumuloUtil.setupAccumuloClient(j.getConfiguration(), jobConfig.accumuloClientPropertiesPath, jobConfig.instance,
                                jobConfig.zookeepers, jobConfig.username, getPassword());
            }

            AccumuloUtil accumuloUtil = new AccumuloUtil();
            accumuloUtil.setAccumuloClient(accumuloClient);

            RFileUtil rFileUtil = new RFileUtil(config);

            ShardReindexJob.buildSplittableRanges(accumuloUtil, rFileUtil, jobConfig.splitThreads, jobConfig.indexBlocksPerSplit,
                            ShardReindexMapper.BatchMode.NONE, jobConfig.source1Table, jobConfig.startKey, jobConfig.endKey);
        }
    }

    private void configureSources(Job j) {
        verifyAndSetSource(1, j, jobConfig.source1Type, jobConfig.source1Table, jobConfig.source1Paths);
        verifyAndSetSource(2, j, jobConfig.source2Type, jobConfig.source2Table, jobConfig.source2Paths);
    }

    private String getPassword() {
        if (jobConfig.password.toLowerCase().startsWith("env:")) {
            return System.getenv(jobConfig.password.substring(4));
        }

        return jobConfig.password;
    }

    private void verifyAndSetSource(int sourceNum, Job j, SourceType sourceType, String table, String paths) {
        if (sourceType == SourceType.ACCUMULO) {
            if (paths != null) {
                throw new IllegalArgumentException("Cannot set sourcePaths with sourceType ACCUMULO");
            } else {
                if (table == null) {
                    throw new IllegalArgumentException("sourceTable must be set with sourceType ACCUMULO");
                } else if (jobConfig.username == null || jobConfig.password == null || jobConfig.instance == null || jobConfig.zookeepers == null) {
                    throw new IllegalArgumentException("username, password, instance, and zookeepers are required when using sourceType ACCUMULO");
                }

                if (accumuloClient == null) {
                    accumuloClient = AccumuloUtil.setupAccumuloClient(j.getConfiguration(), jobConfig.accumuloClientPropertiesPath, jobConfig.instance,
                                    jobConfig.zookeepers, jobConfig.username, getPassword());
                }

                // verify the table exists
                try {
                    accumuloClient.tableOperations().getTableProperties(table);
                } catch (AccumuloException | TableNotFoundException e) {
                    throw new IllegalArgumentException("Table does not exist " + table, e);
                }

                Configuration config = j.getConfiguration();
                config.set("source" + sourceNum, SourceType.ACCUMULO.name());
                config.set("source" + sourceNum + ".table", table);
            }
        } else if (sourceType == SourceType.FILE) {
            if (table != null) {
                throw new IllegalArgumentException("Cannot set table with sourceType FILE");
            }

            if (paths == null) {
                throw new IllegalArgumentException("sourcePaths must be set with sourceType FILE");
            }

            log.info("processing source paths: " + paths);

            // convert all paths to files and verify they exist
            String[] pathSplits = paths.split(",");
            Configuration config = j.getConfiguration();
            List<Path> filePaths = new ArrayList<>();
            try {
                for (String path : pathSplits) {
                    Path p = new Path(path);
                    FileSystem fs = p.getFileSystem(config);
                    FileStatus status = fs.getFileStatus(p);
                    if (!fs.exists(p)) {
                        FileStatus[] globStatues = fs.globStatus(p);
                        for (FileStatus fileStatus : globStatues) {
                            filePaths.add(fileStatus.getPath());
                        }
                    } else if (status.isDirectory()) {
                        // add all children
                        FileStatus[] globStatues = fs.globStatus(new Path(p, "*"));
                        for (FileStatus fileStatus : globStatues) {
                            filePaths.add(fileStatus.getPath());
                        }
                    } else {
                        filePaths.add(p);
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to expand path: " + paths, e);
            }

            if (filePaths.isEmpty()) {
                throw new IllegalArgumentException("No files found for source " + sourceNum + " at " + paths);
            }

            String filePathProperty = StringUtils.join(filePaths, ',');
            log.info("setting source" + sourceNum + ".files: " + filePathProperty);

            config.set("source" + sourceNum, SourceType.FILE.name());
            config.set("source" + sourceNum + ".files", filePathProperty);
        } else {
            throw new IllegalStateException("Unexpected sourceType: " + sourceType.name());
        }
    }

    private void configureJobCache(Job j) throws IOException {
        String[] jarNames = datawave.util.StringUtils.trimAndRemoveEmptyStrings(jobConfig.cacheJars.split("\\s*,\\s*"));
        for (String jarName : jarNames) {
            File jar = new File(jarName);
            Path path = new Path(jobConfig.cacheDir, jar.getName());
            log.info("adding job cache jar: " + jar);
            j.addFileToClassPath(path);
        }

        j.setJarByClass(this.getClass());
    }

    private void configureJob(Job j) {
        // setup input format
        j.setInputFormatClass(SplittableRFileRangeInputFormat.class);
        SplittableRFileRangeInputFormat.setStartKey(j, jobConfig.startKey);
        SplittableRFileRangeInputFormat.setEndKey(j, jobConfig.endKey);
        SplittableRFileRangeInputFormat.setIndexBlocksPerSplit(j, jobConfig.indexBlocksPerSplit);

        // setup mappers
        j.setMapperClass(ShardReindexVerificationMapper.class);
        j.setOutputKeyClass(Key.class);
        j.setOutputValueClass(Value.class);

        // setup reducers (map only job)
        j.setNumReduceTasks(0);

        // setup output format
        j.setOutputFormatClass(AccumuloFileOutputFormat.class);
        AccumuloFileOutputFormat.setOutputPath(j, new Path(jobConfig.outputDir));
        AccumuloFileOutputFormat.setCompressOutput(j, true);
        AccumuloFileOutputFormat.setOutputCompressorClass(j, ZStandardCodec.class);
    }

    private void cleanup() {
        if (accumuloClient != null) {
            accumuloClient.close();
        }
    }

    private class JobConfig {
        // distributed cache properties
        @Parameter(names = "--cacheDir", description = "HDFS path to cache directory", required = true)
        private String cacheDir;

        @Parameter(names = "--cacheJars", description = "jars located in the cacheDir to add to the classpath and distributed cache", required = true)
        private String cacheJars;

        // define sources
        @Parameter(names = "--source1Type", description = "either FILE or ACCUMULO", required = true)
        private SourceType source1Type;

        @Parameter(names = "--source1Table", description = "if source1Type is ACCUMULO define a source table")
        private String source1Table;

        @Parameter(names = "--source1Paths", description = "comma delimited files/directories to include for source1, must be specified with source1Type=FILE")
        private String source1Paths;

        @Parameter(names = "--source2Type", description = "either FILE or ACCUMULO", required = true)
        private SourceType source2Type;

        @Parameter(names = "--source2Table", description = "if source2Type is ACCUMULO define a source table")
        private String source2Table;

        @Parameter(names = "--source2Paths", description = "comma delimited files/directories to include for source1, must be specified with source2Type=FILE")
        private String source2Paths;

        // define output
        @Parameter(names = "--outputDir", description = "where to store the result of the job", required = true)
        private String outputDir;

        // define data bounds
        @Parameter(names = "--startKey", description = "the start key bound on the sources, if null the start range is unbounded")
        private String startKey;

        @Parameter(names = "--endKey", description = "the end key bound on the sources, if null the end range is unbounded")
        private String endKey;

        // define data splits
        @Parameter(names = "--indexBlocksPerSplit", description = "rfile index block count to use for each split, -1 to disable indexBlockSplitting")
        private int indexBlocksPerSplit = -1;

        @Parameter(names = "--splitThreads", description = "number of threads to use to calculate splits when paired with indexBlocksPerSplit != -1")
        private int splitThreads = 1;

        // define accumulo config, requried if either source is type ACCUMULO
        @Parameter(names = "--instance", description = "accumulo instance name")
        private String instance;

        @Parameter(names = "--zookeepers", description = "accumulo zookeepers")
        private String zookeepers;

        @Parameter(names = "--username", description = "accumulo username")
        private String username;

        @Parameter(names = "--password", description = "accumulo password")
        private String password;

        @Parameter(names = "--accumuloClientPropertiesPath", description = "Filesystem path to accumulo-client.properties to apply when creating a client")
        private String accumuloClientPropertiesPath;

        @Parameter(names = "--useScanServers", description = "When using sourceType ACCUMULO use scan server resources")
        private boolean useScanServers = false;

        @Parameter(names = "--resourceGroup", description = "When using sourceType ACCUMULO set scan_type hint to this value")
        private String resourceGroup;

        @Parameter(names = "--offline", description = "When using sourceType ACCUMULO use an offline scanner, table must be offline")
        private boolean offline = false;
    }

    public enum SourceType {
        FILE, ACCUMULO
    }
}
