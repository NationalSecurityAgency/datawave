package nsa.datawave.metrics.util;

import nsa.datawave.metrics.config.MetricsConfig;
import nsa.datawave.poller.metric.InputFile;
import nsa.datawave.poller.metric.OutputFile;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.io.SequenceFile.Reader;

/**
 * A utility that scans a set of directories for poller metrics counters files, serializes them, and stores them in Accumulo. Because the contents of these
 * files are currently not used, the only index to them is to know these three things:
 * 
 * 1) The output file 2) The duration of the processing time on the file 3) The date (ms since the epoch)
 * 
 */
public class PollerMetricsIngester extends Configured {
    
    private final static Logger log = Logger.getLogger(PollerMetricsIngester.class);
    private final static int DEFAULT_BATCH_SIZE = 2000;
    public static final String POLLER_METRICS_INGESTER_BATCH_SIZE = "poller.metrics.ingester.batch.size";
    private final int batchSize;
    
    private FileSystem localFS;
    private BatchWriter writer;
    
    public PollerMetricsIngester(Configuration conf, Connector conn, String outputTable) throws TableNotFoundException, IOException {
        super(conf);
        batchSize = conf.getInt(POLLER_METRICS_INGESTER_BATCH_SIZE, DEFAULT_BATCH_SIZE);
        BatchWriterConfig bwConfig = new BatchWriterConfig();
        bwConfig.setMaxMemory(5L * 1024L * 1024L);
        bwConfig.setMaxLatency(5, TimeUnit.SECONDS);
        bwConfig.setMaxWriteThreads(15);
        writer = conn.createBatchWriter(outputTable, bwConfig);
        localFS = FileSystem.getLocal(getConf());
    }
    
    /**
     * Given a list of directories, this app will process any files that do not end with '.working'.
     */
    public void run(String[] args) throws Exception {
        for (String directory : args) {
            log.info("Processing " + directory + " ...");
            
            Path metricsDir = Paths.get(directory);
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(metricsDir)) {
                processDirectory(stream);
                log.info("Finished processing " + directory + ".");
            }
        }
    }
    
    void processDirectory(DirectoryStream<Path> stream) throws IOException, MutationsRejectedException {
        Counters counters = new Counters();
        int numMetricsFilesIngested = 0;
        ArrayList<Path> filesToDelete = new ArrayList<>(batchSize);
        for (Path file : stream) {
            if (numMetricsFilesIngested++ >= batchSize) {
                counters = new Counters();
                numMetricsFilesIngested = 0;
            }
            // Skip files that are "in progress"
            String fname = file.getFileName().toString();
            if (fname.endsWith(".working") || fname.endsWith(".working.crc") || fname.startsWith("."))
                continue;
            
            // Write mutations for this file to Accumulo and mark for deletion if we're successful
            if (writeMutations(file, counters))
                filesToDelete.add(file);
            
            // Delete files every batchSize increments
            if (filesToDelete.size() >= batchSize)
                deleteAndClear(filesToDelete);
        }
        
        // Delete any leftover files
        if (!filesToDelete.isEmpty())
            deleteAndClear(filesToDelete);
        
    }
    
    /**
     * Reads each file and generates mutations for them. Then, it uploads those mutations to Accumulo.
     */
    public boolean writeMutations(Path file, Counters counters) {
        boolean success = false;
        log.debug("Processing file: " + file);
        org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(file.toString());
        try (Reader rdr = new Reader(getConf(), Reader.file(localFS.makeQualified(path)))) {
            long nRecords = 0;
            
            nRecords = readCountersInFile(counters, rdr, nRecords);
            success = true;
            log.debug("File " + file + " contained " + nRecords + " records.");
        } catch (IOException e) {
            log.error("Could not finish processing file " + file, e);
        }
        return success;
    }
    
    public long readCountersInFile(Counters counters, Reader rdr, long nRecords) throws IOException {
        while (rdr.next(NullWritable.get(), counters)) {
            getMutation(counters);
            ++nRecords;
        }
        return nRecords;
    }
    
    public void getMutation(Counters counters) {
        long endTime = counters.findCounter(InputFile.POLLER_END_TIME.getDeclaringClass().getName(), InputFile.POLLER_END_TIME.toString()).getValue();
        
        long fileTime;
        Iterator<Counter> ftCounter = counters.getGroup(InputFile.class.getSimpleName()).iterator();
        if (ftCounter.hasNext()) {
            fileTime = ftCounter.next().getValue();
        } else {
            log.error("Could not process counters file for input file -- could not retrieve file name or file timestamp.");
            return;
        }
        
        for (Counter c : counters.getGroup(OutputFile.class.getSimpleName())) {
            Text outFile = new Text(c.getName());
            Mutation m = new Mutation(outFile);
            try {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                counters.write(new DataOutputStream(baos));
                m.put(WritableUtil.getLong(endTime - fileTime), WritableUtil.getLong(endTime), new Value(baos.toByteArray()));
                writer.addMutation(m);
            } catch (IOException | MutationsRejectedException e) {
                log.error("Could not add counters to mutation!!!", e);
            }
        }
    }
    
    public void deleteAndClear(ArrayList<Path> files) throws IOException, MutationsRejectedException {
        writer.flush();
        log.info("Finished writing mutations for " + files.size() + " metrics files.");
        for (Path file : files) {
            Files.delete(file);
        }
        log.info("Deleted " + files.size() + " metrics files.");
        files.clear();
    }
    
    public static void main(String[] args) throws Exception {
        String instanceName = null, zooKeepers = null, username = null, outTable = null;
        byte[] password = null;
        Configuration conf = new Configuration();
        
        LinkedList<String> directories = new LinkedList<>();
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-instance":
                    instanceName = args[++i];
                    log.info("Instance: " + instanceName);
                    break;
                case "-zookeepers":
                    zooKeepers = args[++i];
                    log.info("ZooKeepers: " + zooKeepers);
                    break;
                case "-user":
                    username = args[++i];
                    log.info("User: " + username);
                    break;
                case "-password":
                    password = args[++i].getBytes();
                    log.info("Password: " + MD5Hash.digest(password));
                    break;
                case "-outputTable":
                    outTable = args[++i];
                    log.info("Output Table: " + outTable);
                    break;
                case "-batchSize":
                    int batchSize = Integer.valueOf(args[++i]);
                    conf.setInt(POLLER_METRICS_INGESTER_BATCH_SIZE, batchSize);
                    log.info("Override batch size: " + batchSize);
                    break;
                default:
                    directories.add(args[i]);
                    break;
            }
        }
        
        URL metricsConf = PollerMetricsIngester.class.getClassLoader().getResource("metrics.xml");
        if (metricsConf != null) {
            conf.addResource(metricsConf);
        } else {
            log.warn("No metrics.xml found on class path.");
        }
        
        if (outTable == null) {
            outTable = conf.get(MetricsConfig.POLLER_TABLE, MetricsConfig.DEFAULT_POLLER_TABLE);
        }
        
        ZooKeeperInstance inst = new ZooKeeperInstance(ClientConfiguration.loadDefault().withInstance(instanceName).withZkHosts(zooKeepers));
        Connector conn = inst.getConnector(username, new PasswordToken(password));
        
        try {
            if (!conn.tableOperations().exists(outTable)) {
                conn.tableOperations().create(outTable);
            }
        } catch (TableExistsException te) {
            // in this case, somebody else must have created the table after our
            // existence check
            log.info("Tried to create " + outTable + " but somebody beat us to the punch");
        }
        
        PollerMetricsIngester ing = new PollerMetricsIngester(conf, conn, outTable);
        try {
            ing.run(directories.toArray(new String[directories.size()]));
        } catch (Exception e) {
            log.error("Caught an exception while running the poller metrics ingester!", e);
        } finally {
            log.info("Closing batch writer...");
            ing.writer.close();
            log.info("Closed batch writer.");
            
            log.info("Closing file system handle...");
            ing.localFS.close();
            log.info("Closed file system handle.");
        }
    }
}
