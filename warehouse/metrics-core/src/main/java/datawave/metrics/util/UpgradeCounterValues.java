package datawave.metrics.util;

import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import static java.util.Map.Entry;

/**
 * Upgrades the serialized counters in the values of the supplied table from Hadoop 0/1.x wire format to Hadoop 2.x wire format. This class has no real way to
 * tell if the value is actually a counters object, so it is up to the called to ensure the correct range is passed to ensure only value objects are returned by
 * the scanned values.
 */
public class UpgradeCounterValues {
    
    private Options options;
    private Option instanceNameOpt, zookeeperOpt, usernameOpt, passwordOpt, tableNameOpt, rangesOpt, bsThreadsOpt, bwThreadsOpt, bwMemoryOpt;
    private String instanceName, zookeepers, username, password, tableName;
    private int bsThreads = 10;
    private int bwThreads = 10;
    private long bwMemory = 10 * 1048576;
    private ArrayList<Range> ranges;
    
    public UpgradeCounterValues() {
        generateCommandLineOptions();
    }
    
    protected void run(String[] args) throws ParseException, AccumuloSecurityException, AccumuloException, TableNotFoundException, IOException {
        parseConfig(args);
        
        try (AccumuloClient client = Accumulo.newClient().to(instanceName, zookeepers).as(username, password).build()) {
            Authorizations auths = client.securityOperations().getUserAuthorizations(client.whoami());
            try (BatchWriter writer = client.createBatchWriter(tableName,
                            new BatchWriterConfig().setMaxWriteThreads(bwThreads).setMaxMemory(bwMemory).setMaxLatency(60, TimeUnit.SECONDS));
                            BatchScanner scanner = client.createBatchScanner(tableName, auths, bsThreads)) {
                scanner.setRanges(ranges);
                
                for (Entry<Key,Value> entry : scanner) {
                    Key key = entry.getKey();
                    
                    ByteArrayDataInput in = ByteStreams.newDataInput(entry.getValue().get());
                    Counters counters = new Counters();
                    try {
                        counters.readFields(in);
                    } catch (IOException e) {
                        // The IO exception means the counters are in the wrong format. We *assume* that they are in
                        // the old (CDH3) format, and de-serialize according to that, and re-write the key with the new value.
                        in = ByteStreams.newDataInput(entry.getValue().get());
                        int numGroups = in.readInt();
                        while (numGroups-- > 0) {
                            String groupName = Text.readString(in);
                            String groupDisplayName = Text.readString(in);
                            CounterGroup group = counters.addGroup(groupName, groupDisplayName);
                            
                            int groupSize = WritableUtils.readVInt(in);
                            for (int i = 0; i < groupSize; i++) {
                                String counterName = Text.readString(in);
                                String counterDisplayName = counterName;
                                if (in.readBoolean())
                                    counterDisplayName = Text.readString(in);
                                long value = WritableUtils.readVLong(in);
                                group.addCounter(counterName, counterDisplayName, value);
                            }
                        }
                        
                        ByteArrayDataOutput out = ByteStreams.newDataOutput();
                        counters.write(out);
                        Mutation m = new Mutation(key.getRow());
                        m.put(key.getColumnFamily(), key.getColumnQualifier(), key.getColumnVisibilityParsed(), key.getTimestamp() + 1,
                                        new Value(out.toByteArray()));
                        writer.addMutation(m);
                    }
                }
                
            }
        }
    }
    
    private void parseConfig(String[] args) throws ParseException {
        CommandLine cl = new BasicParser().parse(options, args);
        instanceName = cl.getOptionValue(instanceNameOpt.getOpt());
        zookeepers = cl.getOptionValue(zookeeperOpt.getOpt());
        username = cl.getOptionValue(usernameOpt.getOpt());
        password = cl.getOptionValue(passwordOpt.getOpt());
        tableName = cl.getOptionValue(tableNameOpt.getOpt());
        ranges = new ArrayList<>();
        if (!cl.hasOption(rangesOpt.getOpt())) {
            System.out.println("NOTE: no ranges specified on the command line. Scanning the entire table.");
            ranges.add(new Range());
        } else {
            for (String rangeStr : cl.getOptionValues(rangesOpt.getOpt())) {
                String[] startEnd = rangeStr.split("\\s*,\\s*");
                ranges.add(new Range(startEnd[0], false, startEnd[1], false));
            }
            System.out.println("Using ranges: " + ranges);
        }
        
        if (cl.hasOption(bsThreadsOpt.getOpt()))
            bsThreads = Integer.parseInt(cl.getOptionValue(bsThreadsOpt.getOpt()));
        if (cl.hasOption(bwThreadsOpt.getOpt()))
            bwThreads = Integer.parseInt(cl.getOptionValue(bwThreadsOpt.getOpt()));
        if (cl.hasOption(bwMemoryOpt.getOpt()))
            bwMemory = Long.parseLong(cl.getOptionValue(bwMemoryOpt.getOpt()));
    }
    
    private void generateCommandLineOptions() {
        options = new Options();
        
        tableNameOpt = new Option("tn", "tableName", true, "The name of the table to scan");
        tableNameOpt.setRequired(true);
        options.addOption(tableNameOpt);
        
        instanceNameOpt = new Option("i", "instance", true, "Accumulo instance name");
        instanceNameOpt.setArgName("name");
        instanceNameOpt.setRequired(true);
        options.addOption(instanceNameOpt);
        
        zookeeperOpt = new Option("zk", "zookeeper", true, "Comma-separated list of ZooKeeper servers");
        zookeeperOpt.setArgName("server[,server]");
        zookeeperOpt.setRequired(true);
        options.addOption(zookeeperOpt);
        
        usernameOpt = new Option("u", "username", true, "Accumulo username");
        usernameOpt.setArgName("name");
        usernameOpt.setRequired(true);
        options.addOption(usernameOpt);
        
        passwordOpt = new Option("p", "password", true, "Accumulo password");
        passwordOpt.setArgName("passwd");
        passwordOpt.setRequired(true);
        options.addOption(passwordOpt);
        
        rangesOpt = new Option("r", "range", true, "Range in startRow,endRow format (both start and end are exclusive");
        rangesOpt.setArgName("range");
        rangesOpt.setRequired(false);
        options.addOption(rangesOpt);
        
        bsThreadsOpt = new Option("st", "bwThreads", true, "Number of batch writer threads");
        bsThreadsOpt.setArgName("bwThreads");
        bsThreadsOpt.setRequired(false);
        options.addOption(bsThreadsOpt);
        
        bwThreadsOpt = new Option("wt", "bwThreads", true, "Number of batch writer threads");
        bwThreadsOpt.setArgName("bwThreads");
        bwThreadsOpt.setRequired(false);
        options.addOption(bwThreadsOpt);
        
        bwMemoryOpt = new Option("wm", "bwMemory", true, "Bytes to keep before flushing the batch writer");
        bwMemoryOpt.setArgName("bwMemory");
        bwMemoryOpt.setRequired(false);
        options.addOption(bwMemoryOpt);
    }
    
    public static void main(String[] args) throws Exception {
        new UpgradeCounterValues().run(args);
    }
}
