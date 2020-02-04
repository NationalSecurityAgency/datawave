package datawave.ingest.util;

import datawave.util.StringUtils;
import datawave.util.cli.PasswordConverter;
import datawave.util.time.DateHelper;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.server.client.HdfsZooInstance;
import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.io.Text;

import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

/**
 * Generates split points for the specified table and optionally adds them to the table This class creates split points of the form: yyyyMMdd_N In addition this
 * will create maker key/values for the specified data types if requested
 */
public class GenerateShardSplits {
    
    private static final ColumnVisibility EMPTY_VIS = new ColumnVisibility();
    private static final Text EMPTY_TEXT = new Text();
    private static final Value EMPTY_VALUE = new Value(EMPTY_TEXT.getBytes());
    
    private static void printUsageAndExit() {
        System.out.println("Usage: datawave.ingest.util.GenerateShardSplits <startDate (yyyyMMDD)> <daysToGenerate> <numShardsPerDay> <numShardsPerSplit> [-markersOnly] [-addShardMarkers] [-addDataTypeMarkers <comma delim data types>] [<username> <password> <tableName> [<instanceName> <zookeepers>]]");
        System.exit(-1);
    }
    
    public static void main(String[] args) throws Exception {
        
        if (args.length < 3) {
            printUsageAndExit();
        }
        // parse out the args
        Date startDate = null;
        int DAYS_TO_GENERATE = -1;
        int SHARDS = -1;
        int splitStep = 1;
        boolean addSplits = true;
        boolean addShardMarkers = false;
        String[] shardMarkerTypes = null;
        String username = null;
        byte[] password = null;
        String tableName = null;
        String instanceName = null;
        String zookeepers = null;
        for (int i = 0; i < args.length; i++) {
            if (i == 0) {
                try {
                    startDate = DateHelper.parse(args[i]);
                } catch (DateTimeParseException e) {
                    System.out.println("Start Date does not match format. Exception=" + e.getMessage());
                    System.exit(-2);
                }
            } else if (i == 1) {
                try {
                    DAYS_TO_GENERATE = Integer.parseInt(args[i]);
                } catch (NumberFormatException e) {
                    System.out.println("Days to Generate argument is not an integer:" + e.getMessage());
                    System.exit(-2);
                }
            } else if (i == 2) {
                try {
                    SHARDS = Integer.parseInt(args[i]);
                } catch (NumberFormatException e) {
                    System.out.println("Shards argument is not an integer:" + e.getMessage());
                    System.exit(-2);
                }
            } else if (i == 3) {
                try {
                    splitStep = Integer.parseInt(args[i]);
                } catch (NumberFormatException e) {
                    System.out.println("Split Step argument is not an integer:" + e.getMessage());
                    System.exit(-2);
                }
            } else if (args[i].equals("-markersOnly")) {
                addSplits = false;
            } else if (args[i].equals("-addShardMarkers")) {
                addShardMarkers = true;
            } else if (args[i].equals("-addDataTypeMarkers")) {
                shardMarkerTypes = StringUtils.split(args[i + 1], ',');
                // skip over cmd, for loop will skip over arg
                i++;
            } else {
                // need at least 3 more args
                if (i + 3 > args.length) {
                    printUsageAndExit();
                } else {
                    username = args[i];
                    password = PasswordConverter.parseArg(args[i + 1]).getBytes();
                    tableName = args[i + 2];
                    // skip over args
                    i += 3;
                }
                // if we still have args
                if (i < args.length) {
                    // then we need exactly 2 more args
                    if (i + 2 != args.length) {
                        printUsageAndExit();
                    } else {
                        instanceName = args[i];
                        zookeepers = args[i + 1];
                        // skip over args to terminate loop
                        i += 2;
                    }
                }
            }
        }
        
        SortedSet<Text> splits = new TreeSet<>();
        List<Mutation> mutations = new ArrayList<>();
        for (int x = 0; x < DAYS_TO_GENERATE; x++) {
            
            // Generate configured shards per day
            for (int i = 0; i < SHARDS; i += splitStep) {
                Text split = new Text(DateHelper.format(startDate) + "_" + i);
                splits.add(split);
                
                // add markers as required
                if (addShardMarkers || shardMarkerTypes != null) {
                    Date nextYear = DateUtils.addYears(startDate, 1);
                    Mutation m = new Mutation(split);
                    if (addShardMarkers) {
                        m.put(EMPTY_TEXT, EMPTY_TEXT, EMPTY_VIS, nextYear.getTime(), EMPTY_VALUE);
                    }
                    if (shardMarkerTypes != null) {
                        for (String type : shardMarkerTypes) {
                            type = type.trim();
                            if (!type.isEmpty()) {
                                m.put(new Text(type), EMPTY_TEXT, EMPTY_VIS, nextYear.getTime(), EMPTY_VALUE);
                            }
                        }
                    }
                    if (m.size() > 0) {
                        mutations.add(m);
                    }
                }
            }
            
            startDate = DateUtils.addDays(startDate, 1);
        }
        
        if (username != null) {
            // Connect to accumulo
            ClientConfiguration zkConfig = ClientConfiguration.loadDefault().withInstance(instanceName).withZkHosts(zookeepers);
            Instance instance = (instanceName != null ? new ZooKeeperInstance(zkConfig) : HdfsZooInstance.getInstance());
            Connector connector = instance.getConnector(username, new PasswordToken(password));
            
            // add the splits
            if (addSplits) {
                connector.tableOperations().addSplits(tableName, splits);
            }
            
            // add the markers
            if (!mutations.isEmpty()) {
                BatchWriter w = connector.createBatchWriter(tableName, new BatchWriterConfig().setMaxLatency(1, TimeUnit.SECONDS).setMaxMemory(100000L)
                                .setMaxWriteThreads(4));
                try {
                    w.addMutations(mutations);
                } finally {
                    w.close();
                }
            }
        } else {
            if (addSplits) {
                for (Text t : splits) {
                    System.out.println(t);
                }
            }
            for (Mutation m : mutations) {
                for (ColumnUpdate update : m.getUpdates()) {
                    System.out.println(new String(m.getRow()) + ' ' + new String(update.getColumnFamily()) + ':' + new String(update.getColumnQualifier())
                                    + " [" + new String(update.getColumnVisibility()) + "] " + new Date(update.getTimestamp()) + " -> "
                                    + new String(update.getValue()));
                }
            }
            
        }
    }
}
