package datawave.ingest.util;

import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.Locations;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.io.Text;

import datawave.util.cli.PasswordConverter;
import datawave.util.time.DateHelper;

/**
 * Generates split points for the specified table and optionally adds them to the table This class creates split points of the form: yyyyMMdd_N In addition this
 * will create maker key/values for the specified data types if requested
 */
public class GenerateShardSplits {

    private static final ColumnVisibility EMPTY_VIS = new ColumnVisibility();
    private static final Text EMPTY_TEXT = new Text();
    private static final Value EMPTY_VALUE = new Value(EMPTY_TEXT.getBytes());

    private static void printUsageAndExit() {
        System.out.println(
                        "Usage: datawave.ingest.util.GenerateShardSplits <startDate (yyyyMMDD)> <daysToGenerate> <numShardsPerDay> <numShardsPerSplit> [-splitsPerBatch <number of splits to create per batch>] [-balancerDelay <milliseconds to wait between batches for balance>] [-maxBalancerDelay <timeout for waiting for balance>] [-pctBatchBalanceRequired <percentage of tablets that must be balanced off before continuation] [-markersOnly] [-addShardMarkers] [-addDataTypeMarkers <comma delim data types>] [<username> <password> <tableName> [<instanceName> <zookeepers>]]");
        System.exit(-1);
    }

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {

        if (args.length < 3) {
            printUsageAndExit();
        }
        // parse out the args
        Date startDate = null;
        int DAYS_TO_GENERATE = -1;
        int SHARDS = -1;
        int splitStep = 1;
        int balancerDelay = 5000; // 5 seconds
        int maxBalancerDelay = 90000; // 90 seconds
        double pctBatchBalanceRequired = .5;
        boolean addSplits = true;
        boolean addShardMarkers = false;
        int splitsPerBatch = 100000;
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
            } else if ("-splitsPerBatch".equalsIgnoreCase(args[i])) {
                if (i + 2 > args.length) {
                    System.err.println("-splitsPerBatch must be followed a number of splits to create per batch");
                    System.exit(-2);
                }
                try {
                    splitsPerBatch = Integer.parseInt(args[i]);
                    if (splitsPerBatch < 0) {
                        System.out.println("splitsPerBatch cannot be less than 0");
                        System.exit(-2);
                    }
                } catch (NumberFormatException e) {
                    System.out.println("splitsPerBatch delay argument is not an integer:" + e.getMessage());
                    System.exit(-2);
                }
            } else if ("-balancerDelay".equalsIgnoreCase(args[i])) {
                if (i + 2 > args.length) {
                    System.err.println("-balancerDelay must be followed a number of millisecond to wait for balance between batches");
                    System.exit(-2);
                }
                try {
                    balancerDelay = Integer.parseInt(args[++i]);
                } catch (NumberFormatException e) {
                    System.err.println("-balancerDelay must be followed a number of milliseconds to wait for balance between batches");
                    System.exit(-2);
                }
            } else if ("-maxBalancerDelay".equalsIgnoreCase(args[i])) {
                if (i + 2 > args.length) {
                    System.err.println("-maxBalancerDelay must be followed a maximum number of milliseconds to wait for balance");
                    System.exit(-2);
                }
                try {
                    maxBalancerDelay = Integer.parseInt(args[++i]);
                } catch (NumberFormatException e) {
                    System.err.println("-maxBalancerDelay must be followed a maximum number of milliseconds to wait for balance");
                    System.exit(-2);
                }
            } else if ("-pctBatchBalanceRequired".equalsIgnoreCase(args[i])) {
                if (i + 2 > args.length) {
                    System.err.println(
                                    "-pctBatchBalanceRequired must be followed a double of the percentage of batch size that must be on a different server to continue");
                    System.exit(-2);
                }
                try {
                    pctBatchBalanceRequired = Integer.parseInt(args[++i]);
                } catch (NumberFormatException e) {
                    System.err.println(
                                    "-pctBatchBalanceRequired must be followed a double of the percentage of batch size that must be on a different server to continue");
                    System.exit(-2);
                }
            } else if (args[i].equals("-markersOnly")) {
                addSplits = false;
            } else if (args[i].equals("-addShardMarkers")) {
                addShardMarkers = true;
            } else if (args[i].equals("-addDataTypeMarkers")) {
                shardMarkerTypes = args[i + 1].split(",");
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

        List<Text> splits = new ArrayList<>();
        List<Mutation> mutations = new ArrayList<>();
        for (int x = 0; x < DAYS_TO_GENERATE; x++) {

            // Generate configured shards per day
            for (int y = 0; y < SHARDS; y += splitStep) {
                Text split = new Text(DateHelper.format(startDate) + "_" + y);
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
        List<Text> results = new ArrayList(splits.size());
        calculateMidpoints(splits, results);

        splits = results;

        if (username != null) {
            // Connect to accumulo
            try (AccumuloClient client = Accumulo.newClient().to(instanceName, zookeepers).as(username, new PasswordToken(password)).build()) {
                // add the splits
                if (addSplits) {
                    int batchSize = splitsPerBatch; // Make splits in batches,

                    int callCount = 0;
                    int reducedBatchSize = 0;

                    // as the addSplits command takes a sortedset, but we intentionally do not want the order to be
                    // lexicographically sorted.
                    while (!splits.isEmpty()) {
                        if (callCount <= 10) {
                            callCount++;
                            reducedBatchSize = ((batchSize / 10) * callCount);
                        }

                        // Determine the end index for the batch
                        reducedBatchSize = Math.min(reducedBatchSize, splits.size());

                        // Extract a batch of splits from the front of the list
                        SortedSet<Text> batch = new TreeSet<>(splits.subList(0, reducedBatchSize));
                        List<Range> rangesToWaitFor = new ArrayList<>();
                        for (Text t : batch) {
                            rangesToWaitFor.add(new Range(t));
                        }

                        long startAddSplits = System.currentTimeMillis();

                        // Perform the operation on the current batch
                        client.tableOperations().addSplits(tableName, batch);

                        // Remove the processed batch from the list
                        splits.subList(0, reducedBatchSize).clear();

                        Set<String> tabletLocations = getTabletLocations(client, tableName, rangesToWaitFor);

                        long currentBatchDelay = System.currentTimeMillis() - startAddSplits;

                        // Ensure at least half the tablets have balanced off the original tserver
                        while ((tabletLocations.size() < reducedBatchSize * pctBatchBalanceRequired) || (currentBatchDelay < maxBalancerDelay)) {
                            tabletLocations = getTabletLocations(client, tableName, rangesToWaitFor);
                            Thread.sleep(balancerDelay);
                            currentBatchDelay = currentBatchDelay + balancerDelay;
                        }
                    }
                }

                // add the markers
                if (!mutations.isEmpty()) {
                    try (BatchWriter w = client.createBatchWriter(tableName,
                                    new BatchWriterConfig().setMaxLatency(1, TimeUnit.SECONDS).setMaxMemory(100000L).setMaxWriteThreads(4))) {
                        w.addMutations(mutations);
                    }
                }
            } // disconnect from accumulo
        } else {
            if (addSplits) {
                for (Text t : splits) {
                    System.out.println(t.toString());
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

    private static Set<String> getTabletLocations(AccumuloClient client, String tableName, List<Range> rangesToWaitFor)
                    throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
        Locations locations = client.tableOperations().locate(tableName, rangesToWaitFor);
        Set<String> tabletLocations = new HashSet<>();
        for (Range range : rangesToWaitFor) {
            TabletId id = locations.groupByRange().get(range).get(0);
            tabletLocations.add(locations.getTabletLocation(id));
            System.out.println(range.getStartKey() + "," + range.getEndKey() + "," + locations.getTabletLocation(id));
        }
        return tabletLocations;
    }

    protected static void calculateMidpoints(List<Text> splits, List<Text> midpoints) {
        if (splits.size() > 2) {
            int n = splits.size();

            if (n % 2 == 0) {
                // Even case: Add the two middle elements
                midpoints.add(splits.get(n / 2 - 1));
                midpoints.add(splits.get(n / 2));

                calculateMidpoints(splits.subList(0, (n / 2) - 1), midpoints);
                calculateMidpoints(splits.subList(((n / 2) + 1), n), midpoints);
            } else {
                // odd case: Add the single middle element
                midpoints.add(splits.get(n / 2));

                calculateMidpoints(splits.subList(0, n / 2), midpoints);
                calculateMidpoints(splits.subList((n / 2) + 1, n), midpoints);
            }
        } else {
            midpoints.addAll(splits);
        }
    }
}
