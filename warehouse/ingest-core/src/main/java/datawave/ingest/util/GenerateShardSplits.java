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
        Options opts;
        try {
            opts = parseOptions(args);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            System.exit(-1);
            return;
        }

        if (opts.printUsageAndExit) {
            printUsageAndExit();
        }

        List<Text> splits = new ArrayList<>();
        List<Mutation> mutations = new ArrayList<>();
        for (int x = 0; x < opts.daysToGenerate; x++) {

            // Generate configured shards per day
            for (int y = 0; y < opts.numShardsPerDay; y += opts.numShardsPerSplit) {
                Text split = new Text(DateHelper.format(opts.startDate) + "_" + y);
                splits.add(split);

                // add markers as required
                if (opts.addShardMarkers || opts.shardMarkerTypes != null) {
                    Date nextYear = DateUtils.addYears(opts.startDate, 1);
                    Mutation m = new Mutation(split);
                    if (opts.addShardMarkers) {
                        m.put(EMPTY_TEXT, EMPTY_TEXT, EMPTY_VIS, nextYear.getTime(), EMPTY_VALUE);
                    }
                    if (opts.shardMarkerTypes != null) {
                        for (String type : opts.shardMarkerTypes) {
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

            opts.startDate = DateUtils.addDays(opts.startDate, 1);
        }
        List<Text> results = new ArrayList(splits.size());
        calculateMidpoints(splits, results);

        splits = results;

        if (opts.username != null) {
            // Connect to accumulo
            try (AccumuloClient client = Accumulo.newClient().to(opts.instanceName, opts.zookeepers).as(opts.username, new PasswordToken(opts.password))
                            .build()) {
                // add the splits
                if (opts.addSplits) {
                    int batchSize = opts.splitsPerBatch; // Make splits in batches,

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
                        client.tableOperations().addSplits(opts.tableName, batch);

                        // Remove the processed batch from the list
                        splits.subList(0, reducedBatchSize).clear();

                        Set<String> tabletLocations = getTabletLocations(client, opts.tableName, rangesToWaitFor);

                        long currentBatchDelay = System.currentTimeMillis() - startAddSplits;

                        // Ensure at least half the tablets have balanced off the original tserver
                        while ((tabletLocations.size() < reducedBatchSize * opts.pctBatchBalanceRequired) && (currentBatchDelay < opts.maxBalancerDelay)) {
                            tabletLocations = getTabletLocations(client, opts.tableName, rangesToWaitFor);
                            Thread.sleep(opts.balancerDelay);
                            currentBatchDelay = currentBatchDelay + opts.balancerDelay;
                        }
                    }
                }

                // add the markers
                if (!mutations.isEmpty()) {
                    try (BatchWriter w = client.createBatchWriter(opts.tableName,
                                    new BatchWriterConfig().setMaxLatency(1, TimeUnit.SECONDS).setMaxMemory(100000L).setMaxWriteThreads(4))) {
                        w.addMutations(mutations);
                    }
                }
            } // disconnect from accumulo
        } else {
            if (opts.addSplits) {
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

    static Options parseOptions(String[] args) {
        Options opts = new Options();
        if (args.length < 3) {
            opts.printUsageAndExit = true;
            return opts;
        }
        for (int i = 0; i < args.length; i++) {
            if (i == 0) {
                try {
                    opts.startDate = DateHelper.parse(args[i]);
                } catch (DateTimeParseException e) {
                    throw new IllegalArgumentException("Start Date does not match format. Exception=" + e.getMessage());
                }
            } else if (i == 1) {
                try {
                    opts.daysToGenerate = Integer.parseInt(args[i]);
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Days to Generate argument is not an integer: " + e.getMessage());
                }
            } else if (i == 2) {
                try {
                    opts.numShardsPerDay = Integer.parseInt(args[i]);
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Num Shards Per Day argument is not an integer: " + e.getMessage());
                }
            } else if (i == 3) {
                try {
                    opts.numShardsPerSplit = Integer.parseInt(args[i]);
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Num Shards Per Split argument is not an integer: " + e.getMessage());
                }
            } else if ("-splitsPerBatch".equalsIgnoreCase(args[i])) {
                if (i + 2 > args.length) {
                    throw new IllegalArgumentException("-splitsPerBatch must be followed by a number of splits to create per batch");
                }
                try {
                    opts.splitsPerBatch = Integer.parseInt(args[++i]);
                    if (opts.splitsPerBatch < 0) {
                        throw new IllegalArgumentException("-splitsPerBatch cannot be less than 0");
                    }
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("-splitsPerBatch argument is not an integer: " + e.getMessage());
                }
            } else if ("-balancerDelay".equalsIgnoreCase(args[i])) {
                if (i + 2 > args.length) {
                    throw new IllegalArgumentException("-balancerDelay must be followed by a number of milliseconds to wait for balance between batches");
                }
                try {
                    opts.balancerDelay = Integer.parseInt(args[++i]);
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("-balancerDelay argument is not an integer: " + e.getMessage());
                }
            } else if ("-maxBalancerDelay".equalsIgnoreCase(args[i])) {
                if (i + 2 > args.length) {
                    throw new IllegalArgumentException("-maxBalancerDelay must be followed a maximum number of milliseconds to wait for balance");
                }
                try {
                    opts.maxBalancerDelay = Integer.parseInt(args[++i]);
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("-maxBalancerDelay argument is not an integer: " + e.getMessage());
                }
            } else if ("-pctBatchBalanceRequired".equalsIgnoreCase(args[i])) {
                if (i + 2 > args.length) {
                    throw new IllegalArgumentException(
                                    "-pctBatchBalanceRequired must be followed a double of the percentage of batch size that must be on a different server to continue");
                }
                try {
                    opts.pctBatchBalanceRequired = Double.parseDouble(args[++i]);
                    if (opts.pctBatchBalanceRequired < 0 || opts.pctBatchBalanceRequired > 1) {
                        throw new IllegalArgumentException("-pctBatchBalanceRequired must be between 0 and 1");
                    }
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("-pctBatchBalanceRequired argument is not a double: " + e.getMessage());
                }
            } else if (args[i].equals("-markersOnly")) {
                opts.addSplits = false;
            } else if (args[i].equals("-addShardMarkers")) {
                opts.addShardMarkers = true;
            } else if (args[i].equals("-addDataTypeMarkers")) {
                if (i + 2 > args.length) {
                    throw new IllegalArgumentException("-addDataTypeMarkers must be followed a comma-delimited list of data types");
                }
                opts.shardMarkerTypes = args[i + 1].split(",");
                // skip over cmd, for loop will skip over arg
                i++;
            } else {
                // need at least 3 more args
                if (i + 3 > args.length) {
                    opts.printUsageAndExit = true;
                    return opts;
                } else {
                    opts.username = args[i];
                    opts.password = PasswordConverter.parseArg(args[i + 1]).getBytes();
                    opts.tableName = args[i + 2];
                    // skip over args
                    i += 3;
                }
                // if we still have args
                if (i < args.length) {
                    // then we need exactly 2 more args
                    if (i + 2 != args.length) {
                        opts.printUsageAndExit = true;
                        return opts;
                    } else {
                        opts.instanceName = args[i];
                        opts.zookeepers = args[i + 1];
                        // skip over args to terminate loop
                        i += 2;
                    }
                }
            }
        }

        return opts;
    }

    static class Options {
        boolean printUsageAndExit = false;
        Date startDate = null;
        int daysToGenerate = -1;
        int numShardsPerDay = -1;
        int numShardsPerSplit = 1;
        int balancerDelay = 5_000; // 5 seconds
        int maxBalancerDelay = 90_000; // 90 seconds
        double pctBatchBalanceRequired = .5;
        boolean addSplits = true;
        boolean addShardMarkers = false;
        int splitsPerBatch = 100_000;
        String[] shardMarkerTypes = null;
        String username = null;
        byte[] password = null;
        String tableName = null;
        String instanceName = null;
        String zookeepers = null;
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
