package datawave.ingest.util;

import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Collections;
import java.util.Comparator;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang.time.DateUtils;
import org.apache.hadoop.io.Text;

import datawave.util.StringUtils;
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
                "Usage: datawave.ingest.util.GenerateShardSplits <startDate (yyyyMMDD)> <daysToGenerate> <numShardsPerDay> <numShardsPerSplit> <numberOfSplitsPerBatch> [-markersOnly] [-addShardMarkers] [-addDataTypeMarkers <comma delim data types>] [<username> <password> <tableName> [<instanceName> <zookeepers>]]");
        System.exit(-1);
    }

    protected static List<Text> sortSplitsByMidpoints(List<Text> unsorted)
    {
        // Sort files by date and number
        List<Text> sortedFiles = new ArrayList<>(unsorted);
        sortedFiles.sort(new Comparator<>() {
            @Override
            public int compare(Text a, Text b) {
                String[] partsA = a.toString().split("_");
                String[] partsB = b.toString().split("_");

                int dateComparison = partsA[0].compareTo(partsB[0]);
                if (dateComparison != 0) {
                    return dateComparison;
                }

                int numberA = Integer.parseInt(partsA[1]);
                int numberB = Integer.parseInt(partsB[1]);
                return Integer.compare(numberA, numberB);
            }
        });

        // Call recursive function to calculate midpoints
        return calculateMidpoints(sortedFiles);
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
        int splitsPerBatch = 100;
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
            } else if (i == 4) {
                try {
                    splitsPerBatch = Integer.parseInt(args[i]);
                } catch (NumberFormatException e) {
                    System.out.println("Splits Per Batch argument is not an integer:" + e.getMessage());
                    System.exit(-2);
                }
            }else if (args[i].equals("-markersOnly")) {
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

        List<Text> splits = new ArrayList<>();
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

        splits = sortSplitsByMidpoints(splits);

        if (username != null) {
            // Connect to accumulo
            try (AccumuloClient client = Accumulo.newClient().to(instanceName, zookeepers).as(username, new PasswordToken(password)).build()) {
                // add the splits
                if (addSplits) {
                    int batchSize = splitsPerBatch; // Make splits in batches,
                    // as the addSplits command takes a sortedset, but we intentionally do not want the order to be
                    // lexicographically sorted.
                    while (!splits.isEmpty()) {
                        // Determine the end index for the batch
                        int endIndex = Math.min(batchSize, splits.size());

                        // Extract a batch of splits from the front of the list
                        SortedSet<Text> batch = new TreeSet<>(splits.subList(0, endIndex));

                        // Remove the processed batch from the list
                        splits.subList(0, endIndex).clear();

                        // Perform the operation on the current batch
                        client.tableOperations().addSplits(tableName, batch);
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


    private static List<Text> calculateMidpoints(List<Text> splits) {
        if (splits.isEmpty()) {
            return Collections.emptyList();
        }

        List<Text> midpoints = new ArrayList<>();
        int n = splits.size();


        if (n % 2 == 0) {
            // Even case: Add the two middle elements
            midpoints.add(splits.get(n / 2 - 1));
            midpoints.add(splits.get(n / 2));

            midpoints.addAll(calculateMidpoints(splits.subList(0, (n / 2) - 1)));
            midpoints.addAll(calculateMidpoints(splits.subList(((n  / 2) + 1), n)));
        } else {
            // odd case: Add the single middle element
            midpoints.add(splits.get(n / 2));

            midpoints.addAll(calculateMidpoints(splits.subList(0, n / 2)));
            midpoints.addAll(calculateMidpoints(splits.subList((n + 1) / 2, n)));
        }

        return midpoints;
    }

}
