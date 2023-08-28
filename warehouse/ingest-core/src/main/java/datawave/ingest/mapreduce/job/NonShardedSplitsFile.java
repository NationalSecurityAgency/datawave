package datawave.ingest.mapreduce.job;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import datawave.ingest.data.config.ConfigurationHelper;
import datawave.ingest.mapreduce.handler.shard.ShardedDataTypeHandler;

/**
 * Extracted from IngestJob Creates a splits file with all the requested tables, omitting the sharded ones Adds it to the specified work dir and sets the confi
 */
public class NonShardedSplitsFile {
    protected static final Logger log = Logger.getLogger(NonShardedSplitsFile.class);
    public static final String SPLITS_FILE_NAME_PROPERTY_KEY = "datawave.ingest.bulk.NonShardedSplitsFile.cutFile";
    private static final String SPLITS_FILE_NAME_PROPERTY_VALUE = "splits.txt";

    public static class Writer {
        private URI uri;
        private final int reduceTasks;
        private final Path workDirPath;
        private final String[] tableNames;
        private final List<String> shardedTableNames;
        private final boolean isTrimmed;
        private final Configuration conf;
        private final FileSystem fs;

        /**
         * Writes out the split points for the tables to splits.txt in the work directory.
         *
         * @param conf
         *            hadoop job configuration
         * @param reduceTasks
         *            number of reduce tasks
         * @param workDirPath
         *            base dir in HDFS where the file is written
         * @param outputFs
         *            the filesystem to which the splits file should be written
         * @param tableNames
         *            names of the table to retrieve splits for
         * @throws IOException
         *             if there is an issue with read or write
         * @throws TableExistsException
         *             if the table already exists
         * @throws TableNotFoundException
         *             if the table could not be found
         * @throws URISyntaxException
         *             if there is an issue with the URI syntax
         */
        public Writer(Configuration conf, int reduceTasks, Path workDirPath, FileSystem outputFs, String[] tableNames)
                        throws TableNotFoundException, IOException, TableExistsException, URISyntaxException {
            this(conf, reduceTasks, workDirPath, outputFs, tableNames, true);
        }

        public Writer(Configuration conf, int reduceTasks, Path workDirPath, FileSystem fs, String[] tableNames, boolean isTrimmed)
                        throws TableNotFoundException, IOException, TableExistsException, URISyntaxException {
            this.conf = conf;
            this.reduceTasks = reduceTasks;
            this.workDirPath = workDirPath;
            this.fs = fs;
            this.tableNames = tableNames;
            this.isTrimmed = isTrimmed;
            this.shardedTableNames = Arrays.asList(ConfigurationHelper.isNull(conf, ShardedDataTypeHandler.SHARDED_TNAMES, String[].class));
        }

        public void createFile(boolean isTrimmed) {
            try {
                TableSplitsCache splits = new TableSplitsCache(conf);
                boolean isCacheValid = TableSplitsCacheStatus.isCacheValid(conf);
                boolean shouldRefreshSplits = TableSplitsCache.shouldRefreshSplits(conf);
                if (shouldRefreshSplits && !isCacheValid) {
                    log.info("Recreating splits");
                    splits.update();
                } else if (!shouldRefreshSplits && !isCacheValid) {
                    throw new Exception("Splits cache is invalid");
                }
                writeSplitsToFile(splits);
                uri = new URI(workDirPath + "/" + createFileName(isTrimmed));
            } catch (Exception e) {
                throw new RuntimeException("Could not create splits file for the job. See documentation for using generateSplitsFile.sh", e);
            }
        }

        private void writeSplitsToFile(TableSplitsCache splits) throws IOException {
            PrintStream out = new PrintStream(new BufferedOutputStream(fs.create(new Path(workDirPath, createFileName(isTrimmed)))));
            outputSplitsForNonShardTables(splits, out);
            out.close();
        }

        private void outputSplitsForNonShardTables(TableSplitsCache splits, PrintStream out) throws IOException {
            for (String table : tableNames) {
                if (null != shardedTableNames && shardedTableNames.contains(table)) {
                    continue;
                }
                outputSplitsForTable(splits, out, table);
            }
        }

        private void outputSplitsForTable(TableSplitsCache splits, PrintStream out, String table) throws IOException {
            Collection<Text> tableSplits;
            if (isTrimmed) {
                tableSplits = splits.getSplits(table, reduceTasks - 1);
            } else {
                tableSplits = splits.getSplits(table);
            }
            for (Text split : tableSplits) {
                out.println(table + "\t" + new String(Base64.encodeBase64(split.getBytes())));
                if (log.isTraceEnabled()) {
                    log.trace(table + " split: " + split);
                }
            }
        }

        public URI getUri() {
            return uri;
        }
    }

    private static String createFileName(boolean isTrimmed) {
        return (isTrimmed ? "trimmed_" : "full_") + SPLITS_FILE_NAME_PROPERTY_VALUE;
    }

    public static Path findSplitsFile(Configuration conf, Path[] filesToCheck, boolean isTrimmed) {
        String fileName = createFileName(isTrimmed);
        if (filesToCheck != null) {
            for (Path cacheFile : filesToCheck) {
                if (matchesFileName(fileName, cacheFile)) {
                    return cacheFile;
                }
            }
        }
        return null;
    }

    private static boolean matchesFileName(String cutFileName, Path cacheFile) {
        return cacheFile.getName().endsWith(cutFileName);
    }

    public static class Reader {
        private Map<String,Text[]> splits;

        public Reader(Configuration conf, Path[] filesToCheck, boolean isTrimmed) throws IOException {
            Path cacheFile = findSplitsFile(conf, filesToCheck, isTrimmed);
            if (null == cacheFile) {
                throw new RuntimeException("Could not find cut point file");
            }

            splits = new HashMap<>();
            ArrayList<Text> cutPoints = new ArrayList<>();
            String previousTableName = null;
            try (BufferedReader in = new BufferedReader(new FileReader(cacheFile.toString()))) {
                String line;
                while ((line = in.readLine()) != null) {
                    String[] parts = line.split("\\t");
                    if (parts[0].equals(previousTableName)) {
                        if (parts.length > 1)
                            cutPoints.add(new Text(Base64.decodeBase64(parts[1].getBytes())));
                    } else if (previousTableName == null) {
                        previousTableName = parts[0];
                        if (parts.length > 1)
                            cutPoints.add(new Text(Base64.decodeBase64(parts[1].getBytes())));
                    } else {
                        Collections.sort(cutPoints);
                        splits.put(previousTableName, cutPoints.toArray(new Text[cutPoints.size()]));
                        log.info("Adding cut points for table: " + previousTableName);
                        previousTableName = parts[0];
                        cutPoints.clear();
                        if (parts.length > 1)
                            cutPoints.add(new Text(Base64.decodeBase64(parts[1].getBytes())));
                    }
                }
            } finally {
                if (null != previousTableName) {
                    // Add the last batch.
                    Collections.sort(cutPoints);
                    splits.put(previousTableName, cutPoints.toArray(new Text[cutPoints.size()]));
                    log.info("Adding cut points for table: " + previousTableName);
                }
            }
        }

        public Map<String,Text[]> getSplitsByTable() {
            return splits;
        }
    }
}
