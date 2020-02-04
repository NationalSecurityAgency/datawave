package datawave.ingest.mapreduce.job;

import datawave.ingest.data.config.ConfigurationHelper;
import datawave.ingest.mapreduce.handler.shard.ShardedDataTypeHandler;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

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
         * @throws TableExistsException
         * @throws TableNotFoundException
         * @throws URISyntaxException
         */
        public Writer(Configuration conf, int reduceTasks, Path workDirPath, FileSystem outputFs, String[] tableNames) throws TableNotFoundException,
                        IOException, TableExistsException, URISyntaxException {
            this(conf, reduceTasks, workDirPath, outputFs, tableNames, SplitsFileType.TRIMMEDBYNUMBER);
        }
        
        public Writer(Configuration conf, int reduceTasks, Path workDirPath, FileSystem fs, String[] tableNames, SplitsFileType splitsFileType)
                        throws TableNotFoundException, IOException, TableExistsException, URISyntaxException {
            this.conf = conf;
            this.reduceTasks = reduceTasks;
            this.workDirPath = workDirPath;
            this.fs = fs;
            this.tableNames = tableNames;
            this.shardedTableNames = Arrays.asList(ConfigurationHelper.isNull(conf, ShardedDataTypeHandler.SHARDED_TNAMES, String[].class));
        }
        
        public void createFile(SplitsFileType splitsFileType) {
            try {
                MetadataTableSplits splits = new MetadataTableSplits(conf);
                boolean isCacheValid = MetadataTableSplitsCacheStatus.isCacheValid(conf);
                boolean shouldRefreshSplits = MetadataTableSplits.shouldRefreshSplits(conf);
                if (shouldRefreshSplits && !isCacheValid) {
                    log.info("Recreating splits");
                    splits.update();
                } else if (!shouldRefreshSplits && !isCacheValid) {
                    throw new Exception("Splits cache is invalid");
                }
                writeSplitsToFile(splits, splitsFileType);
                uri = new URI(workDirPath + "/" + createFileName(splitsFileType));
            } catch (Exception e) {
                throw new RuntimeException("Could not create splits file for the job. See documentation for using generateSplitsFile.sh", e);
            }
        }
        
        private void writeSplitsToFile(MetadataTableSplits splits, SplitsFileType splitsFileType) throws IOException {
            PrintStream out = new PrintStream(new BufferedOutputStream(fs.create(new Path(workDirPath, createFileName(splitsFileType)))));
            outputSplitsForNonShardTables(splits, out, splitsFileType);
            out.close();
        }
        
        private void outputSplitsForNonShardTables(MetadataTableSplits splits, PrintStream out, SplitsFileType splitsFileType) throws IOException {
            for (String table : tableNames) {
                if (null != shardedTableNames && shardedTableNames.contains(table)) {
                    continue;
                }
                outputSplitsForTable(splits, out, table, splitsFileType);
            }
        }
        
        private void outputSplitsForTable(MetadataTableSplits splits, PrintStream out, String table, SplitsFileType splitsFileType) throws IOException {
            Collection<Text> tableSplits = null;
            Map<Text,String> tableSplitsAndLocations;
            if (splitsFileType.equals(SplitsFileType.TRIMMEDBYNUMBER)) {
                tableSplits = splits.getSplits(table, reduceTasks - 1);
            } else if (splitsFileType.equals(SplitsFileType.UNTRIMMED)) {
                tableSplits = splits.getSplits(table);
            } else if (splitsFileType.equals(SplitsFileType.SPLITSANDLOCATIONS)) {
                tableSplitsAndLocations = splits.getSplitsAndLocationByTable(table);
                for (Text splitAndLocation : tableSplitsAndLocations.keySet()) {
                    out.println(table + "\t" + new String(Base64.encodeBase64(splitAndLocation.getBytes())) + "\t"
                                    + tableSplitsAndLocations.get(splitAndLocation));
                    if (log.isTraceEnabled()) {
                        log.trace(table + " split: " + splitAndLocation);
                    }
                }
            }
            if (tableSplits != null) {
                for (Text split : tableSplits) {
                    out.println(table + "\t" + new String(Base64.encodeBase64(split.getBytes())));
                    if (log.isTraceEnabled()) {
                        log.trace(table + " split: " + split);
                    }
                }
            }
        }
        
        public URI getUri() {
            return uri;
        }
    }
    
    private static String createFileName(SplitsFileType splitsFileType) {
        return (splitsFileType) + SPLITS_FILE_NAME_PROPERTY_VALUE;
    }
    
    public static Path findSplitsFile(Configuration conf, Path[] filesToCheck, SplitsFileType splitsFileType) {
        String fileName = createFileName(splitsFileType);
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
        private Map<String,Map<Text,String>> splitsAndLocations;
        
        public Reader(Configuration conf, Path[] filesToCheck, SplitsFileType splitsFileType) throws IOException {
            Path cacheFile = findSplitsFile(conf, filesToCheck, splitsFileType);
            if (null == cacheFile) {
                throw new RuntimeException("Could not find cut point file");
            }
            
            splits = new HashMap<>();
            splitsAndLocations = new HashMap<>();
            ArrayList<Text> cutPoints = new ArrayList<>();
            SortedMap<Text,String> cutPointsAndLocations = new TreeMap<>();
            String previousTableName = null;
            boolean isFirstLine = true;
            boolean isSameTable;
            boolean hasSplits = false;
            boolean hasLocations = false;
            
            try (BufferedReader in = new BufferedReader(new FileReader(cacheFile.toString()))) {
                String line;
                while ((line = in.readLine()) != null) {
                    String[] parts = line.split("\\t");
                    isSameTable = parts[0].equals(previousTableName);
                    hasSplits = parts.length > 1;
                    hasLocations = parts.length > 2;
                    
                    if (isFirstLine) {
                        previousTableName = parts[0];
                        isFirstLine = false;
                    }
                    if (!isSameTable) {
                        saveLastSplitForTable(cutPoints, cutPointsAndLocations, splits, splitsAndLocations, previousTableName, hasSplits, hasLocations);
                        previousTableName = parts[0];
                        cutPoints.clear();
                        cutPointsAndLocations.clear();
                    }
                    if (hasLocations) {
                        cutPointsAndLocations.put(new Text(Base64.decodeBase64(parts[1].getBytes())), parts[2]);
                    } else if (hasSplits) {
                        cutPoints.add(new Text(Base64.decodeBase64(parts[1].getBytes())));
                    }
                }
            } finally {
                // Add the last batch
                saveLastSplitForTable(cutPoints, cutPointsAndLocations, splits, splitsAndLocations, previousTableName, hasSplits, hasLocations);
            }
        }
        
        public Map<String,Text[]> getSplitsByTable() {
            return splits;
        }
        
        public Map<String,Map<Text,String>> getSplitsAndLocationsByTable() {
            return splitsAndLocations;
        }
        
        private void saveLastSplitForTable(ArrayList<Text> cutPoints, SortedMap<Text,String> cutPointsAndLocations, Map<String,Text[]> splits,
                        Map<String,Map<Text,String>> splitsAndLocations, String previousTableName, boolean hasSplits, boolean hasLocations) {
            Collections.sort(cutPoints);
            if (hasLocations) {
                splitsAndLocations.put(previousTableName, cutPointsAndLocations);
                log.info("Adding cut points and locations for the table: " + previousTableName);
                splits.put(previousTableName, cutPointsAndLocations.keySet().toArray(new Text[cutPointsAndLocations.size()]));
            } else if (hasSplits) {
                splits.put(previousTableName, cutPoints.toArray(new Text[cutPoints.size()]));
                log.info("Adding cut points for table: " + previousTableName);
            }
        }
    }
}
