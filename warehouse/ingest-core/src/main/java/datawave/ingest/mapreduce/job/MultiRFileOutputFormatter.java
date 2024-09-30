package datawave.ingest.mapreduce.job;

import static datawave.ingest.mapreduce.job.BulkIngestMapFileLoader.BULK_IMPORT_MODE_CONFIG;
import static org.apache.accumulo.core.conf.Property.TABLE_CRYPTO_PREFIX;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.rfile.RFileWriter;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.crypto.CryptoFactoryLoader;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.LoadPlan;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.file.rfile.RFile;
import org.apache.accumulo.core.spi.crypto.CryptoEnvironment;
import org.apache.accumulo.core.spi.crypto.CryptoService;
import org.apache.accumulo.core.spi.file.rfile.compression.NoCompression;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import datawave.ingest.data.config.ingest.AccumuloHelper;
import datawave.ingest.mapreduce.handler.shard.ShardedDataTypeHandler;
import datawave.ingest.mapreduce.job.BulkIngestMapFileLoader.ImportMode;
import datawave.marking.MarkingFunctions;
import datawave.util.StringUtils;

public class MultiRFileOutputFormatter extends FileOutputFormat<BulkIngestKey,Value> {

    private static final Logger log = Logger.getLogger(MultiRFileOutputFormatter.class);

    protected Map<String,SizeTrackingWriter> writers = null;
    protected boolean loadPlanningEnabled = false;
    protected Map<String,List<LoadPlan>> loadPlans = null;
    protected List<CompletableFuture<Void>> loadPlanFutures = null;
    protected Map<String,Path> unusedWriterPaths = null;
    protected Map<String,Path> usedWriterPaths = null;
    protected Map<String,String> writerTableNames = null;
    protected Map<String,MutableInt> writerCounts = null;

    protected static final String PREFIX = MultiRFileOutputFormatter.class.getName();

    protected static final String USERNAME = PREFIX + ".username";
    protected static final String PASSWORD = PREFIX + ".password";
    protected static final String INSTANCE_NAME = PREFIX + ".instance.name";
    protected static final String ZOOKEEPERS = PREFIX + ".zookeepers";
    protected static final String FILE_TYPE = MultiRFileOutputFormatter.class.getSimpleName() + ".file_type";
    protected static final String COMPRESSION_TYPE = PREFIX + ".compression";
    protected static final String COMPRESSION_DISALLOWLIST = PREFIX + ".compression.table.disallowlist";
    protected static final String MAX_RFILE_UNCOMPRESSED_SIZE = PREFIX + ".maxRFileUncompressedSize";
    protected static final String MAX_RFILE_UNDEDUPPED_ENTRIES = PREFIX + ".maxRFileUndeduppedEntries";
    protected static final String GENERATE_MAP_FILE_ROW_KEYS = PREFIX + ".generateMapFileRowKeys";
    protected static final String GENERATE_MAP_FILE_PER_SHARD_LOCATION = PREFIX + ".generateMapFilePerShardLocation";

    protected static final String BASE = "bulk.output.partition.count.";
    public static final String CONFIGURE_LOCALITY_GROUPS = PREFIX + ".tables";
    public static final String EVENT_PARTITION_COUNT = BASE + "Event";
    public static final String EDGE_PARTITION_COUNT = BASE + "Edge";
    public static final String INDEX_PARTITION_COUNT = BASE + "Index";
    public static final String REV_INDEX_PARTITION_COUNT = BASE + "ReverseIndex";
    public static final String LOCATION_PARTITION_COUNT = BASE + "Location";

    protected FileSystem fs = null;
    protected Map<String,Map<Text,String>> tableShardLocations;
    protected Map<String,Set<Text>> shardMapFileRowKeys = new HashMap<>();
    protected Map<String,Path> shardMapFiles = new HashMap<>();
    protected Set<String> shardedTableNames = null;
    protected Set<String> shardedTablesConfigured = null;
    protected String eventTable = null;
    protected Path workDir;
    protected String extension;
    protected Configuration conf;
    protected Map<String,ConfigurationCopy> tableConfigs;
    protected Set<String> tableIds = null;
    protected long maxRFileSize = 0;
    protected int maxRFileEntries = 0;
    protected boolean generateMapFileRowKeys = false;
    protected boolean generateMapFilePerShardLocation = false;
    private long startWriteTime = 0L;

    protected Map<String,Map<Text,String>> columnFamilyToLocalityGroup;

    protected Map<String,Map<String,Set<ByteSequence>>> localityGroupToColumnFamilies;

    public static final String CONFIGURED_TABLE_NAMES = PREFIX + ".configTableNames";

    public static void setGenerateMapFileRowKeys(Configuration conf, boolean generateMapFileRowKeys) {
        conf.setBoolean(GENERATE_MAP_FILE_ROW_KEYS, generateMapFileRowKeys);
    }

    public static void setGenerateMapFilePerShardLocation(Configuration conf, boolean generateMapFilePerShardLocation) {
        conf.setBoolean(GENERATE_MAP_FILE_PER_SHARD_LOCATION, generateMapFilePerShardLocation);
    }

    public static void setCompressionType(Configuration conf, String compressionType) {
        if (compressionType != null) {
            if (!("snappy".equals(compressionType) || "lzo".equals(compressionType) || "gz".equals(compressionType) || "zstd".equals(compressionType)
                            || "none".equals(compressionType)))

                throw new IllegalArgumentException("compressionType must be one of snappy, lzo, gz, zstd, or none");
            conf.set(COMPRESSION_TYPE, compressionType);
        }
    }

    protected static String getCompressionType(Configuration conf) {
        return conf.get(COMPRESSION_TYPE, "gz");
    }

    public static void setCompressionTableDisallowList(Configuration conf, Set<String> compressionTableDisallowList) {
        if (compressionTableDisallowList != null) {
            StringBuilder tableList = new StringBuilder();
            for (String table : compressionTableDisallowList) {
                if (tableList.length() > 0) {
                    tableList.append(',');
                }
                tableList.append(table);
            }
            conf.set(COMPRESSION_DISALLOWLIST, tableList.toString());
        }
    }

    protected static Set<String> getCompressionTableDisallowList(Configuration conf) {
        String tableListString = conf.get(COMPRESSION_DISALLOWLIST);
        if (tableListString == null) {
            return Collections.EMPTY_SET;
        } else {
            String[] tables = StringUtils.split(tableListString, ',');
            return new HashSet<>(Arrays.asList(tables));
        }
    }

    public static void setFileType(Configuration conf, String type) {
        if (type != null)
            conf.set(FILE_TYPE, type);
    }

    public static void setAccumuloConfiguration(Configuration conf) {
        conf.set(INSTANCE_NAME, conf.get(AccumuloHelper.INSTANCE_NAME));
        conf.set(USERNAME, conf.get(AccumuloHelper.USERNAME));
        conf.set(PASSWORD, conf.get(AccumuloHelper.PASSWORD));
        conf.set(ZOOKEEPERS, conf.get(AccumuloHelper.ZOOKEEPERS));
    }

    public static void setRFileLimits(Configuration conf, int maxEntries, long maxSize) {
        conf.setInt(MAX_RFILE_UNDEDUPPED_ENTRIES, maxEntries);
        conf.setLong(MAX_RFILE_UNCOMPRESSED_SIZE, maxSize);
    }

    public static void addTableToLocalityGroupConfiguration(Configuration conf, String tableName) {
        String locs = conf.get(CONFIGURE_LOCALITY_GROUPS, "");
        Iterable<String> splits = Splitter.on(",").split(locs);
        conf.set(CONFIGURE_LOCALITY_GROUPS, Joiner.on(",").join(splits, tableName));
    }

    public static boolean loadPlanningEnabled(Configuration conf) {
        return conf.getEnum(BULK_IMPORT_MODE_CONFIG, ImportMode.V1).equals(ImportMode.V2_LOAD_PLANNING);
    }

    /**
     * Insert a count into the filename. The filename is expected to end with our extension.
     *
     * @param filename
     *            file name
     * @param count
     *            the count
     * @return filename with the count inserted as follows: {@code path/name + extension -> path/name + _count + extension}
     */
    protected Path insertFileCount(Path filename, int count) {
        String name = filename.getName();
        int index = name.length() - extension.length();
        name = name.substring(0, index) + '_' + count + name.substring(index);
        return new Path(filename.getParent(), name);
    }

    /**
     * Remove a count from a filename. The filename is expected to end with _count.extension.
     *
     * @param filename
     *            file name
     * @return filename with the count removed as follows: {@code path/name + _count + extension -> path/name + extension}
     */
    protected Path removeFileCount(Path filename) {
        String name = filename.getName();
        int index = name.length() - extension.length();
        int index2 = name.lastIndexOf('_', index);
        name = name.substring(0, index2) + name.substring(index);
        return new Path(filename.getParent(), name);
    }

    /**
     * create and register a writer for retrieval later
     *
     * @param key
     *            The key used for later retrieval
     * @param table
     *            The table name
     * @param filename
     *            The path of the file being written to
     * @param tableConf
     *            the table accumulo configuration
     * @throws IOException
     *             if there is an issue with read or write
     * @throws AccumuloException
     *             if there is an issue accumulo
     */
    protected void createAndRegisterWriter(String key, String table, Path filename, AccumuloConfiguration tableConf) throws IOException, AccumuloException {
        // first get the writer count (how many writers have we made for this key)
        MutableInt count = writerCounts.get(key);
        if (count == null) {
            count = new MutableInt(1);
            writerCounts.put(key, count);
        } else {
            count.increment();
        }

        // update the filename with the count
        filename = insertFileCount(filename, count.intValue());

        // now create and register the writer
        SizeTrackingWriter writer = openWriter(filename.toString(), tableConf, table);
        writer.startDefaultLocalityGroup();
        writers.put(key, writer);
        unusedWriterPaths.put(key, filename);
        writerTableNames.put(key, table);
        if (shardedTableNames.contains(table)) {
            shardMapFileRowKeys.put(key, new HashSet<>());
            shardMapFiles.put(key, filename);
        }
    }

    protected SizeTrackingWriter openWriter(String filename, AccumuloConfiguration tableConf, String table) throws IOException {
        startWriteTime = System.currentTimeMillis();
        // @formatter:off

        var builder = org.apache.accumulo.core.client.rfile.RFile.newWriter().to(filename).withFileSystem(fs).withTableProperties(tableConf);
        if(this.loadPlanningEnabled) {
            var splits = SplitsFile.getSplits(conf, table).get();
            if(splits != null && !splits.isEmpty()) {
                LoadPlan.SplitResolver splitResolver = row->findContainingSplits(row, splits);
                builder = builder.withSplitResolver(splitResolver);
            }
        }

        return new SizeTrackingWriter(builder.build());
        // @formatter:on
    }

    /**
     * Close the current writer for the specified key, and create the next writer. The index encoded in the filename will be appropriately updated.
     *
     * @param key
     *            a key
     * @throws IOException
     *             if there is an issue with read or write
     * @throws AccumuloException
     *             if there is an issue with accumulo
     */
    protected void closeAndUpdateWriter(String key) throws IOException, AccumuloException {
        SizeTrackingWriter writer = writers.get(key);
        // don't bother if we have not created a writer for this key yet
        if (writer != null) {
            String table = writerTableNames.get(key);
            Path file = usedWriterPaths.get(key);
            // don't bother if this writer has not been used yet
            if (file != null) {
                writer.close();
                if (loadPlanningEnabled) {
                    addLoadPlan(table, writer.getLoadPlan(file.getName()));
                }
                // pull the index off the filename
                file = removeFileCount(file);
                createAndRegisterWriter(key, table, file, tableConfigs.get(table));
            }
        }
    }

    private void addLoadPlan(String tableName, LoadPlan loadPlan) {
        loadPlans.computeIfAbsent(tableName, t -> new ArrayList<>()).add(loadPlan);
    }

    protected void commitLoadPlans(TaskAttemptContext context) {
        loadPlanFutures.stream().forEach(f -> {
            try {
                f.get();
            } catch (Exception e) {
                log.error("Load planning failed", e);
                throw new RuntimeException(e);
            }
        });
        writeLoadPlans(context);
    }

    private void writeLoadPlans(TaskAttemptContext context) {
        log.debug("Writing bulk load plans to disk for all tables");
        // Consolidate all plans for a table into a single file
        List<CompletableFuture<Void>> futures = new LinkedList<>();
        for (Map.Entry<String,List<LoadPlan>> entry : loadPlans.entrySet()) {
            var builder = LoadPlan.builder();
            entry.getValue().stream().forEach(plan -> builder.addPlan(plan));
            var table = entry.getKey();
            var path = new Path(String.format("%s/%s", workDir, table), getUniqueFile(context, "loadplan", ".json"));
            var loadPlan = builder.build();
            //@formatter:off
            futures.add(CompletableFuture.runAsync(() -> {
                try (FSDataOutputStream out = fs.create(path)) {
                    log.debug("Begin writing load plan for " + path);
                    out.write(loadPlan.toJson().getBytes(StandardCharsets.UTF_8));
                    log.debug("Completed writing load plan for " + path);
                } catch (IOException ioe) {
                    log.error("Failed to write plan for " + path, ioe);
                    throw new RuntimeException(ioe);
                }
            }));
            //@formatter:on
        }
        futures.stream().forEach(f -> {
            try {
                f.get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        log.debug("Finished writing bulk load plans to disk");
    }

    /**
     * Finds two contiguous table splits that contain the specified row should reside
     *
     * @param lookupRow
     *            Row value to be mapped
     * @param tableSplits
     *            Splits for the table in question
     * @return KeyExtent mapping for the given row
     */
    static LoadPlan.TableSplits findContainingSplits(Text lookupRow, List<Text> tableSplits) {
        int position = Collections.binarySearch(tableSplits, lookupRow);
        if (position < 0) {
            position = -1 * (position + 1);
        }

        Text prevRow = position == 0 ? null : tableSplits.get(position - 1);
        Text endRow = position == tableSplits.size() ? null : tableSplits.get(position);

        return new LoadPlan.TableSplits(prevRow, endRow);
    }

    public static class SizeTrackingWriter {
        private RFileWriter delegate;
        long size = 0;
        int entries = 0;

        public long getSize() {
            return size;
        }

        public int getNumEntries() {
            return entries;
        }

        public void startNewLocalityGroup(String name, Set<ByteSequence> columnFamilies) throws IOException {
            var fams = columnFamilies.stream().map(bs -> bs.toArray()).collect(Collectors.toList());
            delegate.startNewLocalityGroup(name, fams);
        }

        public void startDefaultLocalityGroup() throws IOException {
            delegate.startDefaultLocalityGroup();
        }

        public void append(Key key, Value value) throws IOException {
            entries++;
            size += key.getLength() + (value == null ? 0 : value.getSize());
            delegate.append(key, value);
        }

        public void close() throws IOException {
            delegate.close();
        }

        public long getLength() throws IOException {
            return getSize();
        }

        public LoadPlan getLoadPlan(String filename) {
            return delegate.getLoadPlan(filename);
        }

        public SizeTrackingWriter(RFileWriter delegate) {
            this.delegate = delegate;
        }
    }

    /**
     * Get a writer that was previously registered. This will mark the writer as being used.
     *
     * @param key
     *            a key
     * @return the writer
     * @throws IOException
     *             if there is an issue with read or write
     * @throws AccumuloException
     *             if there is an issue with accumulo
     */
    protected SizeTrackingWriter getRegisteredWriter(String key) throws IOException, AccumuloException {
        SizeTrackingWriter writer = writers.get(key);
        if (writer != null) {
            if ((maxRFileEntries > 0 && writer.getNumEntries() >= maxRFileEntries) || (maxRFileSize > 0 && writer.getSize() >= maxRFileSize)) {
                if (log.isInfoEnabled()) {
                    if (maxRFileEntries > 0 && writer.getNumEntries() >= maxRFileEntries) {
                        log.info("Breached the max RFile entries, creating a new file for " + key + ": " + writer.getNumEntries() + " >= " + maxRFileEntries);
                    } else {
                        log.info("Breached the max RFile size, creating a new file for " + key + ": " + writer.getSize() + " >= " + maxRFileSize);
                    }
                }
                closeAndUpdateWriter(key);
                writer = writers.get(key);
            }
            Path path = unusedWriterPaths.remove(key);
            if (path != null) {
                usedWriterPaths.put(key, path);
            }
        }
        return writer;
    }

    // get the sequence file block file size to use
    protected int getSeqFileBlockSize() {
        if (!tableConfigs.isEmpty()) {
            return (int) tableConfigs.values().iterator().next().getAsBytes(Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE);
        } else {
            return 0;
        }
    }

    // Get the table list
    protected Set<String> getTableList() {
        Set<String> tableList = TableConfigurationUtil.getJobOutputTableNames(conf);

        String configNames = conf.get(CONFIGURED_TABLE_NAMES, "");
        if (log.isInfoEnabled())
            log.info("Configured table names are " + configNames);

        String[] configuredTableNames = StringUtils.split(configNames, ',', false);

        if (configuredTableNames.length > 0)
            tableList.addAll(Arrays.asList(configuredTableNames));

        if (log.isInfoEnabled())
            log.info("All table names are " + tableList);

        return tableList;
    }

    protected void setTableIdsAndConfigs() throws IOException {

        tableConfigs = new HashMap<>();
        Iterable<String> localityGroupTables = Splitter.on(",").split(conf.get(CONFIGURE_LOCALITY_GROUPS, ""));

        TableConfigurationUtil tcu = new TableConfigurationUtil(conf);

        tableIds = tcu.getJobOutputTableNames(conf);
        Set<String> compressionTableDisallowList = getCompressionTableDisallowList(conf);
        String compressionType = getCompressionType(conf);
        for (String tableName : tableIds) {
            Map<String,String> properties = tcu.getTableProperties(tableName);
            if (null == properties || properties.isEmpty()) {
                log.error("No properties found for table " + tableName);
            } else {
                ConfigurationCopy tableConfig = new ConfigurationCopy(properties);
                tableConfig.set(Property.TABLE_FILE_COMPRESSION_TYPE.getKey(),
                                (compressionTableDisallowList.contains(tableName) ? new NoCompression().getName() : compressionType));

                // the locality groups feature is broken and will be removed in a future MR
                if (Iterables.contains(localityGroupTables, tableName)) {
                    Map<String,Set<Text>> localityGroups = tcu.getLocalityGroups(tableName);
                    // pull the locality groups for this table.
                    Map<Text,String> cftlg = Maps.newHashMap();
                    Map<String,Set<ByteSequence>> lgtcf = Maps.newHashMap();
                    for (Entry<String,Set<Text>> locs : localityGroups.entrySet()) {
                        lgtcf.put(locs.getKey(), new HashSet<>());
                        for (Text loc : locs.getValue()) {
                            cftlg.put(loc, locs.getKey());
                            lgtcf.get(locs.getKey()).add(new ArrayByteSequence(loc.getBytes()));
                        }
                    }
                    columnFamilyToLocalityGroup.put(tableName, cftlg);
                    localityGroupToColumnFamilies.put(tableName, lgtcf);
                }
                tableConfigs.put(tableName, tableConfig);
            }
        }
    }

    // Creating because super class does not allow overridding
    private SafeFileOutputCommitter _committer = null;

    @Override
    public synchronized OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException {
        if (_committer == null) {
            Path output = getOutputPath(context);
            _committer = new SafeFileOutputCommitter(output, context);
        }
        return _committer;
    }

    // Return the record writer
    @Override
    public RecordWriter<BulkIngestKey,Value> getRecordWriter(final TaskAttemptContext context) throws IOException, InterruptedException {
        MarkingFunctions.Factory.createMarkingFunctions();
        // get the task output path
        FileOutputCommitter committer = (FileOutputCommitter) getOutputCommitter(context);
        workDir = committer.getWorkPath();
        conf = context.getConfiguration();

        setTableIdsAndConfigs();

        fs = workDir.getFileSystem(conf);

        columnFamilyToLocalityGroup = Maps.newHashMap();
        localityGroupToColumnFamilies = Maps.newHashMap();

        loadPlanningEnabled = loadPlanningEnabled(conf);
        if (loadPlanningEnabled) {
            loadPlans = new ConcurrentHashMap<>();
            loadPlanFutures = new LinkedList<>();
        }

        extension = conf.get(FILE_TYPE);
        if (extension == null || extension.isEmpty())
            extension = RFile.EXTENSION;
        extension = "." + extension;

        conf.setInt("io.seqfile.compress.blocksize", getSeqFileBlockSize());

        // Get the list of tables

        String[] tableNames = conf.getStrings(SplitsFile.CONFIGURED_SHARDED_TABLE_NAMES);

        if (null == tableNames) {
            log.warn("Could not find the list of sharded table names");
            tableNames = new String[0];
        }

        // Make a set from the list
        shardedTableNames = new HashSet<>(tableNames.length);
        Collections.addAll(shardedTableNames, tableNames);
        shardedTablesConfigured = new HashSet<>(tableNames.length);

        eventTable = conf.get(ShardedDataTypeHandler.SHARD_TNAME, "");
        if (log.isInfoEnabled())
            log.info("Event Table Name property for " + ShardedDataTypeHandler.SHARD_TNAME + " is " + eventTable);

        maxRFileEntries = conf.getInt(MAX_RFILE_UNDEDUPPED_ENTRIES, maxRFileEntries);
        maxRFileSize = conf.getLong(MAX_RFILE_UNCOMPRESSED_SIZE, maxRFileSize);

        generateMapFileRowKeys = conf.getBoolean(GENERATE_MAP_FILE_ROW_KEYS, generateMapFileRowKeys);
        generateMapFilePerShardLocation = conf.getBoolean(GENERATE_MAP_FILE_PER_SHARD_LOCATION, generateMapFilePerShardLocation);

        // Only do this once.
        if (null == writers) {
            writers = new HashMap<>();
            unusedWriterPaths = new HashMap<>();
            usedWriterPaths = new HashMap<>();
            writerTableNames = new HashMap<>();
            writerCounts = new HashMap<>();

            Set<String> tableList = getTableList();

            for (String table : tableList) {
                if (shardedTableNames.contains(table)) {
                    shardedTablesConfigured.add(table);
                }

                if (!tableIds.contains(table)) {
                    throw new IOException("Unable to determine id for table " + table);
                }
                Path tableDir = new Path(workDir, table);
                // Don't create a writer if this is the sharded table. Instead, we'll create writers on the
                // fly per designated tablet server where the shards are to be served.
                if (!shardedTableNames.contains(table)) {
                    // Create a subdirectory with the table name
                    Path tableFile = new Path(tableDir, getUniqueFile(context, table, extension));
                    try {
                        createAndRegisterWriter(table, table, tableFile, tableConfigs.get(table));
                    } catch (AccumuloException ex) {
                        throw new IOException(ex);
                    }
                }
            }
        }

        return new RecordWriter<BulkIngestKey,Value>() {
            private String currentLocalityGroup = null;

            @Override
            public void write(BulkIngestKey key, Value value) throws IOException {
                String tableName = key.getTableName().toString();
                SizeTrackingWriter writer;
                try {
                    writer = getOrCreateWriter(context, tableName, key.getKey().getRow());
                } catch (AccumuloException e1) {
                    throw new IOException("Unable to create writer", e1);
                }
                if (log.isTraceEnabled()) {
                    log.trace("Appending " + key.getKey());
                }

                final Text keyCf = key.getKey().getColumnFamily();
                final Map<Text,String> cftlg = columnFamilyToLocalityGroup.get(tableName);
                if (null != cftlg) {
                    String localityGroup = cftlg.get(keyCf);
                    boolean create = false;
                    if (null == currentLocalityGroup) // defaultLocalityGroup
                    {
                        create = true;
                    } else if (currentLocalityGroup.compareTo(localityGroup) <= 0) {
                        create = true;
                    }

                    if (create) {
                        writer.startNewLocalityGroup(localityGroup, localityGroupToColumnFamilies.get(tableName).get(localityGroup));
                        currentLocalityGroup = localityGroup;
                    }
                }
                writer.append(key.getKey(), value);
            }

            @Override
            public void close(TaskAttemptContext context) throws IOException, InterruptedException {
                for (Map.Entry<String,SizeTrackingWriter> entry : writers.entrySet()) {
                    var writer = entry.getValue();
                    writer.close();
                    if (loadPlanningEnabled) {
                        var tableName = writerTableNames.get(entry.getKey());
                        var rfilePath = usedWriterPaths.get(entry.getKey());
                        addLoadPlan(tableName, writer.getLoadPlan(rfilePath.getName()));
                    }

                }

                if (loadPlanningEnabled) {
                    commitLoadPlans(context);
                }

                // To verify the file was actually written successfully, we need to reopen it which will reread
                // the index at the end and verify its integrity.
                FileOperations fops = FileOperations.getInstance();
                for (Map.Entry<String,Path> entry : usedWriterPaths.entrySet()) {
                    Path path = entry.getValue();
                    String table = writerTableNames.get(entry.getKey());
                    try {
                        CryptoService cs = CryptoFactoryLoader.getServiceForClient(CryptoEnvironment.Scope.TABLE,
                                        context.getConfiguration().getPropsWithPrefix(TABLE_CRYPTO_PREFIX.name()));
                        FileSKVIterator openReader = fops.newReaderBuilder().forFile(path.toString(), fs, conf, cs)
                                        .withTableConfiguration(tableConfigs.get(table)).build();
                        FileStatus fileStatus = fs.getFileStatus(path);
                        long fileSize = fileStatus.getLen();
                        openReader.close();
                        log.info("Successfully wrote " + path + ". Total size: " + fileSize + " B. Total time: " + (System.currentTimeMillis() - startWriteTime)
                                        + " ms.");
                    } catch (Exception ex) {
                        log.error("Verification of successful RFile completion failed!!! " + path, ex);
                        throw new IOException(ex);
                    }
                }
                for (Path path : unusedWriterPaths.values()) {
                    log.info("Nothing written to " + path + ".  Deleting from HDFS.");
                    fs.delete(path, true);
                }
                if (generateMapFileRowKeys && !shardMapFileRowKeys.isEmpty()) {
                    log.info("Writing mapFileRowKeys");
                    Path shardMapFilePath = new Path(workDir, getUniqueFile(context, "mapFileRowKeys", ".lst"));
                    try (SequenceFile.Writer output = SequenceFile.createWriter(fs, conf, shardMapFilePath, Text.class, Text.class)) {
                        for (Map.Entry<String,Set<Text>> entry : shardMapFileRowKeys.entrySet()) {
                            Path path = shardMapFiles.get(entry.getKey());
                            Text pathText = new Text(path.getParent().getName() + "/" + path.getName());
                            for (Text rowKey : entry.getValue()) {
                                output.append(pathText, rowKey);
                            }
                        }
                    }
                }
            }

            private SizeTrackingWriter getOrCreateWriter(TaskAttemptContext context, String tableName, Text rowKey) throws IOException, AccumuloException {
                SizeTrackingWriter writer;
                if (shardedTableNames.contains(tableName)) {
                    if (!shardedTablesConfigured.contains(tableName)) {
                        throw new IOException("Asked to create writer for sharded table " + tableName
                                        + ", however this table was not in the configured set of ingest job tables");
                    }
                    // By default, throw all shards for a table into one output rfile. If the number of tservers is small enough, we may want to
                    // produce one rfile per shard location (tserver) in order to be more efficient. However, when the number of tservers is large
                    // enough, the overhead of dealing with the large number of files over a large number of jobs may overwhelm HDFS, so we're better
                    // off having fewer files even though more tablets will end up referring to each file.
                    String shardLocation = "shards";
                    if (generateMapFilePerShardLocation) {
                        // Look up the shard location (tablet server serving shard ID rowKey)
                        // If we don't have a location, then just use the rowKey itself.
                        Map<Text,String> shardLocs = getShardLocations(tableName);
                        shardLocation = shardLocs.containsKey(rowKey) ? shardLocs.get(rowKey) : null;
                        if (shardLocation == null) {
                            // in this case we have a shard id that has no split. Lets put this in one "extra" file
                            shardLocation = "extra";
                        } else {
                            // Ensure there's no colon
                            shardLocation = shardLocation.replace(":", "_");
                        }
                    }
                    // Combine table name with shard location so that we end up
                    // with all of the shard map files under directories that can be
                    // pattern matched.
                    String writerKey = tableName + "-" + shardLocation;
                    writer = getRegisteredWriter(writerKey);
                    if (writer == null) {
                        Path tableDir = new Path(workDir, tableName);
                        Path tableFile = new Path(tableDir, getUniqueFile(context, shardLocation, extension));
                        createAndRegisterWriter(writerKey, tableName, tableFile, tableConfigs.get(tableName));
                        writer = getRegisteredWriter(writerKey);
                    }

                    shardMapFileRowKeys.get(writerKey).add(rowKey);
                } else {
                    writer = getRegisteredWriter(tableName);
                    if (writer == null) {
                        throw new IOException("Asked to create writer for table " + tableName
                                        + ", however this table was not in the configured set of ingest job tables");
                    }
                }
                return writer;
            }
        };
    }

    /**
     * Read in the sequence file (that was created at job startup) for the given table that contains a list of shard IDs and the corresponding tablet server to
     * which that shard is assigned.
     *
     * @param tableName
     *            the table name
     * @return a mapping of the shard ids and tablet server
     * @throws IOException
     *             if there is an issue with read or write
     */
    protected Map<Text,String> getShardLocations(String tableName) throws IOException {
        // Create the Map of sharded table name to [shardId -> server]
        if (this.tableShardLocations == null) {
            this.tableShardLocations = new HashMap<>();
        }

        if (null == this.tableShardLocations.get(tableName)) {
            this.tableShardLocations.put(tableName, SplitsFile.getSplitsAndLocations(conf, tableName));
        }

        return tableShardLocations.get(tableName);
    }
}
