package nsa.datawave.mr.bulk;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

import nsa.datawave.ingest.data.config.ingest.AccumuloHelper;
import nsa.datawave.ingest.mapreduce.job.RFileInputFormat;
import nsa.datawave.mr.bulk.split.FileRangeSplit;
import nsa.datawave.mr.bulk.split.RfileSplit;
import nsa.datawave.mr.bulk.split.TabletSplitSplit;
import nsa.datawave.query.util.Tuple2;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.RowIterator;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.InstanceOperations;
import org.apache.accumulo.core.client.impl.Tables;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyExtent;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeMultimap;

public class MultiRfileInputformat extends RFileInputFormat {
    /**
     * The following can be set via your configuration to override the default namespace
     */
    public static final String FS_DEFAULT_NAMESPACE = "fs.default.namespace";
    /**
     * Merge ranges can be overrided to the default of true so that you don't merge on the hosting tablet server
     */
    public static final String MERGE_RANGE = "merge.range";
    public static final String CACHE_METADATA = "rfile.cache.metdata";
    public static final String CACHE_RETRIEVE_SIZE = "rfile.size.compute";
    public static final String CACHE_METADATA_SIZE = "rfile.cache.metdata.size";
    public static final String CACHE_METADATA_RETRIES = "rfile.cache.metdata.retries";
    private static final String HDFS_BASE = "hdfs://";
    private static final String ACCUMULO_BASE_PATH = "/accumulo";
    
    private static final String FS_DEFAULT_NAME = "fs.default.name";
    private static final Logger log = Logger.getLogger(MultiRfileInputformat.class);
    public static final String tableStr = Path.SEPARATOR + "tables" + Path.SEPARATOR;
    
    private static LoadingCache<Range,Tuple2<String,Set<String>>> locationMap = null;
    
    protected static Map<String,String> dfsUriMap = new ConcurrentHashMap<String,String>();
    protected static Map<String,String> dfsDirMap = new ConcurrentHashMap<String,String>();
    
    @Override
    public RecordReader<Key,Value> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new RangeRecordReader();
    }
    
    /**
     * Return the lists of computed slit points
     */
    public List<InputSplit> getSplits(JobContext job) throws IOException {
        
        AccumuloHelper cbHelper = new AccumuloHelper();
        cbHelper.setup(job.getConfiguration());
        
        String tableName = BulkInputFormat.getTablename(job.getConfiguration());
        boolean autoAdjust = BulkInputFormat.getAutoAdjustRanges(job.getConfiguration());
        List<Range> ranges = autoAdjust ? Range.mergeOverlapping(BulkInputFormat.getRanges(job.getConfiguration())) : BulkInputFormat.getRanges(job
                        .getConfiguration());
        
        if (ranges.isEmpty()) {
            ranges = Lists.newArrayListWithCapacity(1);
            ranges.add(new Range());
        }
        
        List<InputSplit> inputSplits = Lists.newArrayList();
        try {
            inputSplits = computeSplitPoints(job, tableName, ranges);
        } catch (TableNotFoundException | AccumuloException | AccumuloSecurityException | InterruptedException e) {
            throw new IOException(e);
        }
        
        return inputSplits;
    }
    
    List<InputSplit> computeSplitPoints(JobContext job, String tableName, List<Range> ranges) throws TableNotFoundException, AccumuloException,
                    AccumuloSecurityException, IOException, InterruptedException {
        return computeSplitPoints(job.getConfiguration(), tableName, ranges);
    }
    
    public static void clearMetadataCache() {
        synchronized (MultiRfileInputformat.class) {
            if (null == locationMap) {
                return;
            }
        }
        locationMap.invalidateAll();
    }
    
    public static void clearMetadataCache(Range range) {
        
        synchronized (MultiRfileInputformat.class) {
            if (null == locationMap) {
                return;
            }
        }
        
        locationMap.invalidate(range);
        locationMap.refresh(range);
    }
    
    public static List<InputSplit> computeSplitPoints(Configuration conf, String tableName, List<Range> ranges) throws TableNotFoundException,
                    AccumuloException, AccumuloSecurityException, IOException, InterruptedException {
        final Instance instance = BulkInputFormat.getInstance(conf);
        final PasswordToken token = new PasswordToken(BulkInputFormat.getPassword(conf));
        return computeSplitPoints(instance.getConnector(BulkInputFormat.getUsername(conf), token), conf, tableName, ranges);
    }
    
    public static List<InputSplit> computeSplitPoints(Connector conn, Configuration conf, String tableName, List<Range> ranges) throws TableNotFoundException,
                    AccumuloException, AccumuloSecurityException, IOException, InterruptedException {
        
        final Multimap<Range,RfileSplit> binnedRanges = ArrayListMultimap.create();
        
        final Instance instance = conn.getInstance();
        final PasswordToken token = new PasswordToken(BulkInputFormat.getPassword(conf));
        
        final String tableId = Tables.getTableId(instance, tableName);
        
        final List<InputSplit> inputSplitList = Lists.newArrayList();
        
        Multimap<Text,Range> rowMap = TreeMultimap.create();
        
        String defaultNamespace = null, basePath = null;
        
        /**
         * Attempt the following 1) try to get the default namespace from accumulo 2) Use the custom config option 3) use default name in the hdfs configuration
         */
        if (dfsUriMap.get(tableId) == null || dfsDirMap.get(tableId) == null) {
            
            synchronized (MultiRfileInputformat.class) {
                final InstanceOperations instOps = conn.instanceOperations();
                dfsUriMap.put(tableId, instOps.getSystemConfiguration().get(Property.INSTANCE_DFS_URI.getKey()));
                dfsDirMap.put(tableId, instOps.getSystemConfiguration().get(Property.INSTANCE_DFS_DIR.getKey()));
            }
        }
        
        defaultNamespace = dfsUriMap.get(tableId);
        
        if (StringUtils.isEmpty(defaultNamespace)) {
            defaultNamespace = conf.get(FS_DEFAULT_NAMESPACE);
            
            if (StringUtils.isEmpty(defaultNamespace)) {
                defaultNamespace = conf.get(FS_DEFAULT_NAME);
            }
        }
        
        basePath = dfsDirMap.get(tableId);
        
        if (StringUtils.isEmpty(basePath)) {
            basePath = ACCUMULO_BASE_PATH;
        }
        
        // ensure we have a separator
        if (!basePath.startsWith(Path.SEPARATOR)) {
            basePath = Path.SEPARATOR + basePath;
        }
        
        // must get the default base path since accumulo only stores the full namespace path
        // when one is not stored on the default.
        final String defaultBasePath = defaultNamespace + basePath;
        
        if (conf.getBoolean(CACHE_METADATA, false) == true) {
            synchronized (MultiRfileInputformat.class) {
                if (null == locationMap) {
                    final int retries = conf.getInt(CACHE_METADATA_RETRIES, Integer.MAX_VALUE);
                    final long size = conf.getLong(CACHE_METADATA_SIZE, 10000);
                    locationMap = CacheBuilder.newBuilder().maximumSize(size)
                                    .build(new MetadataCacheLoader(instance.getConnector(BulkInputFormat.getUsername(conf), token), defaultBasePath, retries));
                }
            }
        }
        
        for (Range range : ranges) {
            Text startRow;
            
            if (range.getStartKey() != null)
                startRow = range.getStartKey().getRow();
            else
                startRow = new Text();
            
            Key startKey = new Key(new KeyExtent(new Text(tableId), startRow, null).getMetadataEntry());
            Range metadataRange = new Range(startKey, true, startKey.followingKey(PartialKey.ROW), false);
            if (null == locationMap) {
                Scanner scanner = conn.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
                MetadataSchema.TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN.fetch(scanner);
                scanner.fetchColumnFamily(MetadataSchema.TabletsSection.LastLocationColumnFamily.NAME);
                scanner.fetchColumnFamily(MetadataSchema.TabletsSection.DataFileColumnFamily.NAME);
                scanner.fetchColumnFamily(MetadataSchema.TabletsSection.CurrentLocationColumnFamily.NAME);
                scanner.fetchColumnFamily(MetadataSchema.TabletsSection.FutureLocationColumnFamily.NAME);
                
                scanner.setRange(metadataRange);
                
                RowIterator rowIter = new RowIterator(scanner);
                
                String baseLocation = defaultBasePath + tableStr + tableId + Path.SEPARATOR;
                
                while (rowIter.hasNext()) {
                    Iterator<Entry<Key,Value>> row = rowIter.next();
                    String location = "";
                    Set<String> fileLocations = Sets.newHashSet();
                    
                    while (row.hasNext()) {
                        Entry<Key,Value> entry = row.next();
                        Key key = entry.getKey();
                        
                        if (key.getColumnFamily().equals(MetadataSchema.TabletsSection.DataFileColumnFamily.NAME)) {
                            String fileLocation = entry.getKey().getColumnQualifier().toString();
                            if (!fileLocation.startsWith(HDFS_BASE))
                                fileLocation = baseLocation.concat(entry.getKey().getColumnQualifier().toString());
                            fileLocations.add(fileLocation);
                        }
                        
                        if (key.getColumnFamily().equals(MetadataSchema.TabletsSection.CurrentLocationColumnFamily.NAME)
                                        || key.getColumnFamily().equals(MetadataSchema.TabletsSection.FutureLocationColumnFamily.NAME)) {
                            location = entry.getValue().toString();
                        }
                        
                    }
                    
                    if (location.isEmpty() || fileLocations.isEmpty())
                        throw new IOException("Unable to find location or files associated with " + range.toString() + " " + location + " " + fileLocations);
                    
                    for (String fileLocation : fileLocations) {
                        
                        Path path = new Path(fileLocation);
                        
                        long length = path.getFileSystem(conf).getFileStatus(path).getLen();
                        String[] locations = new String[] {location};
                        
                        binnedRanges.put(range, new RfileSplit(path, 0, length, locations));
                        
                        rowMap.put(range.getStartKey().getRow(), range);
                    }
                }
                
            } else {
                Tuple2<String,Set<String>> cachedMetadata;
                try {
                    cachedMetadata = locationMap.get(metadataRange);
                    
                    if (null != cachedMetadata && cachedMetadata.first() != null && cachedMetadata.second() != null) {
                        String location = cachedMetadata.first();
                        Set<String> fileLocations = cachedMetadata.second();
                        for (String fileLocation : fileLocations) {
                            
                            Path path = new Path(fileLocation);
                            
                            boolean pullSize = conf.getBoolean(CACHE_RETRIEVE_SIZE, false);
                            long length = Long.MAX_VALUE;
                            if (pullSize) {
                                length = path.getFileSystem(conf).getFileStatus(path).getLen();
                            }
                            
                            String[] locations = new String[] {location};
                            
                            binnedRanges.put(range, new RfileSplit(path, 0, length, locations));
                            
                            rowMap.put(range.getStartKey().getRow(), range);
                        }
                    }
                } catch (ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        
        boolean mergeRanges = conf.getBoolean(MERGE_RANGE, true);
        
        if (!mergeRanges) {
            for (Range range : binnedRanges.keySet()) {
                Collection<RfileSplit> rangeSplits = binnedRanges.get(range);
                
                if (0 == rangeSplits.size())
                    continue;
                TabletSplitSplit compositeInputSplit = new TabletSplitSplit(rangeSplits.size());
                compositeInputSplit.setTable(tableName);
                for (RfileSplit split : rangeSplits) {
                    compositeInputSplit.add(new FileRangeSplit(range, split.path, 0, split.length, split.hosts));
                }
                
                inputSplitList.add(compositeInputSplit);
            }
        } else {
            
            for (Text row : rowMap.keySet()) {
                
                Collection<Range> rangeColl = rowMap.get(row);
                
                Set<RfileSplit> rfiles = Sets.newHashSet();
                
                for (Range range : rangeColl) {
                    Collection<RfileSplit> rangeSplits = binnedRanges.get(range);
                    rfiles.addAll(rangeSplits);
                }
                
                TabletSplitSplit compositeInputSplit = new TabletSplitSplit(rfiles.size());
                compositeInputSplit.setTable(tableName);
                for (RfileSplit split : rfiles) {
                    
                    compositeInputSplit.add(new FileRangeSplit(rangeColl, split.path, 0, split.length, split.hosts));
                    
                }
                
                inputSplitList.add(compositeInputSplit);
            }
        }
        
        if (log.isTraceEnabled())
            log.trace("Size is " + inputSplitList.size());
        
        return inputSplitList;
    }
}
