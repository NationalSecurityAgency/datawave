package datawave.mr.bulk;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import java.util.concurrent.TimeUnit;
import datawave.ingest.data.config.ingest.AccumuloHelper;
import datawave.ingest.mapreduce.job.RFileInputFormat;
import datawave.mr.bulk.split.FileRangeSplit;
import datawave.mr.bulk.split.RfileSplit;
import datawave.mr.bulk.split.TabletSplitSplit;
import datawave.query.util.Tuple2;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.InstanceOperations;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.data.Value;
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
    public static final String CACHE_METADATA_EXPIRE_SECONDS = "rfile.cache.expire.seconds";
    public static final String CACHE_RETRIEVE_SIZE = "rfile.size.compute";
    public static final String CACHE_METADATA_SIZE = "rfile.cache.metdata.size";
    private static final String HDFS_BASE = "hdfs://";
    private static final String ACCUMULO_BASE_PATH = "/accumulo";
    
    private static final String FS_DEFAULT_NAME = "fs.default.name";
    private static final Logger log = Logger.getLogger(MultiRfileInputformat.class);
    public static final String tableStr = Path.SEPARATOR + "tables" + Path.SEPARATOR;
    
    private static LoadingCache<Range,Set<Tuple2<String,Set<String>>>> locationMap = null;
    
    protected static Map<String,String> dfsUriMap = new ConcurrentHashMap<>();
    protected static Map<String,String> dfsDirMap = new ConcurrentHashMap<>();
    
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
        
        final String tableId = conn.tableOperations().tableIdMap().get(tableName);
        
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
                    final long size = conf.getLong(CACHE_METADATA_SIZE, 10000);
                    final long seconds = conf.getInt(CACHE_METADATA_EXPIRE_SECONDS, 7200);
                    locationMap = CacheBuilder.newBuilder().maximumSize(size).expireAfterWrite(seconds, TimeUnit.SECONDS)
                                    .build(new MetadataCacheLoader(instance.getConnector(BulkInputFormat.getUsername(conf), token), defaultBasePath));
                }
            }
        }
        
        for (Range range : ranges) {
            // turn this range into a range of rows against the accumulo metadata: (e.g. <tableId>;row)
            Range metadataRange = MetadataCacheLoader.createMetadataRange(TableId.of(tableId), range);
            
            Set<Tuple2<String,Set<String>>> metadataEntries;
            try {
                if (null == locationMap) {
                    metadataEntries = new MetadataCacheLoader(conn, defaultBasePath).load(metadataRange);
                } else {
                    metadataEntries = locationMap.get(metadataRange);
                }
            } catch (Exception e) {
                throw new RuntimeException("Unable to get rfile locations from accumulo metadata", e);
            }
            
            if (metadataEntries == null || metadataEntries.isEmpty()) {
                throw new IOException("Unable to find location or files associated with " + range);
            }
            
            for (Tuple2<String,Set<String>> entry : metadataEntries) {
                String location = entry.first();
                Set<String> fileLocations = entry.second();
                if (fileLocations != null && !fileLocations.isEmpty()) {
                    
                    if (location == null || location.isEmpty()) {
                        log.warn("Unable to find a location associated with " + range + " : ? -> " + fileLocations);
                    }
                    
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
                } else {
                    log.warn("Unable to find a some files associated with " + range + " : " + location);
                }
            }
        }
        
        boolean mergeRanges = conf.getBoolean(MERGE_RANGE, true);
        
        if (!mergeRanges) {
            for (Range range : binnedRanges.keySet()) {
                Collection<RfileSplit> rangeSplits = binnedRanges.get(range);
                
                if (rangeSplits.isEmpty())
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
