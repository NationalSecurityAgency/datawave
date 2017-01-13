package nsa.datawave.mr.bulk;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import nsa.datawave.ingest.data.config.ingest.AccumuloHelper;
import nsa.datawave.ingest.mapreduce.job.RFileInputFormat;
import nsa.datawave.mr.bulk.split.FileRangeSplit;
import nsa.datawave.mr.bulk.split.TabletSplitSplit;

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
    private static final String HDFS_BASE = "hdfs://";
    private static final String ACCUMULO_BASE_PATH = "/accumulo";
    
    private static final String FS_DEFAULT_NAME = "fs.default.name";
    private static final Logger log = Logger.getLogger(MultiRfileInputformat.class);
    private static final String tableStr = Path.SEPARATOR + "tables" + Path.SEPARATOR;
    
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
    
    public static List<InputSplit> computeSplitPoints(Configuration conf, String tableName, List<Range> ranges) throws TableNotFoundException,
                    AccumuloException, AccumuloSecurityException, IOException, InterruptedException {
        
        final Multimap<Range,FileRangeSplit> binnedRanges = ArrayListMultimap.create();
        
        final Instance instance = BulkInputFormat.getInstance(conf);
        
        final PasswordToken token = new PasswordToken(BulkInputFormat.getPassword(conf));
        
        final Connector conn = instance.getConnector(BulkInputFormat.getUsername(conf), token);
        
        final String tableId = Tables.getTableId(instance, tableName);
        
        final List<InputSplit> inputSplitList = Lists.newArrayList();
        
        Multimap<Text,Range> rowMap = TreeMultimap.create();
        
        String defaultNamespace = null, basePath = null;
        
        /**
         * Attempt the following 1) try to get the default namespace from accumulo 2) Use the custom config option 3) use default name in the hdfs configuration
         */
        final InstanceOperations instOps = conn.instanceOperations();
        
        if (null != instOps) {
            defaultNamespace = instOps.getSystemConfiguration().get(Property.INSTANCE_DFS_URI.getKey());
        }
        
        if (StringUtils.isEmpty(defaultNamespace)) {
            defaultNamespace = conf.get(FS_DEFAULT_NAMESPACE);
            
            if (StringUtils.isEmpty(defaultNamespace)) {
                defaultNamespace = conf.get(FS_DEFAULT_NAME);
            }
        }
        if (null != instOps)
            basePath = instOps.getSystemConfiguration().get(Property.INSTANCE_DFS_DIR.getKey());
        
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
        
        for (Range range : ranges) {
            Text startRow;
            
            if (range.getStartKey() != null)
                startRow = range.getStartKey().getRow();
            else
                startRow = new Text();
            
            Key startKey = new Key(new KeyExtent(new Text(tableId), startRow, null).getMetadataEntry());
            Range metadataRange = new Range(startKey, true, startKey.followingKey(PartialKey.ROW), false);
            
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
                        if (!fileLocation.contains(HDFS_BASE))
                            fileLocation = baseLocation.concat(entry.getKey().getColumnQualifier().toString());
                        fileLocations.add(fileLocation);
                    }
                    
                    if (key.getColumnFamily().equals(MetadataSchema.TabletsSection.CurrentLocationColumnFamily.NAME)
                                    || key.getColumnFamily().equals(MetadataSchema.TabletsSection.FutureLocationColumnFamily.NAME)) {
                        location = entry.getValue().toString();
                    }
                    
                }
                
                if (location.isEmpty() || fileLocations.isEmpty())
                    log.error("Unable to find location or files associated with " + range.toString() + " " + location + " " + fileLocations);
                
                for (String fileLocation : fileLocations) {
                    
                    Path path = new Path(fileLocation);
                    
                    long length = path.getFileSystem(conf).getFileStatus(path).getLen();
                    String[] locations = new String[] {location};
                    FileRangeSplit newSplit = new FileRangeSplit(range, path, 0, length, locations);
                    
                    binnedRanges.put(range, newSplit);
                    
                    rowMap.put(range.getStartKey().getRow(), range);
                }
            }
            
        }
        
        boolean mergeRanges = conf.getBoolean(MERGE_RANGE, true);
        
        if (!mergeRanges) {
            for (Range range : binnedRanges.keySet()) {
                Collection<FileRangeSplit> rangeSplits = binnedRanges.get(range);
                
                if (0 == rangeSplits.size())
                    continue;
                TabletSplitSplit compositeInputSplit = new TabletSplitSplit(rangeSplits.size());
                for (FileRangeSplit split : rangeSplits) {
                    compositeInputSplit.add(split);
                }
                
                inputSplitList.add(compositeInputSplit);
            }
        } else {
            
            for (Text row : rowMap.keySet()) {
                
                Collection<Range> rangeColl = rowMap.get(row);
                int size = 0;
                for (Range range : rangeColl) {
                    Collection<FileRangeSplit> rangeSplits = binnedRanges.get(range);
                    size += rangeSplits.size();
                }
                
                TabletSplitSplit compositeInputSplit = new TabletSplitSplit(size);
                for (Range range : rangeColl) {
                    Collection<FileRangeSplit> rangeSplits = binnedRanges.get(range);
                    
                    if (0 == rangeSplits.size())
                        continue;
                    
                    for (FileRangeSplit split : rangeSplits) {
                        compositeInputSplit.add(split);
                    }
                    
                }
                
                inputSplitList.add(compositeInputSplit);
            }
        }
        
        if (log.isTraceEnabled())
            log.trace("Size is " + inputSplitList.size());
        
        return inputSplitList;
    }
    
}
