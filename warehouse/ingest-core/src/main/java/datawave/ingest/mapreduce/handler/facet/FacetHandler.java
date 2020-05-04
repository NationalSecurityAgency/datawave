package datawave.ingest.mapreduce.handler.facet;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.clearspring.analytics.stream.cardinality.ICardinality;
import com.clearspring.analytics.stream.frequency.CountMinSketch;
import com.clearspring.analytics.util.Lists;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.Type;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.ConfigurationHelper;
import datawave.ingest.data.config.DataTypeHelper.Properties;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.ingest.IngestHelperInterface;
import datawave.ingest.mapreduce.handler.ExtendedDataTypeHandler;
import datawave.ingest.mapreduce.handler.shard.ShardIdFactory;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.mapreduce.job.writer.ContextWriter;
import datawave.ingest.metadata.RawRecordMetadata;
import datawave.marking.MarkingFunctions;
import datawave.util.StringUtils;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FacetHandler<KEYIN,KEYOUT,VALUEOUT> implements ExtendedDataTypeHandler<KEYIN,KEYOUT,VALUEOUT>, FacetedEstimator<RawRecordContainer> {
    
    private static final Logger log = Logger.getLogger(FacetHandler.class);
    
    /* Global configuration properties */
    
    public static final String FACET_TABLE_NAME = "facet.table.name";
    public static final String FACET_LPRIORITY = "facet.table.loader.priority";
    
    public static final String FACET_METADATA_TABLE_NAME = "facet.metadata.table.name";
    public static final String FACET_METADATA_LPRIORITY = "facet.metadata.table.loader.priority";
    
    public static final String FACET_HASH_TABLE_NAME = "facet.hash.table.name";
    public static final String FACET_HASH_TABLE_LPRIORITY = "facet.hash.table.loader.priority";
    
    /* Per-datatype configuration properties */
    
    public static final String FACET_HASH_THRESHOLD = ".facet.hash.threshold";
    
    public static final String FACET_CATEGORY_DELIMITER = ".facet.category.delimiter";
    public static final String FACET_FIELD_PREDICATE_CLASS = ".facet.field.predicate.class";
    
    public static final String FACET_CATEGORY_PREFIX_REGEX = "\\.facet\\.category\\.name\\..*";
    
    private static final Text PV = new Text("pv");
    private static final String NULL = "\0";
    private static final Value EMPTY_VALUE = new Value(new byte[] {});
    
    /* Global configuration fields */
    
    protected Text facetTableName;
    protected Text facetMetadataTableName;
    protected Text facetHashTableName;
    
    /* Per-datatype configuration fields */
    
    protected int facetHashThreshold;
    protected String categoryDelimiter = "/";
    
    /* Instance variables */
    
    protected MarkingFunctions markingFunctions;
    protected ShardIdFactory shardIdFactory;
    protected TaskAttemptContext taskAttemptContext;
    
    protected Predicate<String> fieldFilter = null;
    protected Multimap<String,String> pivotMap;
    
    @Override
    public void setup(TaskAttemptContext context) {
        markingFunctions = MarkingFunctions.Factory.createMarkingFunctions();
        
        taskAttemptContext = context;
        
        Configuration conf = context.getConfiguration();
        
        final String t = ConfigurationHelper.isNull(conf, Properties.DATA_NAME, String.class);
        TypeRegistry.getInstance(conf);
        Type type = TypeRegistry.getType(t);
        
        categoryDelimiter = conf.get(type.typeName() + FACET_CATEGORY_DELIMITER, categoryDelimiter);
        
        Map<String,String> categories = conf.getValByRegex(type.typeName() + FACET_CATEGORY_PREFIX_REGEX);
        
        pivotMap = HashMultimap.create();
        
        if (null != categories) {
            for (Map.Entry<String,String> category : categories.entrySet()) {
                final String fields = category.getValue();
                Preconditions.checkNotNull(fields);
                final String[] fieldArray = StringUtils.split(fields, categoryDelimiter.charAt(0));
                Preconditions.checkArgument(fieldArray.length == 2);
                final String pivot = fieldArray[0];
                final String[] facets = StringUtils.split(fieldArray[1], ',');
                pivotMap.putAll(pivot, ImmutableList.copyOf(facets));
            }
        } else {
            throw new IllegalStateException("Categories must be specified");
        }
        
        String predClazzStr = conf.get(FACET_FIELD_PREDICATE_CLASS);
        if (null != predClazzStr) {
            try {
                // Will throw RuntimeException if class can't be coerced into Predicate<String>
                @SuppressWarnings("unchecked")
                Class<Predicate<String>> projClazz = (Class<Predicate<String>>) Class.forName(predClazzStr).asSubclass(Predicate.class);
                fieldFilter = projClazz.newInstance();
            } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
        
        shardIdFactory = new ShardIdFactory(conf);
        facetTableName = new Text(ConfigurationHelper.isNull(conf, FACET_TABLE_NAME, String.class));
        facetMetadataTableName = new Text(conf.get(FACET_METADATA_TABLE_NAME, facetTableName.toString() + "Metadata"));
        facetHashTableName = new Text(conf.get(FACET_HASH_TABLE_NAME, facetTableName.toString() + "Hash"));
        facetHashThreshold = conf.getInt(type.typeName() + FACET_HASH_THRESHOLD, 20);
    }
    
    @Override
    public String[] getTableNames(Configuration conf) {
        final List<String> tableNames = new ArrayList<>();
        
        String tableName = conf.get(FACET_TABLE_NAME, null);
        if (null != tableName)
            tableNames.add(tableName);
        
        tableName = conf.get(FACET_METADATA_TABLE_NAME, null);
        if (null != tableName)
            tableNames.add(tableName);
        
        tableName = conf.get(FACET_HASH_TABLE_NAME, null);
        if (null != tableName)
            tableNames.add(tableName);
        
        return tableNames.toArray(new String[0]);
    }
    
    @Override
    public int[] getTableLoaderPriorities(Configuration conf) {
        int[] priorities = new int[2];
        int index = 0;
        String tableName = conf.get(FACET_TABLE_NAME, null);
        if (null != tableName)
            priorities[index++] = conf.getInt(FACET_LPRIORITY, 40);
        
        tableName = conf.get(FACET_METADATA_TABLE_NAME, null);
        if (null != tableName)
            priorities[index++] = conf.getInt(FACET_METADATA_LPRIORITY, 40);
        
        tableName = conf.get(FACET_HASH_TABLE_NAME, null);
        if (null != tableName)
            priorities[index++] = conf.getInt(FACET_HASH_TABLE_LPRIORITY, 40);
        
        if (index != priorities.length) {
            return Arrays.copyOf(priorities, index);
        } else {
            return priorities;
        }
    }
    
    @Override
    public Multimap<BulkIngestKey,Value> processBulk(KEYIN key, RawRecordContainer event, Multimap<String,NormalizedContentInterface> fields,
                    StatusReporter reporter) {
        throw new UnsupportedOperationException("processBulk is not supported, please use process");
    }
    
    @Override
    public IngestHelperInterface getHelper(Type datatype) {
        return datatype.getIngestHelper(this.taskAttemptContext.getConfiguration());
    }
    
    @Override
    public void close(TaskAttemptContext context) {
        /* no-op */
    }
    
    @Override
    public RawRecordMetadata getMetadata() {
        return null;
    }
    
    protected byte[] flatten(ColumnVisibility vis) {
        return markingFunctions == null ? vis.flatten() : markingFunctions.flatten(vis);
    }
    
    @Override
    public long process(KEYIN key, RawRecordContainer event, Multimap<String,NormalizedContentInterface> fields,
                    TaskInputOutputContext<KEYIN,? extends RawRecordContainer,KEYOUT,VALUEOUT> context, ContextWriter<KEYOUT,VALUEOUT> contextWriter)
                    throws IOException, InterruptedException {
        
        final String shardId = shardIdFactory.getShardId(event).substring(0, 8);
        Text dateColumnQualifier = new Text(shardId); // TODO: Makes an assumption about the structure of the shardId.
        
        HyperLogLogPlus cardinality = new HyperLogLogPlus(10);
        cardinality.offer(event.getId().toString());
        
        Text cv = new Text(flatten(event.getVisibility()));
        
        final HashTableFunction<KEYIN,KEYOUT,VALUEOUT> func = new HashTableFunction<>(contextWriter, context, facetHashTableName, facetHashThreshold,
                        event.getDate());
        Multimap<String,NormalizedContentInterface> eventFields = hashEventFields(fields, func);
        
        Stream<String> eventFieldKeyStream = eventFields.keySet().stream().filter(new TokenPredicate());
        if (fieldFilter != null) {
            eventFieldKeyStream = eventFieldKeyStream.filter(fieldFilter);
        }
        Set<String> keySet = eventFieldKeyStream.collect(Collectors.toSet());
        List<Set<String>> keySetList = Lists.newArrayList();
        keySetList.add(keySet);
        keySetList.add(keySet);
        
        long countWritten = 0;
        
        Value sharedValue = new Value(cardinality.getBytes());
        Multimap<BulkIngestKey,Value> results = ArrayListMultimap.create();
        
        for (String pivotFieldName : pivotMap.keySet()) {
            Text reflexiveCf = createColumnFamily(pivotFieldName, pivotFieldName);
            for (NormalizedContentInterface pivotTypes : eventFields.get(pivotFieldName)) {
                for (String facetFieldName : pivotMap.get(pivotFieldName)) {
                    if (pivotFieldName.equals(facetFieldName))
                        continue;
                    
                    Text generatedCf = createColumnFamily(pivotFieldName, facetFieldName);
                    Text myCf = generatedCf;
                    
                    if (HashTableFunction.isReduced(pivotTypes))
                        continue;
                    
                    for (NormalizedContentInterface facetTypes : eventFields.get(facetFieldName)) {
                        if (HashTableFunction.isReduced(facetTypes)) {
                            myCf.append(HashTableFunction.FIELD_APPEND_BYTES, 0, HashTableFunction.FIELD_APPEND_BYTES.length);
                        }
                        
                        Text row = createFieldValuePair(pivotTypes.getIndexedFieldValue(), facetTypes.getIndexedFieldValue(), event.getDataType());
                        Key result = new Key(row, myCf, dateColumnQualifier, cv, event.getDate());
                        results.put(new BulkIngestKey(facetTableName, result), sharedValue);
                        
                        countWritten++;
                    }
                    
                    myCf = generatedCf;
                }
                
                Text row = createFieldValuePair(pivotTypes.getIndexedFieldValue(), pivotTypes.getIndexedFieldValue(), event.getDataType());
                Key result = new Key(row, reflexiveCf, dateColumnQualifier, cv, event.getDate());
                results.put(new BulkIngestKey(facetTableName, result), sharedValue);
            }
        }
        
        for (Map.Entry<String,String> pivot : pivotMap.entries()) {
            Key result = new Key(new Text(pivot.getKey() + NULL + pivot.getValue()), PV);
            results.put(new BulkIngestKey(facetMetadataTableName, result), EMPTY_VALUE);
            countWritten++;
        }
        contextWriter.write(results, context);
        return countWritten;
    }
    
    private Multimap<String,NormalizedContentInterface> hashEventFields(Multimap<String,NormalizedContentInterface> fields,
                    HashTableFunction<KEYIN,KEYOUT,VALUEOUT> func) {
        Multimap<String,NormalizedContentInterface> eventFields = HashMultimap.create();
        for (Map.Entry<String,Collection<NormalizedContentInterface>> entry : fields.asMap().entrySet()) {
            Collection<NormalizedContentInterface> coll = func.apply(entry.getValue());
            if (coll != null && !coll.isEmpty()) {
                eventFields.putAll(entry.getKey(), coll);
            }
        }
        return eventFields;
    }
    
    /**
     * Create the column qualifier that includes pivotFieldValue, facetFieldValue and datatype.
     * 
     * @param pivotFieldValue
     * @param facetFieldValue
     * @param dataType
     * @return
     */
    protected Text createFieldValuePair(String pivotFieldValue, String facetFieldValue, Type dataType) {
        return new Text(pivotFieldValue + NULL + facetFieldValue + NULL + dataType.typeName());
    }
    
    /**
     * Create the column family consisting of pivotFieldName and facetFieldName
     * 
     * @param pivotFieldName
     * @param facetFieldName
     * @return
     */
    protected Text createColumnFamily(String pivotFieldName, String facetFieldName) {
        return new Text(pivotFieldName + NULL + facetFieldName);
    }
    
    @Override
    public FacetValue estimate(RawRecordContainer input) {
        // precision value: 10, sparse set disabled.
        HyperLogLogPlus card = new HyperLogLogPlus(10);
        card.offer(input.getId().toString());
        
        return new FacetValue(card, new CountMinSketch(10, 1, 1));
    }
    
    /** A predicate used to ignore values that are generated via tokenization */
    // TODO: make configurable
    public static class TokenPredicate implements Predicate<String> {
        @Override
        public boolean test(String input) {
            return !input.endsWith("_TOKEN");
        }
    }
    
    /**
     * Extract the cardinality from a Value object.
     *
     * @param v
     *            the value containing the encoded cardinality.
     *
     * @return The cardinality extracted, otherwise -1L if no cardinality is present -2L if there's en error extracting
     */
    public static final long extractCardinality(Value v) {
        if (v == null) {
            return -1L;
        } else if (v.getSize() > 0) {
            try {
                ICardinality cc = HyperLogLogPlus.Builder.build(v.get());
                return cc.cardinality();
            } catch (IOException e) {
                log.debug("IOException extracting cardinality from value", e);
                return -2L;
            }
        } else {
            return -1L;
        }
    }
}
