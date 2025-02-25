package datawave.ingest.mapreduce.handler.facet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.log4j.Logger;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.clearspring.analytics.stream.cardinality.ICardinality;
import com.clearspring.analytics.stream.frequency.CountMinSketch;
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
import datawave.util.time.DateHelper;

public class FacetHandler<KEYIN,KEYOUT,VALUEOUT> implements ExtendedDataTypeHandler<KEYIN,KEYOUT,VALUEOUT>, FacetedEstimator<RawRecordContainer> {

    private static final Logger log = Logger.getLogger(FacetHandler.class);

    /* Global configuration properties */

    public static final String FACET_TABLE_NAME = "facet.table.name";
    public static final String FACET_TABLE_LOADER_PRIORITY = "facet.table.loader.priority";

    public static final String FACET_METADATA_TABLE_NAME = "facet.metadata.table.name";
    public static final String FACET_METADATA_TABLE_LOADER_PRIORITY = "facet.metadata.table.loader.priority";

    public static final String FACET_HASH_TABLE_NAME = "facet.hash.table.name";
    public static final String FACET_HASH_TABLE_LOADER_PRIORITY = "facet.hash.table.loader.priority";

    /* Per-datatype configuration properties */

    public static final String FACET_HASH_THRESHOLD = ".facet.hash.threshold";

    public static final String FACET_CATEGORY_DELIMITER = ".facet.category.delimiter";
    public static final String FACET_FIELD_PREDICATE_CLASS = ".facet.field.predicate.class";

    public static final String FACET_CATEGORY_PREFIX_REGEX = "\\.facet\\.category\\.name\\..*";

    public static final String DEFAULT_FACET_CATEGORY_DELIMITER = ";";

    protected static final Text PV = new Text("pv");
    protected static final String NULL = "\0";
    protected static final Value EMPTY_VALUE = new Value(new byte[] {});

    /* Global configuration fields */

    protected Text facetTableName;
    protected Text facetMetadataTableName;
    protected Text facetHashTableName;

    /* Per-datatype configuration fields */

    protected int facetHashThreshold;
    protected String categoryDelimiter = DEFAULT_FACET_CATEGORY_DELIMITER;

    /* Instance variables */

    protected MarkingFunctions markingFunctions;
    protected ShardIdFactory shardIdFactory;
    protected TaskAttemptContext taskAttemptContext;

    protected Predicate<String> fieldSelectionPredicate = new TokenPredicate();
    protected Predicate<String> fieldFilterPredicate = null;
    protected Multimap<String,String> pivotMap;

    public void setFieldSelectionPredicate(Predicate<String> predicate) {
        this.fieldSelectionPredicate = predicate;
    }

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
                fieldFilterPredicate = projClazz.newInstance();
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
        return getNonNullTableNames(conf, FACET_TABLE_NAME, FACET_METADATA_TABLE_NAME, FACET_HASH_TABLE_NAME);
    }

    @Override
    public int[] getTableLoaderPriorities(Configuration conf) {
        // @formatter:off
        return getNonNullTableLoaderPriorities(
                conf, 40,
                FACET_TABLE_NAME, FACET_TABLE_LOADER_PRIORITY,
                FACET_METADATA_TABLE_NAME, FACET_METADATA_TABLE_LOADER_PRIORITY,
                FACET_HASH_TABLE_NAME, FACET_HASH_TABLE_LOADER_PRIORITY);
        // @formatter:on
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

        final String shardId = shardIdFactory.getShardId(event);
        final String shardDateString = ShardIdFactory.getDateString(shardId);
        final Text dateColumnQualifier = new Text(shardDateString);
        final Date shardDate = DateHelper.parse(shardDateString);
        final long timestamp = shardDate.getTime();

        Text cv = new Text(flatten(event.getVisibility()));

        // filter out event fields that are generated as the result of tokenization.
        Stream<String> fieldKeyStream = fields.keySet().stream().filter(fieldSelectionPredicate);
        if (fieldFilterPredicate != null) {
            fieldKeyStream = fieldKeyStream.filter(fieldFilterPredicate);
        }
        final Set<String> filteredFieldSet = fieldKeyStream.collect(Collectors.toSet());
        Set<String> pivotFieldSet = new HashSet<>(filteredFieldSet);
        Set<String> facetFieldSet = new HashSet<>(filteredFieldSet);

        // fields with a large number of values are hashed. See HashTableFunction for details
        // @formatter:off
        final HashTableFunction<KEYIN,KEYOUT,VALUEOUT> func = new HashTableFunction<>(
                contextWriter, context, facetHashTableName, facetHashThreshold, timestamp);
        final Multimap<String,NormalizedContentInterface> eventFields = filterAndHashEventFields(fields, filteredFieldSet, func);
        // @formatter:on

        long countWritten = 0;

        // the event id offered to the cardinality is a uid based on the 'EVENT_ID',
        // so it's helpful to have that around for tracing when logging about the
        // facet keys that are created.
        String eventId = null;
        if (log.isTraceEnabled()) {
            StringBuilder b = new StringBuilder();
            for (NormalizedContentInterface f : eventFields.get("EVENT_ID")) {
                b.append(f.getEventFieldValue());
            }
            eventId = b.toString();
        }

        // compute the cardinality based on the uid, this becomes the value shared
        // across each facet row generated.
        final HyperLogLogPlus cardinality = new HyperLogLogPlus(10);
        final String id = shardId + "/" + event.getDataType().typeName() + "/" + event.getId().toString();
        cardinality.offer(id);
        final Value sharedValue = new Value(cardinality.getBytes());

        final Multimap<BulkIngestKey,Value> results = ArrayListMultimap.create();

        for (String pivotFieldName : pivotMap.keySet()) {
            if (!pivotFieldSet.contains(pivotFieldName))
                continue;

            final Text reflexiveCf = createColumnFamily(pivotFieldName, pivotFieldName);

            for (NormalizedContentInterface pivotTypes : eventFields.get(pivotFieldName)) {
                if (HashTableFunction.isReduced(pivotTypes))
                    continue;

                // Generate the pivot entry.
                // @formatter:off
                final BulkIngestKey pivotIngestKey = generateFacetIngestKey(
                        pivotTypes.getIndexedFieldValue(),
                        pivotTypes.getIndexedFieldValue(),
                        event.getDataType(),
                        reflexiveCf,
                        dateColumnQualifier,
                        cv,
                        timestamp);

                results.put(pivotIngestKey, sharedValue);

                if (log.isTraceEnabled()) {
                    log.trace("created BulkIngestKey (pivot): " + pivotIngestKey.getKey() +
                            " for " + event.getId().toString() +
                            " in " + event.getRawFileName() +
                            " event " + eventId);
                }
                // @formatter:on

                // Generate the facet entries.
                for (String facetFieldName : pivotMap.get(pivotFieldName)) {
                    if (pivotFieldName.equals(facetFieldName))
                        continue;
                    if (!facetFieldSet.contains(facetFieldName))
                        continue;

                    final Text generatedCf = createColumnFamily(pivotFieldName, facetFieldName);

                    for (NormalizedContentInterface facetTypes : eventFields.get(facetFieldName)) {
                        Text facetCf = new Text(generatedCf);

                        if (HashTableFunction.isReduced(facetTypes)) {
                            facetCf.append(HashTableFunction.FIELD_APPEND_BYTES, 0, HashTableFunction.FIELD_APPEND_BYTES.length);
                        }

                        // @formatter:off
                        final BulkIngestKey facetIngestKey = generateFacetIngestKey(
                                pivotTypes.getIndexedFieldValue(),
                                facetTypes.getIndexedFieldValue(),
                                event.getDataType(),
                                facetCf,
                                dateColumnQualifier,
                                cv,
                                timestamp);

                        results.put(facetIngestKey, sharedValue);

                        if (log.isDebugEnabled()) {
                            log.debug("created BulkIngestKey (facet): " + facetIngestKey.getKey() +
                                    " for " + event.getId().toString() +
                                    " in " + event.getRawFileName() +
                                    " event " + eventId);
                        }
                        // @formatter:on

                        countWritten++;
                    }
                }
            }
        }

        // metadata for each pivot field - maps to itself
        for (String pivot : pivotMap.keySet()) {
            if (!pivotFieldSet.contains(pivot))
                continue;
            results.put(generateFacetMetadataIngestKey(pivot, pivot, timestamp), EMPTY_VALUE);
            countWritten++;
        }

        // metadata mapping each pivot field to their facet fields.
        for (Map.Entry<String,String> facet : pivotMap.entries()) {
            if (!pivotFieldSet.contains(facet.getKey()) || !facetFieldSet.contains(facet.getValue()))
                continue;
            results.put(generateFacetMetadataIngestKey(facet.getKey(), facet.getValue(), timestamp), EMPTY_VALUE);
            countWritten++;
        }

        contextWriter.write(results, context);
        return countWritten;
    }

    /**
     * Filter the source data and apply the supplied HashTableFunction to the fields provided. The results are collected and returned. This is commonly used
     * where there are a large number of values for a field.
     *
     * @param fields
     *            The source data to process.
     * @param filteredFieldSet
     *            The names of fields to keep.
     * @param func
     *            The function to apply to hash fields if necessary.
     * @return The modified set of fields after hashing.
     */
    private Multimap<String,NormalizedContentInterface> filterAndHashEventFields(Multimap<String,NormalizedContentInterface> fields,
                    Set<String> filteredFieldSet, HashTableFunction<KEYIN,KEYOUT,VALUEOUT> func) {
        Multimap<String,NormalizedContentInterface> eventFields = HashMultimap.create();
        for (Map.Entry<String,Collection<NormalizedContentInterface>> entry : fields.asMap().entrySet()) {
            if (filteredFieldSet.contains(entry.getKey())) {
                Collection<NormalizedContentInterface> coll = func.apply(entry.getValue());
                if (coll != null && !coll.isEmpty()) {
                    eventFields.putAll(entry.getKey(), coll);
                }
            }
        }
        return eventFields;
    }

    /**
     * Generate the entry for the facet table in the form of a BulkIngestKey
     *
     * @param pivotFieldValue
     *            the value of the pivot field - actual value depends on facet type
     * @param facetFieldValue
     *            the value of the facet field - actual value depends on facet type
     * @param dataType
     *            the datatype for the data from which the facet was generated
     * @param cf
     *            the column family for the facet. This encodes the field names the facet originated from, possibly modified if we need to hash the field
     *            values.
     * @param dateCq
     *            the date for the column qualifier.
     * @param cv
     *            the column visibility for this facet entry.
     * @param ts
     *            the timestamp use for the facet key
     * @return A bulk ingest key for this facet entry.
     */
    public BulkIngestKey generateFacetIngestKey(String pivotFieldValue, String facetFieldValue, Type dataType, Text cf, Text dateCq, Text cv, long ts) {
        final Text facetRow = createFieldValuePair(pivotFieldValue, facetFieldValue, dataType);
        final Key facetResult = new Key(facetRow, cf, dateCq, cv, ts);
        return new BulkIngestKey(facetTableName, facetResult);
    }

    public BulkIngestKey generateFacetMetadataIngestKey(String from, String to, long ts) {
        final Key facetMetadataResult = new Key(new Text(from + NULL + to), PV, new Text(""), ts);
        return new BulkIngestKey(facetMetadataTableName, facetMetadataResult);
    }

    /**
     * Create the column qualifier that includes pivotFieldValue, facetFieldValue and datatype.
     *
     * @param pivotFieldValue
     *            the value of the pivot field
     * @param facetFieldValue
     *            the value of the facet field
     * @param dataType
     *            the data type of the pivot/facet pair
     * @return a properly formed column-qualifier containing pivot and facet values + datatype.
     */
    protected Text createFieldValuePair(String pivotFieldValue, String facetFieldValue, Type dataType) {
        return new Text(pivotFieldValue + NULL + facetFieldValue + NULL + dataType.typeName());
    }

    /**
     * Create the column family consisting of pivotFieldName and facetFieldName.
     *
     * @param pivotFieldName
     *            the name of the pivot field
     * @param facetFieldName
     *            the name of the facet field
     * @return a properly formed column-family containing the pivot and facet field names.
     */
    protected Text createColumnFamily(String pivotFieldName, String facetFieldName) {
        return new Text(pivotFieldName + NULL + facetFieldName);
    }

    /**
     * Return an array of table loader priorities extracted from the specified configuration based on the property names supplied, where those properties are
     * present in the configuration.
     *
     * @param conf
     *            the configuration to extract the priority values from
     * @param defaultPriority
     *            the defailt priority to use if none is specified.
     * @param properties
     *            the properties to read from the configuration, must be an even number organized based on table name property name, table priority property
     *            name.
     * @return an array of the table priorities loaded from the configuration the length of this will be based on the number of the specified table name
     *         properties present in the configuration.
     */
    protected int[] getNonNullTableLoaderPriorities(Configuration conf, int defaultPriority, String... properties) {
        if (properties.length % 2 != 0) {
            throw new IllegalArgumentException("Received an odd number of properties, expected an even number, "
                            + "each table name property should be followed by the table priority property");
        }

        int[] priorities = new int[properties.length / 2];
        int index = 0;

        for (int i = 0; i < properties.length; i += 2) {
            final String tableNameProp = properties[i];
            final String tablePriorityProp = properties[i + 1];
            final String tableName = conf.get(tableNameProp, null);
            if (null != tableName)
                priorities[index++] = conf.getInt(tablePriorityProp, defaultPriority);
        }

        if (index != priorities.length) {
            return Arrays.copyOf(priorities, index);
        } else {
            return priorities;
        }
    }

    /**
     * Return an array of table names extracted from the specified configuration based on the property names supplied, where those properties are present in the
     * configuration.
     *
     * @param conf
     *            the configuration to extract the table names from.
     * @param properties
     *            the properties to read from the configuration in order to obtain table names
     * @return an array of the table priorities loaded from the configuration. The length of this will be dependent on the number of specified properties
     *         present in the configuration.
     */
    protected String[] getNonNullTableNames(Configuration conf, String... properties) {
        final List<String> tableNames = new ArrayList<>();

        for (final String p : properties) {
            final String tableName = conf.get(p, null);
            if (null != tableName)
                tableNames.add(tableName);
        }

        return tableNames.toArray(new String[0]);
    }

    @Override
    public FacetValue estimate(RawRecordContainer input) {
        // precision value: 10, sparse set disabled.
        final HyperLogLogPlus card = new HyperLogLogPlus(10);
        final String id = shardIdFactory.getShardId(input) + "/" + input.getDataType() + "/" + input.getId().toString();
        card.offer(id);

        return new FacetValue(card, new CountMinSketch(10, 1, 1));
    }

    /** A predicate used to ignore values that are generated via tokenization */
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
    public static long extractCardinality(Value v) {
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
