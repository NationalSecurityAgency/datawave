package datawave.ingest.mapreduce.handler.ssdeep;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.Type;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.ConfigurationHelper;
import datawave.ingest.data.config.DataTypeHelper;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.ingest.IngestHelperInterface;
import datawave.ingest.mapreduce.handler.ExtendedDataTypeHandler;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.mapreduce.job.writer.ContextWriter;
import datawave.ingest.metadata.RawRecordMetadata;
import datawave.marking.MarkingFunctions;
import datawave.util.ssdeep.BucketAccumuloKeyGenerator;
import datawave.util.ssdeep.NGramByteHashGenerator;
import datawave.util.ssdeep.NGramTuple;

public class SSDeepIndexHandler<KEYIN,KEYOUT,VALUEOUT> implements ExtendedDataTypeHandler<KEYIN,KEYOUT,VALUEOUT> {

    /* Global configuration properties */

    public static final String SSDEEP_INDEX_TABLE_NAME = "ssdeepIndex.table.name";
    public static final String SSDEEP_INDEX_TABLE_LOADER_PRIORITY = "ssdeepIndex.table.loader.priority";
    public static final String SSDEEP_FIELD_SET = ".ssdeepIndex.fields";

    public static final String SSDEEP_BUCKET_COUNT = ".ssdeepIndex.bucket.count";

    public static final int DEFAULT_SSDEEP_BUCKET_COUNT = BucketAccumuloKeyGenerator.DEFAULT_BUCKET_COUNT;

    public static final String SSDEEP_BUCKET_ENCODING_BASE = ".ssdeepIndex.bucket.encoding.base";

    public static final int DEFAULT_SSDEEP_BUCKET_ENCODING_BASE = BucketAccumuloKeyGenerator.DEFAULT_BUCKET_ENCODING_BASE;

    public static final String SSDEEP_BUCKET_ENCODING_LENGTH = ".ssdeepIndex.bucket.encoding.length";

    public static final int DEFAULT_SSDEEP_BUCKET_ENCODING_LENGTH = BucketAccumuloKeyGenerator.DEFAULT_BUCKET_ENCODING_LENGTH;

    public static final String SSDEEP_INDEX_NGRAM_SIZE = ".ssdeepIndex.ngram.size.min";

    public static final int DEFAULT_SSDEEP_INDEX_NGRAM_SIZE = 7;

    public static final String SSDEEP_MIN_HASH_SIZE = ".ssdeepIndex.chunk.size.min";

    public static final int DEFAULT_SSDEEP_MIN_HASH_SIZE = 3;

    protected Text ssdeepIndexTableName;

    protected MarkingFunctions markingFunctions;

    protected TaskAttemptContext taskAttemptContext;

    protected Set<String> ssdeepFieldNames;

    protected NGramByteHashGenerator nGramGenerator;
    protected BucketAccumuloKeyGenerator accumuloKeyGenerator;

    @Override
    public void setup(TaskAttemptContext context) {
        markingFunctions = MarkingFunctions.Factory.createMarkingFunctions();
        taskAttemptContext = context;

        Configuration conf = context.getConfiguration();

        final String t = ConfigurationHelper.isNull(conf, DataTypeHelper.Properties.DATA_NAME, String.class);
        TypeRegistry.getInstance(conf);
        Type type = TypeRegistry.getType(t);

        int bucketCount = conf.getInt(type.typeName() + SSDEEP_BUCKET_COUNT, DEFAULT_SSDEEP_BUCKET_COUNT);
        int bucketEncodingBase = conf.getInt(type.typeName() + SSDEEP_BUCKET_ENCODING_BASE, DEFAULT_SSDEEP_BUCKET_ENCODING_BASE);
        int bucketEncodingLength = conf.getInt(type.typeName() + SSDEEP_BUCKET_ENCODING_LENGTH, DEFAULT_SSDEEP_BUCKET_ENCODING_LENGTH);
        int ngramSize = conf.getInt(type.typeName() + SSDEEP_INDEX_NGRAM_SIZE, DEFAULT_SSDEEP_INDEX_NGRAM_SIZE);
        int minHashSize = conf.getInt(type.typeName() + SSDEEP_MIN_HASH_SIZE, DEFAULT_SSDEEP_MIN_HASH_SIZE);

        accumuloKeyGenerator = new BucketAccumuloKeyGenerator(bucketCount, bucketEncodingBase, bucketEncodingLength);
        nGramGenerator = new NGramByteHashGenerator(ngramSize, bucketCount, minHashSize);

        String[] fieldNameArray = conf.getStrings(type.typeName() + SSDEEP_FIELD_SET);
        ssdeepFieldNames = new HashSet<>(List.of(fieldNameArray));
        ssdeepIndexTableName = new Text(ConfigurationHelper.isNull(conf, SSDEEP_INDEX_TABLE_NAME, String.class));
    }

    @Override
    public String[] getTableNames(Configuration conf) {
        return getNonNullTableNames(conf, SSDEEP_INDEX_TABLE_NAME);
    }

    @Override
    public int[] getTableLoaderPriorities(Configuration conf) {
        // @formatter:off
        return getNonNullTableLoaderPriorities(
                conf, 40,
                SSDEEP_INDEX_TABLE_NAME, SSDEEP_INDEX_TABLE_LOADER_PRIORITY);
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

        final Set<String> eventFieldSet = fields.keySet();
        long countWritten = 0;
        final Multimap<BulkIngestKey,Value> results = ArrayListMultimap.create();

        for (String ssdeepFieldName : ssdeepFieldNames) {
            if (!eventFieldSet.contains(ssdeepFieldName))
                continue;

            for (NormalizedContentInterface ssdeepTypes : fields.get(ssdeepFieldName)) {
                countWritten += generateSSDeepIndexEntries(ssdeepTypes.getEventFieldValue(), results);
            }
        }

        contextWriter.write(results, context);
        return countWritten;
    }

    public long generateSSDeepIndexEntries(String fieldValue, Multimap<BulkIngestKey,Value> results) {
        long countWritten = 0L;
        Iterator<ImmutablePair<NGramTuple,byte[]>> it = nGramGenerator.call(fieldValue);
        while (it.hasNext()) {
            ImmutablePair<NGramTuple,byte[]> nt = it.next();
            ImmutablePair<Key,Value> at = accumuloKeyGenerator.call(nt);
            BulkIngestKey indexKey = new BulkIngestKey(ssdeepIndexTableName, at.getKey());
            results.put(indexKey, at.getValue());
            countWritten++;
        }
        return countWritten;
    }

    /**
     * Return an array of table loader priorities extracted from the specified configuration based on the property names supplied, where those properties are
     * present in the configuration.
     *
     * @param conf
     *            the configuration to extract the priority values from
     * @param defaultPriority
     *            the default priority to use if none is specified.
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
}
