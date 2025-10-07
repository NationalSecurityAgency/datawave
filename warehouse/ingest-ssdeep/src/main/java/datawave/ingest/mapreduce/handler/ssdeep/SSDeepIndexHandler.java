package datawave.ingest.mapreduce.handler.ssdeep;

import java.io.IOException;
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
import datawave.util.ssdeep.NGramGenerator;
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

    public static final int DEFAULT_SSDEEP_INDEX_NGRAM_SIZE = NGramGenerator.DEFAULT_NGRAM_SIZE;

    public static final String SSDEEP_MIN_HASH_SIZE = ".ssdeepIndex.chunk.size.min";

    public static final int DEFAULT_SSDEEP_MIN_HASH_SIZE = NGramGenerator.DEFAULT_MIN_HASH_SIZE;

    /** The priority of 40 is arbitrary, based on setting other priorities - this controls the bulk loading process */
    public static final int DEFAULT_SSDEEP_INDEX_TABLE_LOADER_PRIORITY = 40;
    public static final String DEFAULT_SSDEEP_INDEX_TABLE_NAME = "ssdeepIndex";

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

        final String dataType = ConfigurationHelper.isNull(conf, DataTypeHelper.Properties.DATA_NAME, String.class);
        TypeRegistry.getInstance(conf);
        Type type = TypeRegistry.getType(dataType);

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
        return new String[] {conf.get(SSDEEP_INDEX_TABLE_NAME, DEFAULT_SSDEEP_INDEX_TABLE_NAME)};
    }

    @Override
    public int[] getTableLoaderPriorities(Configuration conf) {
        return new int[] {conf.getInt(SSDEEP_INDEX_TABLE_LOADER_PRIORITY, DEFAULT_SSDEEP_INDEX_TABLE_LOADER_PRIORITY)};
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
}
