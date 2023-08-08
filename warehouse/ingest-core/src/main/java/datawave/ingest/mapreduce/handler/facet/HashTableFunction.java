package datawave.ingest.mapreduce.handler.facet;

import java.io.IOException;
import java.util.Collection;

import javax.annotation.Nullable;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import com.google.common.base.Function;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.NormalizedFieldAndValue;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.mapreduce.job.writer.ContextWriter;

/** Generate hashes for large collections of fields */
@SuppressWarnings("UnstableApiUsage")
// Guava HashFunction, Hasher, etc. TODO: migrate to Java 8 functional API
public class HashTableFunction<KEYIN,KEYOUT,VALUEOUT> implements Function<Collection<NormalizedContentInterface>,Collection<NormalizedContentInterface>> {

    public static final String FIELD_APPEND = ".hash";
    public static final byte[] FIELD_APPEND_BYTES = FIELD_APPEND.getBytes();
    private static final byte[] EMPTY_BYTES = new byte[] {};
    private static final Value EMPTY_VALUE = new Value(EMPTY_BYTES);

    private final long maxValues;
    private final long timestamp;

    private final ContextWriter<KEYOUT,VALUEOUT> contextWriter;
    private final Text outputTable;
    private final TaskInputOutputContext<KEYIN,? extends RawRecordContainer,KEYOUT,VALUEOUT> context;

    protected final HashFunction hashingFunction = Hashing.sha1();
    protected Hasher hasher;

    /**
     *
     * @param contextWriter
     *            used for writing the BulkIngest keys we will output
     * @param context
     *            output context used when writing.
     * @param outputTable
     *            the table our hashes will be written to
     * @param maxValues
     *            the threshold for generation of hashed values.
     * @param timestamp
     *            the timestamp for the entry
     */
    public HashTableFunction(ContextWriter<KEYOUT,VALUEOUT> contextWriter, TaskInputOutputContext<KEYIN,? extends RawRecordContainer,KEYOUT,VALUEOUT> context,
                    Text outputTable, long maxValues, long timestamp) {
        this.maxValues = maxValues;
        this.contextWriter = contextWriter;
        this.context = context;
        this.outputTable = outputTable;
        this.timestamp = timestamp;
    }

    /**
     * If the number of field/value pairs in the input is larger than a threshold, generate a hash for this collection and map individual field values to that
     * hash. These are emitted to the specified table.
     *
     * @param input
     *            the set of field value pairs the hash is based upon.
     * @return if the input exceeds maxValues in size, return the hash of the collection in a single field based on the name of the first element provided with
     *         FIELD_APPEND appended. Otherwise, returns the list of input fields.
     */
    @Nullable
    @Override
    public Collection<NormalizedContentInterface> apply(@Nullable Collection<NormalizedContentInterface> input) {
        if (input == null || input.isEmpty())
            return input;

        if (input.size() > maxValues) {
            hasher = hashingFunction.newHasher(input.size());
            byte[] hashBytes = new byte[hashingFunction.bits() / 8];

            Multimap<BulkIngestKey,Value> map = ArrayListMultimap.create(input.size(), 1);
            NormalizedContentInterface firstElement = Iterables.getFirst(input, null);

            if (firstElement == null) {
                return input;
            }

            for (NormalizedContentInterface nci : input) {
                hasher.putUnencodedChars(nci.getIndexedFieldValue());
                byte[] indexedValue = nci.getIndexedFieldValue().getBytes();
                // key maintains a reference to hashBytes, so it is properly updated by the time it is written
                map.put(new BulkIngestKey(outputTable, new Key(hashBytes, indexedValue, EMPTY_BYTES, EMPTY_BYTES, timestamp, false, false)), EMPTY_VALUE);
            }

            final String hash = hasher.hash().toString();
            final byte[] hashStringBytes = hash.getBytes();
            System.arraycopy(hashStringBytes, 0, hashBytes, 0, hashBytes.length);

            try {
                contextWriter.write(map, context);
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }

            final String newFieldName = (firstElement.getIndexedFieldName() + FIELD_APPEND).intern();

            NormalizedFieldAndValue generated = new NormalizedFieldAndValue(firstElement);
            generated.setFieldName(newFieldName);
            generated.setIndexedFieldName(newFieldName);
            generated.setIndexedFieldValue(hash);

            return Lists.newArrayList(generated);
        }

        return input;
    }

    /**
     * Determine if the indexedFieldName ends with the suffix defined in FIELD_APPEND_BYTES
     *
     * @param pivotTypes
     *            the field to inspect
     * @return true if the field name ends with the FIELD_APPEND_BYTES suffix.
     */
    public static boolean isReduced(NormalizedContentInterface pivotTypes) {
        final byte[] fieldNameBytes = pivotTypes.getIndexedFieldName().getBytes();
        if (fieldNameBytes.length > FIELD_APPEND_BYTES.length) {
            // @formatter:off
            final int cmp = WritableComparator.compareBytes(
                    fieldNameBytes,
                    fieldNameBytes.length - FIELD_APPEND.length(), // start
                    fieldNameBytes.length - (fieldNameBytes.length - FIELD_APPEND.length()), // length
                    FIELD_APPEND_BYTES,
                    0, // start
                    FIELD_APPEND_BYTES.length // length
            );
            // @formatter:on

            return cmp == 0;
        }
        return false;
    }
}
