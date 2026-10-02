package datawave.test.framework.writers;

import static datawave.test.framework.util.MetadataColumn.I;
import static datawave.test.framework.writers.TableWriter.TIMESTAMP;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang3.StringUtils;

import datawave.data.type.Type;
import datawave.ingest.protobuf.Uid;
import datawave.table.constants.TableName;
import datawave.test.framework.FieldMetadata;
import datawave.test.framework.util.MetadataColumn;
import datawave.test.framework.util.ShardKeyUtil;
import datawave.test.framework.util.UidGenerator;

public class ShardIndexTableWriter {

    private static final ColumnVisibility VISIBILITY = new ColumnVisibility(TableWriter.DEFAULT_VISIBILITY);

    private ShardIndexTableWriter() {
        // enforce static access
    }

    // index keys are
    // value FIELD : datatype<null>shard viz
    public static void write(AccumuloClient client, List<FieldMetadata> fields, int numShards) {
        writeIndex(client, fields, numShards, TableName.SHARD_INDEX, I, false);
    }

    static void writeIndex(AccumuloClient client, List<FieldMetadata> fields, int numShards, String tableName, MetadataColumn indexColumn, boolean reverse) {
        try (BatchWriter bw = client.createBatchWriter(tableName)) {

            for (FieldMetadata field : fields) {
                if (!field.getMetadataColumns().contains(indexColumn)) {
                    continue;
                }

                for (String value : field.getValues()) {
                    // a value can have no backing events (e.g. valuesPerField exceeds the number of events the field appears in); skip it rather than
                    // writing a mutation with no puts, which Accumulo rejects
                    List<Integer> eventIds = field.getEventIdsForValue(value);
                    if (eventIds.isEmpty()) {
                        continue;
                    }

                    Set<String> indexedValues = new LinkedHashSet<>();
                    for (Type<?> normalizer : field.getNormalizers()) {
                        // As in ShardTableWriter, a content field indexes its whole phrase alongside its tokens under one field name, matching real
                        // ingest configured without a token field name designator.
                        indexedValues.add(normalizer.normalize(value));

                        if (field.isContentField()) {
                            // index each token individually so content:phrase's FIELD == 'word' index-expansion can resolve
                            for (String token : value.split(" ")) {
                                indexedValues.add(normalizer.normalize(token));
                            }
                        }
                    }
                    // The uid aggregator sums counts, so repeated tokens or overlapping normalizers must not write the same event twice.
                    for (String indexedValue : indexedValues) {
                        String row = reverse ? StringUtils.reverse(indexedValue) : indexedValue;
                        for (String datatype : field.getDatatypes()) {
                            writeIndexMutation(bw, row, field.getFieldName(), datatype, eventIds, numShards);
                        }
                    }
                }
            }
        } catch (TableNotFoundException | MutationsRejectedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Write one global index entry per event carrying the value.
     * <p>
     * Every event that shares a value within one shard produces the <b>same</b> key: row, column family, column qualifier, visibility and
     * {@link TableWriter#TIMESTAMP} are all identical, and only the {@link Uid.List} value differs. Those entries survive as a single multi-uid list purely
     * because {@link datawave.test.framework.TableCreator#createShardIndex} installs {@code GlobalIndexUidAggregator} beneath the versioning iterator. Remove
     * or reprioritise that aggregator and the versioning iterator keeps one arbitrary version, leaving one uid per shard - which surfaces as queries quietly
     * returning too few results rather than as a failure here.
     *
     * @param bw
     *            the batch writer
     * @param indexedValue
     *            the normalized value, used as the row
     * @param fieldName
     *            the field name, used as the column family
     * @param datatype
     *            the datatype
     * @param eventIds
     *            the events carrying this value
     * @param numShards
     *            the number of shards
     * @throws MutationsRejectedException
     *             if a mutation is rejected
     */
    private static void writeIndexMutation(BatchWriter bw, String indexedValue, String fieldName, String datatype, List<Integer> eventIds, int numShards)
                    throws MutationsRejectedException {
        for (int eventId : eventIds) {
            String shardRow = ShardKeyUtil.buildRow(eventId, numShards);
            String cq = shardRow + "\u0000" + datatype;

            String uid = UidGenerator.uid(String.valueOf(eventId));
            Value tv = createValue(uid);

            // One mutation per event, deliberately. Every event sharing a value within a shard produces an identical key, and an in-memory Accumulo
            // instance keys its memtable by (key, mutation count) with that count incremented once per mutation - so colliding entries batched into a
            // single mutation overwrite one another before the aggregator ever sees them, silently leaving one uid per shard.
            Mutation m = new Mutation(indexedValue);
            m.put(fieldName, cq, VISIBILITY, TIMESTAMP, tv);
            bw.addMutation(m);
        }
    }

    /**
     * Create a shard index {@link Value} containing a {@link Uid.List} of exactly one uid.
     * <p>
     * A count of one is correct per entry: the aggregator described on {@link #writeIndexMutation} sums the colliding entries into the real per-shard count.
     *
     * @param uid
     *            a {@link datawave.table.hash.HashUID}
     * @return a Value containing a protobuf uid list
     */
    private static Value createValue(String uid) {
        //  @formatter:off
        Uid.List uids = Uid.List.newBuilder()
                .setIGNORE(false)
                .setCOUNT(1L)
                .addUID(uid)
                .build();
        //  @formatter:on
        return new Value(uids.toByteArray());
    }
}
