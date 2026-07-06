package datawave.test.framework.writers;

import static datawave.test.framework.util.MetadataColumn.I;
import static datawave.test.framework.writers.TableWriter.TIMESTAMP;

import java.util.List;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;

import datawave.data.type.Type;
import datawave.ingest.protobuf.Uid;
import datawave.table.constants.TableName;
import datawave.test.framework.FieldMetadata;
import datawave.test.framework.util.ShardKeyUtil;
import datawave.test.framework.util.UidGenerator;

public class ShardIndexTableWriter {

    private ShardIndexTableWriter() {
        // enforce static access
    }

    // index keys are
    // value FIELD : datatype<null>shard viz
    public static void write(AccumuloClient client, List<FieldMetadata> fields, int numShards) {
        try (BatchWriter bw = client.createBatchWriter(TableName.SHARD_INDEX)) {

            for (FieldMetadata field : fields) {
                if (!field.getMetadataColumns().contains(I)) {
                    continue;
                }

                for (String value : field.getValues()) {
                    // a value can have no backing events (e.g. valuesPerField exceeds the number of events the field appears in); skip it rather than
                    // writing a mutation with no puts, which Accumulo rejects
                    List<Integer> eventIds = field.getEventIdsForValue(value);
                    if (eventIds.isEmpty()) {
                        continue;
                    }

                    for (Type<?> normalizer : field.getNormalizers()) {
                        String normalizedValue = normalizer.normalize(value);
                        for (String datatype : field.getDatatypes()) {
                            writeIndexMutation(bw, normalizedValue, field.getFieldName(), datatype, eventIds, numShards);
                        }

                        if (field.isContentField()) {
                            // index each token individually so content:phrase's FIELD == 'word' index-expansion can resolve
                            for (String token : value.split(" ")) {
                                String normalizedToken = normalizer.normalize(token);
                                for (String datatype : field.getDatatypes()) {
                                    writeIndexMutation(bw, normalizedToken, field.getFieldName(), datatype, eventIds, numShards);
                                }
                            }
                        }
                    }
                }
            }
        } catch (TableNotFoundException | MutationsRejectedException e) {
            throw new RuntimeException(e);
        }
    }

    private static void writeIndexMutation(BatchWriter bw, String indexedValue, String fieldName, String datatype, List<Integer> eventIds, int numShards)
                    throws MutationsRejectedException {
        Mutation m = new Mutation(indexedValue);
        for (int eventId : eventIds) {
            String shardRow = ShardKeyUtil.buildRow(eventId, numShards);
            Key key = new Key(indexedValue, fieldName, shardRow + "\u0000" + datatype, "ALL", TIMESTAMP);

            String uid = UidGenerator.uid(String.valueOf(eventId));
            Value tv = createValue(uid);
            m.put(key.getColumnFamily(), key.getColumnQualifier(), key.getColumnVisibilityParsed(), key.getTimestamp(), tv);
        }
        bw.addMutation(m);
    }

    /**
     * Create a shard index {@link Value} containing a {@link Uid.List}
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
