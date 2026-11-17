package datawave.test.framework.writers;

import static datawave.test.framework.writers.TableWriter.TIMESTAMP;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;

import datawave.data.type.Type;
import datawave.ingest.protobuf.TermWeight;
import datawave.table.constants.TableName;
import datawave.test.framework.FieldMetadata;
import datawave.test.framework.util.MetadataColumn;
import datawave.test.framework.util.ShardKeyUtil;
import datawave.test.framework.util.UidGenerator;

public class ShardTableWriter {

    private static final Value EMPTY_VALUE = new Value();
    private static final ColumnVisibility VISIBILITY = new ColumnVisibility(TableWriter.DEFAULT_VISIBILITY);

    private ShardTableWriter() {
        // enforce static access
    }

    public static void write(AccumuloClient client, List<FieldMetadata> fields, int numShards) {
        try (BatchWriter bw = client.createBatchWriter(TableName.SHARD)) {
            for (FieldMetadata field : fields) {
                for (MetadataColumn col : field.getMetadataColumns()) {
                    switch (col) {
                        case I:
                            createFieldIndexColumn(bw, field, numShards);
                            break;
                        case E:
                            createEventColumn(bw, field, numShards);
                            break;
                        case TF:
                            createTermFrequencyColumn(bw, field, numShards);
                            break;
                        case RI:
                        case T:
                            // no-op for shard table
                            break;
                        default:
                            throw new RuntimeException("Column " + col + " is not supported");
                    }
                }
            }
        } catch (TableNotFoundException | MutationsRejectedException e) {
            throw new RuntimeException(e);
        }
    }

    private static void createFieldIndexColumn(BatchWriter bw, FieldMetadata field, int numShards) {
        // row fi<null>FIELD : value<null>datatype<null>uid VIZ
        String cf = "fi\0" + field.getFieldName();
        Map<String,Mutation> mutationsByRow = new LinkedHashMap<>();
        for (String datatype : field.getDatatypes()) {
            for (int eventId : field.getEventIds()) {
                String row = ShardKeyUtil.buildRow(eventId, numShards);
                Mutation m = mutationsByRow.computeIfAbsent(row, Mutation::new);

                String value = field.getValueForEventId(eventId);
                String uid = UidGenerator.uid(String.valueOf(eventId));
                for (Type<?> normalizer : field.getNormalizers()) {
                    String normalizedValue = normalizer.normalize(value);

                    // A content field indexes its whole phrase here and its individual tokens below, both under the same field name. That models real
                    // ingest configured without a token field name designator, where AbstractContentIngestHelper#isIndexedField implicitly treats a
                    // content-indexed field as indexed and the phrase is written alongside the tokens. Query-core pins that configuration - see
                    // AbstractDataTypeConfig, which disables the designator - and WiseGuysIngest hand-writes the same shape.
                    String cq = normalizedValue + "\0" + datatype + "\0" + uid;
                    m.put(cf, cq, VISIBILITY, TIMESTAMP, EMPTY_VALUE);

                    if (field.isContentField()) {
                        // index each token individually so content:phrase's FIELD == 'word' index-expansion can resolve
                        for (String token : value.split(" ")) {
                            String normalizedToken = normalizer.normalize(token);
                            String tokenCq = normalizedToken + "\0" + datatype + "\0" + uid;
                            m.put(cf, tokenCq, VISIBILITY, TIMESTAMP, EMPTY_VALUE);
                        }
                    }
                }
            }
        }

        try {
            for (Mutation m : mutationsByRow.values()) {
                bw.addMutation(m);
            }
        } catch (MutationsRejectedException e) {
            throw new RuntimeException(e);
        }
    }

    private static void createEventColumn(BatchWriter bw, FieldMetadata field, int numShards) {
        // row dt<null>uid : FIELD<null>value
        Map<String,Mutation> mutationsByRow = new LinkedHashMap<>();
        for (String datatype : field.getDatatypes()) {
            for (int eventId : field.getEventIds()) {
                String row = ShardKeyUtil.buildRow(eventId, numShards);
                Mutation m = mutationsByRow.computeIfAbsent(row, Mutation::new);

                String uid = UidGenerator.uid(String.valueOf(eventId));
                String cf = datatype + "\0" + uid;

                String value = field.getValueForEventId(eventId);
                String cq = field.getFieldName() + "\0" + value;
                m.put(cf, cq, VISIBILITY, TIMESTAMP, EMPTY_VALUE);
            }
        }
        try {
            for (Mutation m : mutationsByRow.values()) {
                bw.addMutation(m);
            }
        } catch (MutationsRejectedException e) {
            throw new RuntimeException(e);
        }
    }

    private static void createTermFrequencyColumn(BatchWriter bw, FieldMetadata field, int numShards) {
        // row shard : tf : datatype<null>uid<null>normalizedToken<null>FIELD : TermWeight.Info(termOffset)
        // IngestMetadata generates LcNoDiacriticsType phrases, but manually configured fields can carry several normalizers.
        Map<String,Mutation> mutationsByRow = new LinkedHashMap<>();
        for (String datatype : field.getDatatypes()) {
            for (int eventId : field.getEventIds()) {
                String row = ShardKeyUtil.buildRow(eventId, numShards);
                Mutation m = mutationsByRow.computeIfAbsent(row, Mutation::new);

                String value = field.getValueForEventId(eventId);
                String uid = UidGenerator.uid(String.valueOf(eventId));
                String[] tokens = value.split(" ");
                Map<String,TermWeight.Info.Builder> offsetsByToken = new LinkedHashMap<>();
                for (int position = 0; position < tokens.length; position++) {
                    Set<String> normalizedTokens = new LinkedHashSet<>();
                    for (Type<?> normalizer : field.getNormalizers()) {
                        normalizedTokens.add(normalizer.normalize(tokens[position]));
                    }
                    for (String normalizedToken : normalizedTokens) {
                        offsetsByToken.computeIfAbsent(normalizedToken, token -> TermWeight.Info.newBuilder()).addTermOffset(position);
                    }
                }
                // Repeated normalized tokens share a key; collect all positions before putting that key once.
                for (Map.Entry<String,TermWeight.Info.Builder> entry : offsetsByToken.entrySet()) {
                    String cq = datatype + "\0" + uid + "\0" + entry.getKey() + "\0" + field.getFieldName();
                    Value tfValue = new Value(entry.getValue().build().toByteArray());
                    m.put("tf", cq, VISIBILITY, TIMESTAMP, tfValue);
                }
            }
        }

        try {
            for (Mutation m : mutationsByRow.values()) {
                bw.addMutation(m);
            }
        } catch (MutationsRejectedException e) {
            throw new RuntimeException(e);
        }
    }
}
