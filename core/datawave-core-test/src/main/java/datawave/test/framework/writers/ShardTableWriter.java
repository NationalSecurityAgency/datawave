package datawave.test.framework.writers;

import static datawave.test.framework.writers.TableWriter.TIMESTAMP;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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
        // field.isContentField() is guaranteed true here (the TF column is only dispatched for content fields), and its value is a space-joined phrase
        // normalized with LcNoDiacriticsType (see IngestMetadata's TF/normalizer skip rule).
        Type<?> normalizer = field.getNormalizers().get(0);
        Map<String,Mutation> mutationsByRow = new LinkedHashMap<>();
        for (String datatype : field.getDatatypes()) {
            for (int eventId : field.getEventIds()) {
                String row = ShardKeyUtil.buildRow(eventId, numShards);
                Mutation m = mutationsByRow.computeIfAbsent(row, Mutation::new);

                String value = field.getValueForEventId(eventId);
                String uid = UidGenerator.uid(String.valueOf(eventId));
                String[] tokens = value.split(" ");
                for (int position = 0; position < tokens.length; position++) {
                    String normalizedToken = normalizer.normalize(tokens[position]);
                    String cq = datatype + "\0" + uid + "\0" + normalizedToken + "\0" + field.getFieldName();
                    TermWeight.Info info = TermWeight.Info.newBuilder().addTermOffset(position).build();
                    Value tfValue = new Value(info.toByteArray());
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
