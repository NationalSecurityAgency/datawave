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
import datawave.table.constants.TableName;
import datawave.test.framework.FieldMetadata;
import datawave.test.framework.util.MetadataColumn;

/**
 * Writes the DatawaveMetadata entries describing where each field lives and how it is normalized.
 * <p>
 * Entries carry the same visibility and timestamp as the shard and shard index entries, so a single set of auths and a single date cover every table the
 * framework populates.
 */
public class MetadataTableWriter {

    private static final Value EMPTY_VALUE = new Value();
    private static final ColumnVisibility VISIBILITY = new ColumnVisibility(TableWriter.DEFAULT_VISIBILITY);

    private MetadataTableWriter() {
        // enforce static access
    }

    /**
     * Write the metadata entries for every field.
     *
     * @param client
     *            the accumulo client
     * @param fields
     *            the field metadata
     */
    public static void write(AccumuloClient client, List<FieldMetadata> fields) {
        try (BatchWriter bw = client.createBatchWriter(TableName.METADATA)) {
            // the row is the field name, so all of a field's columns belong to one mutation rather than one mutation per put
            Map<String,Mutation> mutationsByField = new LinkedHashMap<>();

            for (FieldMetadata field : fields) {
                Mutation m = mutationsByField.computeIfAbsent(field.getFieldName(), Mutation::new);
                for (MetadataColumn column : field.getMetadataColumns()) {
                    switch (column) {
                        case I:
                            putStandardColumn(m, field, "i");
                            break;
                        case RI:
                            putStandardColumn(m, field, "ri");
                            break;
                        case E:
                            putStandardColumn(m, field, "e");
                            break;
                        case TF:
                            putStandardColumn(m, field, "tf");
                            break;
                        case T:
                            putTypeColumn(m, field);
                            break;
                        default:
                            throw new IllegalStateException("Unexpected column: " + column);
                    }
                }
            }

            for (Mutation m : mutationsByField.values()) {
                // accumulo rejects a mutation with no puts, which a field configured with no metadata columns would produce
                if (m.size() > 0) {
                    bw.addMutation(m);
                }
            }
        } catch (TableNotFoundException | MutationsRejectedException e) {
            throw new RuntimeException(e);
        }
    }

    private static void putStandardColumn(Mutation m, FieldMetadata field, String column) {
        for (String datatype : field.getDatatypes()) {
            m.put(column, datatype, VISIBILITY, TIMESTAMP, EMPTY_VALUE);
        }
    }

    private static void putTypeColumn(Mutation m, FieldMetadata field) {
        for (String datatype : field.getDatatypes()) {
            for (Type<?> normalizer : field.getNormalizers()) {
                m.put("t", datatype + "\0" + normalizer.getClass().getName(), VISIBILITY, TIMESTAMP, EMPTY_VALUE);
            }
        }
    }
}
