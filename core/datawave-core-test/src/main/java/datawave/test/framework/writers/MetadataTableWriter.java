package datawave.test.framework.writers;

import java.util.List;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;

import datawave.data.type.Type;
import datawave.table.constants.TableName;
import datawave.test.framework.FieldMetadata;
import datawave.test.framework.util.MetadataColumn;

public class MetadataTableWriter {

    private static final Value EMPTY_VALUE = new Value();

    private MetadataTableWriter() {
        // enforce static access
    }

    public static void write(AccumuloClient client, List<FieldMetadata> fields) {
        try (BatchWriter bw = client.createBatchWriter(TableName.METADATA)) {
            for (FieldMetadata field : fields) {
                for (MetadataColumn column : field.getMetadataColumns()) {
                    switch (column) {
                        case I:
                            writeStandardColumn(bw, field, "i");
                            break;
                        case RI:
                            writeStandardColumn(bw, field, "ri");
                            break;
                        case E:
                            writeStandardColumn(bw, field, "e");
                            break;
                        case TF:
                            writeStandardColumn(bw, field, "tf");
                            break;
                        case T:
                            writeTColumn(bw, field);
                            break;
                        default:
                            throw new IllegalStateException("Unexpected column: " + column);
                    }
                }
            }
        } catch (TableNotFoundException | MutationsRejectedException e) {
            throw new RuntimeException(e);
        }
    }

    private static void writeStandardColumn(BatchWriter bw, FieldMetadata field, String column) throws MutationsRejectedException {
        for (String datatype : field.getDatatypes()) {
            Mutation m = new Mutation(field.getFieldName());
            m.put(column, datatype, EMPTY_VALUE);
            bw.addMutation(m);
        }
    }

    private static void writeTColumn(BatchWriter bw, FieldMetadata field) throws MutationsRejectedException {
        for (String datatype : field.getDatatypes()) {
            for (Type<?> normalizer : field.getNormalizers()) {
                Mutation m = new Mutation(field.getFieldName());
                m.put("t", datatype + "\0" + normalizer.getClass().getName(), EMPTY_VALUE);
                bw.addMutation(m);
            }
        }
    }
}
