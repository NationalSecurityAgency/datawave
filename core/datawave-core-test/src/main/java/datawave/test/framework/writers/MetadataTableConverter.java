package datawave.test.framework.writers;

import static datawave.test.framework.util.MetadataColumn.T;

import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.data.Key;

import datawave.data.type.Type;
import datawave.table.constants.TableName;
import datawave.test.framework.FieldMetadata;
import datawave.test.framework.util.MetadataColumn;

/**
 * Simple utility used to convert {@link FieldMetadata} into keys formatted for the DatawaveMetadata table.
 */
public class MetadataTableConverter {

    private MetadataTableConverter() {
        // enforce static access
    }

    /**
     * Convert the list of {@link FieldMetadata} to keys formatted for the {@link TableName#METADATA}
     *
     * @param fields
     *            the list of field metadata
     * @return a list of keys
     */
    public static List<Key> convert(List<FieldMetadata> fields) {
        List<Key> keys = new ArrayList<>();
        for (FieldMetadata field : fields) {
            for (MetadataColumn column : field.getMetadataColumns()) {
                switch (column) {
                    case I:
                        createI(keys, field);
                        break;
                    case RI:
                        createRI(keys, field);
                        break;
                    case E:
                        createE(keys, field);
                        break;
                    case TF:
                        createTF(keys, field);
                        break;
                    case T:
                        createT(keys, field);
                        break;
                    default:
                        throw new IllegalStateException("Unexpected column: " + column);
                }
            }
        }
        return keys;
    }

    private static void createI(List<Key> keys, FieldMetadata metadata) {
        createStandardColumn(keys, metadata, "i");
    }

    private static void createRI(List<Key> keys, FieldMetadata metadata) {
        createStandardColumn(keys, metadata, "ri");
    }

    private static void createE(List<Key> keys, FieldMetadata metadata) {
        createStandardColumn(keys, metadata, "e");
    }

    private static void createTF(List<Key> keys, FieldMetadata metadata) {
        createStandardColumn(keys, metadata, "tf");
    }

    private static void createStandardColumn(List<Key> keys, FieldMetadata metadata, String column) {
        for (String datatype : metadata.getDatatypes()) {
            Key key = new Key(metadata.getFieldName(), column, datatype);
            keys.add(key);
        }
    }

    private static void createT(List<Key> keys, FieldMetadata metadata) {
        for (String datatype : metadata.getDatatypes()) {
            for (Type<?> normalizer : metadata.getNormalizers()) {
                Key key = new Key(metadata.getFieldName(), "t", datatype + "\0" + normalizer.getClass().getName());
                keys.add(key);
            }
        }
    }
}
