package datawave.test.framework.writers;

import static datawave.test.framework.util.MetadataColumn.E;
import static datawave.test.framework.util.MetadataColumn.I;
import static datawave.test.framework.util.MetadataColumn.RI;
import static datawave.test.framework.util.MetadataColumn.T;
import static datawave.test.framework.util.MetadataColumn.TF;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.junit.jupiter.api.Test;

import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.NumberType;
import datawave.table.constants.TableName;
import datawave.test.framework.FieldMetadata;

class MetadataTableWriterTest extends AbstractTableWriterTest {

    private static final String LC_TYPE = LcNoDiacriticsType.class.getName();
    private static final String NUMBER_TYPE = NumberType.class.getName();

    /**
     * Each metadata column maps to its own column family, and the datatype is the column qualifier.
     */
    @Test
    void testEachMetadataColumnMapsToItsColumnFamily() {
        FieldMetadata field = createField("FIELD", I, RI, E, TF);
        MetadataTableWriter.write(client, List.of(field));

        //  @formatter:off
        List<String> expected = List.of(
                "FIELD e:" + DATATYPE,
                "FIELD i:" + DATATYPE,
                "FIELD ri:" + DATATYPE,
                "FIELD t:" + DATATYPE + "\0" + LC_TYPE,
                "FIELD tf:" + DATATYPE);
        //  @formatter:on
        assertEquals(expected, scanKeys(TableName.METADATA));
    }

    /**
     * The type column is keyed on the datatype and the normalizer class, so a field with two normalizers and two datatypes produces all four pairings.
     */
    @Test
    void testTypeColumnCoversEveryDatatypeAndNormalizerPairing() {
        FieldMetadata field = new FieldMetadata("FIELD");
        field.setMetadataColumns(List.of(T));
        field.setDatatypes(List.of("dt-a", "dt-b"));
        field.setNormalizers(normalizers(new LcNoDiacriticsType(), new NumberType()));

        MetadataTableWriter.write(client, List.of(field));

        //  @formatter:off
        List<String> expected = List.of(
                "FIELD t:dt-a\0" + LC_TYPE,
                "FIELD t:dt-a\0" + NUMBER_TYPE,
                "FIELD t:dt-b\0" + LC_TYPE,
                "FIELD t:dt-b\0" + NUMBER_TYPE);
        //  @formatter:on
        assertEquals(expected, scanKeys(TableName.METADATA));
    }

    /**
     * A standard column is written once per datatype.
     */
    @Test
    void testStandardColumnIsWrittenPerDatatype() {
        FieldMetadata field = new FieldMetadata("FIELD");
        field.setMetadataColumns(List.of(I));
        field.setDatatypes(List.of("dt-a", "dt-b"));

        MetadataTableWriter.write(client, List.of(field));

        assertEquals(List.of("FIELD i:dt-a", "FIELD i:dt-b"), scanKeys(TableName.METADATA));
    }

    /**
     * The row is the field name, so two {@link FieldMetadata} entries describing the same field collapse into a single row carrying both columns rather than
     * one row overwriting the other.
     */
    @Test
    void testFieldsSharingANameCollapseIntoOneRow() {
        FieldMetadata indexed = new FieldMetadata("FIELD");
        indexed.setMetadataColumns(List.of(I));
        indexed.setDatatypes(List.of(DATATYPE));

        FieldMetadata event = new FieldMetadata("FIELD");
        event.setMetadataColumns(List.of(E));
        event.setDatatypes(List.of(DATATYPE));

        MetadataTableWriter.write(client, List.of(indexed, event));

        assertEquals(List.of("FIELD e:" + DATATYPE, "FIELD i:" + DATATYPE), scanKeys(TableName.METADATA));
    }

    /**
     * Accumulo rejects a mutation carrying no puts, so a field with no metadata columns must be dropped before it reaches the batch writer.
     * <p>
     * Such a field cannot carry a normalizer either: {@link FieldMetadata#setNormalizers(List)} would add the
     * {@link datawave.test.framework.util.MetadataColumn#T} column and give it something to write.
     */
    @Test
    void testFieldWithNoMetadataColumnsIsSkipped() {
        FieldMetadata empty = new FieldMetadata("FIELD");
        empty.setMetadataColumns(List.of());
        empty.setDatatypes(List.of(DATATYPE));

        MetadataTableWriter.write(client, List.of(empty));

        assertTrue(scanKeys(TableName.METADATA).isEmpty(), "a field with no metadata columns should write nothing");
    }

    /**
     * Setting a normalizer implicitly adds the type column, so a field is never described as normalized without saying how.
     */
    @Test
    void testNormalizedFieldAlwaysCarriesATypeColumn() {
        FieldMetadata field = new FieldMetadata("FIELD");
        field.setMetadataColumns(List.of(I));
        field.setDatatypes(List.of(DATATYPE));
        field.setNormalizers(normalizers(new LcNoDiacriticsType()));

        MetadataTableWriter.write(client, List.of(field));

        assertEquals(List.of("FIELD i:" + DATATYPE, "FIELD t:" + DATATYPE + "\0" + LC_TYPE), scanKeys(TableName.METADATA));
    }

    /**
     * Every entry carries the framework's single timestamp, visibility and an empty value, so one set of auths and one date cover the whole table.
     */
    @Test
    void testEntriesShareTimestampVisibilityAndEmptyValue() {
        MetadataTableWriter.write(client, List.of(createField("FIELD", I, E)));

        List<Map.Entry<Key,Value>> entries = scan(TableName.METADATA);
        assertEquals(3, entries.size());
        for (Map.Entry<Key,Value> entry : entries) {
            assertEquals(TableWriter.TIMESTAMP, entry.getKey().getTimestamp());
            assertEquals(TableWriter.DEFAULT_VISIBILITY, entry.getKey().getColumnVisibility().toString());
            assertEquals(0, entry.getValue().get().length, "metadata entries carry no value");
        }
    }
}
