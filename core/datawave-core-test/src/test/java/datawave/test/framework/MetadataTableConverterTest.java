package datawave.test.framework;

import static datawave.test.framework.util.MetadataColumn.E;
import static datawave.test.framework.util.MetadataColumn.I;
import static datawave.test.framework.util.MetadataColumn.RI;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.data.Key;
import org.junit.jupiter.api.Test;

import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.Type;
import datawave.test.framework.util.MetadataColumn;
import datawave.test.framework.writers.MetadataTableConverter;

public class MetadataTableConverterTest {

    private final List<Type<?>> types = List.of(new LcNoDiacriticsType());

    /**
     * Have I mentioned how much I dislike the namespace collision between datatypes and typed normalizers?
     */
    @Test
    public void testConvertSingleColumnSingleDatatypeSingleTypeType() {
        FieldMetadata metadata = createMetadata(List.of(I), List.of("datatype-a"), types);
        List<Key> results = MetadataTableConverter.convert(List.of(metadata));

        List<Key> expected = new ArrayList<>();
        expected.add(new Key("ID", "i", "datatype-a"));
        expected.add(new Key("ID", "t", "datatype-a\u0000datawave.data.type.LcNoDiacriticsType"));

        assertEquals(expected, results);
    }

    @Test
    public void testConvertSingleColumnMultipleDatatypes() {
        FieldMetadata metadata = createMetadata(List.of(I), List.of("datatype-a", "datatype-b"));
        List<Key> results = MetadataTableConverter.convert(List.of(metadata));

        List<Key> expected = new ArrayList<>();
        expected.add(new Key("ID", "i", "datatype-a"));
        expected.add(new Key("ID", "i", "datatype-b"));

        assertEquals(expected, results);
    }

    @Test
    public void testConvertMultipleColumnsSingleDatatype() {
        FieldMetadata metadata = createMetadata(List.of(I, E), List.of("datatype-a"));
        List<Key> results = MetadataTableConverter.convert(List.of(metadata));

        List<Key> expected = new ArrayList<>();
        expected.add(new Key("ID", "i", "datatype-a"));
        expected.add(new Key("ID", "e", "datatype-a"));

        assertEquals(expected, results);
    }

    @Test
    public void testConvertMultipleColumnsMultipleDatatypes() {
        FieldMetadata metadata = createMetadata(List.of(I, RI, E), List.of("datatype-a", "datatype-b"));
        List<Key> results = MetadataTableConverter.convert(List.of(metadata));

        List<Key> expected = new ArrayList<>();
        expected.add(new Key("ID", "i", "datatype-a"));
        expected.add(new Key("ID", "i", "datatype-b"));
        expected.add(new Key("ID", "ri", "datatype-a"));
        expected.add(new Key("ID", "ri", "datatype-b"));
        expected.add(new Key("ID", "e", "datatype-a"));
        expected.add(new Key("ID", "e", "datatype-b"));

        assertEquals(expected, results);
    }

    private FieldMetadata createMetadata(List<MetadataColumn> columns, List<String> datatypes) {
        return createMetadata(columns, datatypes, null);
    }

    private FieldMetadata createMetadata(List<MetadataColumn> columns, List<String> datatypes, List<Type<?>> normalizers) {
        FieldMetadata metadata = new FieldMetadata("ID");
        metadata.setMetadataColumns(columns);
        metadata.setDatatypes(datatypes);
        if (normalizers != null) {
            metadata.setNormalizers(normalizers);
        }
        return metadata;
    }
}
