package datawave.webservice.metadata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import datawave.webservice.dictionary.data.DefaultDataDictionary;
import datawave.webservice.dictionary.data.DefaultDescription;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;

public class DefaultMetadataFieldTest {

    @Test
    public void testProtostuffRoundTripCarriesDescriptionVisibilities() {
        DefaultMetadataField field = createField();

        byte[] bytes = ProtostuffIOUtil.toByteArray(field, DefaultMetadataField.getSchema(), LinkedBuffer.allocate());
        DefaultMetadataField restored = new DefaultMetadataField();
        ProtostuffIOUtil.mergeFrom(bytes, restored, DefaultMetadataField.getSchema());

        assertEquals("FIELD", restored.getFieldName());
        assertEquals("datatype", restored.getDataType());
        assertEquals("20240101", restored.getLastUpdated());
        assertEquals(2, restored.getDescriptions().size());

        Map<String,String> visibilities = restored.getDescriptions().stream()
                        .collect(Collectors.toMap(DefaultDescription::getDescription, DefaultDescription::getColumnVisibility));
        assertEquals("PRIVATE", visibilities.get("first"));
        assertEquals("PUBLIC|USER", visibilities.get("second"));
    }

    @Test
    public void testDataDictionaryMainContentShowsVisibility() {
        DefaultDataDictionary dictionary = new DefaultDataDictionary();
        dictionary.setFields(List.of(createField()));

        String content = dictionary.getMainContent();
        assertTrue(content.contains("PRIVATE first"), content);
        assertTrue(content.contains("PUBLIC|USER second"), content);
    }

    private static DefaultMetadataField createField() {
        DefaultMetadataField field = new DefaultMetadataField();
        field.setFieldName("FIELD");
        field.setDataType("datatype");
        field.setLastUpdated("20240101");
        field.getDescriptions().add(new DefaultDescription("first", "PRIVATE"));
        field.getDescriptions().add(new DefaultDescription("second", "PUBLIC|USER"));
        return field;
    }
}
