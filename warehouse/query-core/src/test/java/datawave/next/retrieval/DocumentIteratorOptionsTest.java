package datawave.next.retrieval;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import datawave.data.type.LcNoDiacriticsType;
import datawave.query.iterator.QueryOptions;
import datawave.query.util.TypeMetadata;
import datawave.query.util.TypeMetadataSerializer;

public class DocumentIteratorOptionsTest {

    private Map<String,String> baseOptions() {
        Map<String,String> options = new HashMap<>();
        options.put(QueryOptions.QUERY, "FIELD_A == 'a'");
        options.put(DocumentIteratorOptions.CANDIDATES, "dt\0uid-1");
        options.put(QueryOptions.START_TIME, String.valueOf(0L));
        options.put(QueryOptions.END_TIME, String.valueOf(Long.MAX_VALUE));
        return options;
    }

    @Test
    public void testTypeMetadataFromNativeStringFormat() {
        TypeMetadata expected = new TypeMetadata();
        expected.put("FIELD_A", "ingestA", LcNoDiacriticsType.class.getSimpleName());

        Map<String,String> options = baseOptions();
        options.put(QueryOptions.TYPE_METADATA, expected.toString());

        DocumentIteratorOptions documentIteratorOptions = new DocumentIteratorOptions();
        documentIteratorOptions.validateOptions(options);

        assertEquals(expected, documentIteratorOptions.typeMetadata);
    }

    @Test
    public void testTypeMetadataFromKryoFormat() {
        TypeMetadata expected = new TypeMetadata();
        expected.put("FIELD_A", "ingestA", LcNoDiacriticsType.class.getSimpleName());

        Map<String,String> options = baseOptions();
        options.put(QueryOptions.TYPE_METADATA, new TypeMetadataSerializer().serialize(expected));
        options.put(QueryOptions.TYPE_METADATA_KRYO, "true");

        DocumentIteratorOptions documentIteratorOptions = new DocumentIteratorOptions();
        documentIteratorOptions.validateOptions(options);

        assertEquals(expected, documentIteratorOptions.typeMetadata);
    }
}
