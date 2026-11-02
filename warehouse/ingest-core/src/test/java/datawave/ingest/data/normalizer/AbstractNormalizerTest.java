package datawave.ingest.data.normalizer;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import datawave.TestBaseIngestHelper;
import datawave.data.normalizer.NormalizationException;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.NormalizedFieldAndValue;

public class AbstractNormalizerTest {
    public AbstractNormalizer normalizer = new AbstractNormalizer() {
        @Override
        public String convertFieldValue(String fieldName, String fieldValue) {
            return "converted";
        }

        @Override
        public String convertFieldRegex(String fieldName, String fieldRegex) {
            return "converted";
        }
    };

    @BeforeEach
    public void setUp() {
        Configuration conf = new Configuration();
        conf.set("test" + TypeRegistry.INGEST_HELPER, TestBaseIngestHelper.class.getName());
        TypeRegistry.reset();
        TypeRegistry.getInstance(conf);
        normalizer.setup(TypeRegistry.getType("test"), "test", conf);
    }

    @Test
    public void testHandlesAll() throws NormalizationException {
        assertEquals("converted", normalizer.normalizeFieldValue("FIELD1", "test"));
        assertEquals("converted", normalizer.normalizeFieldValue("FIELD2", "test"));

        NormalizedContentInterface nf = normalizer.normalize(new NormalizedFieldAndValue("FIELD1", "test"));
        assertEquals("FIELD1", nf.getEventFieldName());
        assertEquals("test", nf.getEventFieldValue());
        assertEquals("FIELD1", nf.getIndexedFieldName());
        assertEquals("converted", nf.getIndexedFieldValue());
        nf = normalizer.normalize(new NormalizedFieldAndValue("FIELD2", "test"));
        assertEquals("FIELD2", nf.getEventFieldName());
        assertEquals("test", nf.getEventFieldValue());
        assertEquals("FIELD2", nf.getIndexedFieldName());
        assertEquals("converted", nf.getIndexedFieldValue());

        Multimap<String,String> fields = HashMultimap.create();
        fields.put("FIELD1", "test");
        fields.put("FIELD2", "test");
        fields.put("FIELD2", "test2");
        Multimap<String,NormalizedContentInterface> nmap = normalizer.normalize(fields);
        assertEquals(3, nmap.size());
        assertEquals(1, nmap.get("FIELD1").size());
        assertEquals("converted", nmap.get("FIELD1").iterator().next().getIndexedFieldValue());
        assertEquals(2, nmap.get("FIELD2").size());
        assertEquals("converted", nmap.get("FIELD2").iterator().next().getIndexedFieldValue());

        Multimap<String,NormalizedContentInterface> nciFields = HashMultimap.create();
        nciFields.put("FIELD1", new NormalizedFieldAndValue("FIELD1", "test"));
        nciFields.put("FIELD2", new NormalizedFieldAndValue("FIELD2", "test"));
        nciFields.put("FIELD2", new NormalizedFieldAndValue("FIELD2", "test2"));
        nmap = normalizer.normalizeMap(nciFields);
        assertEquals(3, nmap.size());
        assertEquals(1, nmap.get("FIELD1").size());
        assertEquals("converted", nmap.get("FIELD1").iterator().next().getIndexedFieldValue());
        assertEquals(2, nmap.get("FIELD2").size());
        assertEquals("converted", nmap.get("FIELD2").iterator().next().getIndexedFieldValue());

    }
}
