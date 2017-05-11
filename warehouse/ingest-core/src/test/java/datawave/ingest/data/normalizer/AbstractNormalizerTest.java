package datawave.ingest.data.normalizer;

import datawave.TestBaseIngestHelper;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.NormalizedFieldAndValue;

import datawave.data.normalizer.NormalizationException;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

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
    
    @Before
    public void setUp() {
        Configuration conf = new Configuration();
        conf.set("test" + TypeRegistry.INGEST_HELPER, TestBaseIngestHelper.class.getName());
        TypeRegistry.reset();
        TypeRegistry.getInstance(conf);
        normalizer.setup(TypeRegistry.getType("test"), "test", conf);
    }
    
    @Test
    public void testHandlesAll() throws NormalizationException {
        Assert.assertEquals("converted", normalizer.normalizeFieldValue("FIELD1", "test"));
        Assert.assertEquals("converted", normalizer.normalizeFieldValue("FIELD2", "test"));
        
        NormalizedContentInterface nf = normalizer.normalize(new NormalizedFieldAndValue("FIELD1", "test"));
        Assert.assertEquals("FIELD1", nf.getEventFieldName());
        Assert.assertEquals("test", nf.getEventFieldValue());
        Assert.assertEquals("FIELD1", nf.getIndexedFieldName());
        Assert.assertEquals("converted", nf.getIndexedFieldValue());
        nf = normalizer.normalize(new NormalizedFieldAndValue("FIELD2", "test"));
        Assert.assertEquals("FIELD2", nf.getEventFieldName());
        Assert.assertEquals("test", nf.getEventFieldValue());
        Assert.assertEquals("FIELD2", nf.getIndexedFieldName());
        Assert.assertEquals("converted", nf.getIndexedFieldValue());
        
        Multimap<String,String> fields = HashMultimap.create();
        fields.put("FIELD1", "test");
        fields.put("FIELD2", "test");
        fields.put("FIELD2", "test2");
        Multimap<String,NormalizedContentInterface> nmap = normalizer.normalize(fields);
        Assert.assertEquals(3, nmap.size());
        Assert.assertEquals(1, nmap.get("FIELD1").size());
        Assert.assertEquals("converted", nmap.get("FIELD1").iterator().next().getIndexedFieldValue());
        Assert.assertEquals(2, nmap.get("FIELD2").size());
        Assert.assertEquals("converted", nmap.get("FIELD2").iterator().next().getIndexedFieldValue());
        
        Multimap<String,NormalizedContentInterface> nciFields = HashMultimap.create();
        nciFields.put("FIELD1", new NormalizedFieldAndValue("FIELD1", "test"));
        nciFields.put("FIELD2", new NormalizedFieldAndValue("FIELD2", "test"));
        nciFields.put("FIELD2", new NormalizedFieldAndValue("FIELD2", "test2"));
        nmap = normalizer.normalizeMap(nciFields);
        Assert.assertEquals(3, nmap.size());
        Assert.assertEquals(1, nmap.get("FIELD1").size());
        Assert.assertEquals("converted", nmap.get("FIELD1").iterator().next().getIndexedFieldValue());
        Assert.assertEquals(2, nmap.get("FIELD2").size());
        Assert.assertEquals("converted", nmap.get("FIELD2").iterator().next().getIndexedFieldValue());
        
    }
}
