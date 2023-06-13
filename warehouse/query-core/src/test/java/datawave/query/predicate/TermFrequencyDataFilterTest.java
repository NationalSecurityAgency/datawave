package datawave.query.predicate;

import com.google.common.collect.Sets;
import datawave.data.type.LcNoDiacriticsType;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.util.TypeMetadata;
import org.apache.accumulo.core.data.Key;
import org.apache.commons.jexl3.parser.JexlNode;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TermFrequencyDataFilterTest {
    
    private TermFrequencyDataFilter filter;
    private TypeMetadata metadata;
    
    private final Set<String> nonEventFields = Sets.newHashSet("FIELD1", "FIELD2");
    
    private final String row = "row";
    private final String cf = "tf";
    
    private final Key tfKey1 = new Key(row, cf, "datatype\0uid\0value\0FIELD1");
    private final Key tfKey2 = new Key(row, cf, "datatype\0uid\0value\0FIELD2");
    private final Key tfKey3 = new Key(row, cf, "datatype\0uid\0value\0FIELD3");
    
    private final Key tfKey4 = new Key(row, cf, "datatype\0uid\0random\0FIELD1");
    private final Key tfKey5 = new Key(row, cf, "datatype\0uid\0random\0FIELD2");
    private final Key tfKey6 = new Key(row, cf, "datatype\0uid\0random\0FIELD3");
    
    @Before
    public void setup() {
        
        String lcNoDiacritics = LcNoDiacriticsType.class.getName();
        
        metadata = new TypeMetadata();
        metadata.put("FIELD1", "datatype", lcNoDiacritics);
        metadata.put("FIELD2", "datatype", lcNoDiacritics);
        metadata.put("FIELD3", "datatype", lcNoDiacritics);
    }
    
    @Test
    public void testSingleFieldAndValue() {
        JexlNode node = JexlNodeFactory.buildEQNode("FIELD1", "value");
        filter = new TermFrequencyDataFilter(node, metadata, nonEventFields);
        
        // first key matches 'FIELD1' and 'value'
        assertTrue(filter.keep(tfKey1));
        assertFalse(filter.keep(tfKey2));
        assertFalse(filter.keep(tfKey3));
        
        // no matches on the value
        assertFalse(filter.keep(tfKey4));
        assertFalse(filter.keep(tfKey5));
        assertFalse(filter.keep(tfKey6));
    }
    
    // same test as above, different field
    @Test
    public void testOtherFieldAndValue() {
        JexlNode node = JexlNodeFactory.buildEQNode("FIELD3", "value");
        filter = new TermFrequencyDataFilter(node, metadata, nonEventFields);
        
        // third key matches 'FIELD3' and 'value'
        assertFalse(filter.keep(tfKey1));
        assertFalse(filter.keep(tfKey2));
        assertTrue(filter.keep(tfKey3));
        
        // no matches on the value
        assertFalse(filter.keep(tfKey4));
        assertFalse(filter.keep(tfKey5));
        assertFalse(filter.keep(tfKey6));
    }
    
}
