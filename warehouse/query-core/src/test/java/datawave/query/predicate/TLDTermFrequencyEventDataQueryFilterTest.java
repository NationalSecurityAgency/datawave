package datawave.query.predicate;

import com.google.common.collect.Sets;
import datawave.query.jexl.JexlASTHelper;
import org.apache.accumulo.core.data.Key;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ParseException;
import org.junit.Test;

import java.util.Collections;
import java.util.Set;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TLDTermFrequencyEventDataQueryFilterTest {
    
    private final Key tldField1 = new Key("row", "fi\0FIELD1", "value\0datatype\0d8zay2.-3pnndm.-anolok");
    private final Key tldField2 = new Key("row", "fi\0FIELD2", "value\0datatype\0d8zay2.-3pnndm.-anolok");
    private final Key tldField3 = new Key("row", "fi\0FIELD3", "value\0datatype\0d8zay2.-3pnndm.-anolok");
    
    private final Key childField1 = new Key("row", "fi\0FIELD1", "value\0datatype\0d8zay2.-3pnndm.-anolok.23");
    private final Key childField2 = new Key("row", "fi\0FIELD2", "value\0datatype\0d8zay2.-3pnndm.-anolok.33");
    private final Key childField3 = new Key("row", "fi\0FIELD3", "value\0datatype\0d8zay2.-3pnndm.-anolok.45");
    
    @Test
    public void testTLDTermFrequencyEventDataQueryFilter() throws ParseException {
        
        String query = "FIELD1 == 'value'";
        ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery(query);
        EventDataQueryFieldFilter fieldFilter = new EventDataQueryFieldFilter(script, Collections.emptySet());
        
        Set<String> indexOnlyFields = Sets.newHashSet("FIELD1", "FIELD2");
        TLDTermFrequencyEventDataQueryFilter filter = new TLDTermFrequencyEventDataQueryFilter(indexOnlyFields, fieldFilter);
        
        // retain query index-only fields in the tld
        assertTrue(filter.keep(tldField1));
        assertFalse(filter.keep(tldField2));
        assertFalse(filter.keep(tldField3));
        
        // retain ALL non-tld fields
        assertTrue(filter.keep(childField1));
        assertFalse(filter.keep(childField2));
        assertFalse(filter.keep(childField3));
    }
    
}
