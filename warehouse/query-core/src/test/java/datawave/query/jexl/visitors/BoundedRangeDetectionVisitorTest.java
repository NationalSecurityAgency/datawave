package datawave.query.jexl.visitors;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import datawave.data.type.StringType;
import datawave.data.type.Type;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.util.MockMetadataHelper;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class BoundedRangeDetectionVisitorTest {
    private ShardQueryConfiguration config;
    
    @Before
    public void init() throws Exception {
        this.config = ShardQueryConfiguration.create();
    }
    
    @Test
    public void testBoundedRange() throws ParseException {
        Multimap<String,Type<?>> queryFieldsDatatypes = HashMultimap.create();
        queryFieldsDatatypes.put("foo", new StringType());
        
        config.setQueryFieldsDatatypes(queryFieldsDatatypes);
        
        MockMetadataHelper helper = new MockMetadataHelper();
        helper.setNonEventFields(queryFieldsDatatypes.keySet());
        
        String queryString = "((_Bounded_ = true) && (foo >= '1' && foo <= '10'))";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(queryString);
        boolean foo = BoundedRangeDetectionVisitor.mustExpandBoundedRange(config, helper, script);
        
        assertTrue(foo);
    }
    
}
