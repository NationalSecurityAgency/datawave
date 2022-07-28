package datawave.query.jexl.visitors;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import datawave.data.type.StringType;
import datawave.data.type.Type;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.util.MockMetadataHelper;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BoundedRangeDetectionVisitorTest {
    private ShardQueryConfiguration config;
    private MockMetadataHelper helper;
    
    @Before
    public void init() throws Exception {
        this.config = ShardQueryConfiguration.create();
        Multimap<String,Type<?>> queryFieldsDatatypes = HashMultimap.create();
        queryFieldsDatatypes.put("FOO", new StringType());
        
        config.setQueryFieldsDatatypes(queryFieldsDatatypes);
        
        this.helper = new MockMetadataHelper();
        this.helper.setNonEventFields(queryFieldsDatatypes.keySet());
    }
    
    @Test
    public void testBoundedRange() throws ParseException {
        String queryString = "((_Bounded_ = true) && (FOO >= '1' && FOO <= '10'))";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(queryString);
        assertTrue(BoundedRangeDetectionVisitor.mustExpandBoundedRange(config, helper, script));
    }
    
    @Test
    public void testUnboundedRange() throws ParseException {
        String queryString = "(FOO == '1' && FOO == '10')";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(queryString);
        assertFalse(BoundedRangeDetectionVisitor.mustExpandBoundedRange(config, helper, script));
    }
    
    @Test(expected = DatawaveFatalQueryException.class)
    public void testBoundedRangeMalformed() throws ParseException {
        String queryString = "((_Bounded_ = true) && (FOO == '1' && FOO == '10'))";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(queryString);
        BoundedRangeDetectionVisitor.mustExpandBoundedRange(config, helper, script);
    }
    
    @Test
    public void testBoundedRangeChildOr() throws ParseException {
        String queryString = "(FOO == 'bar' || ((_Bounded_ = true) && (FOO >= '1' && FOO <= '10')))";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(queryString);
        assertTrue(BoundedRangeDetectionVisitor.mustExpandBoundedRange(config, helper, script));
    }
    
    @Test
    public void testBoundedRangeChildAnd() throws ParseException {
        String queryString = "(FOO == 'bar' && ((_Bounded_ = true) && (FOO >= '1' && FOO <= '10')))";
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(queryString);
        assertTrue(BoundedRangeDetectionVisitor.mustExpandBoundedRange(config, helper, script));
    }
    
}
