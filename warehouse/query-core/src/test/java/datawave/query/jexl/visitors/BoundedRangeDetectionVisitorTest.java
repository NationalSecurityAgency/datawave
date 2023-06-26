package datawave.query.jexl.visitors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import datawave.data.type.StringType;
import datawave.data.type.Type;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.util.MockMetadataHelper;

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
    public void testBoundedRanges() {
        // simple case
        test("((_Bounded_ = true) && (FOO >= '1' && FOO <= '10'))", true);
        // bounded range as the child of a union
        test("(FOO == 'bar' || ((_Bounded_ = true) && (FOO >= '1' && FOO <= '10')))", true);
        // bounded range as the child of an intersection
        test("(FOO == 'bar' && ((_Bounded_ = true) && (FOO >= '1' && FOO <= '10')))", true);
    }

    @Test
    public void testNonBoundedRanges() {
        test("(FOO == '1' && FOO == '10')", false);
    }

    @Test(expected = DatawaveFatalQueryException.class)
    public void testBoundedRangeMalformed() {
        test("((_Bounded_ = true) && (FOO == '1' && FOO == '10'))", false);
    }

    private void test(String query, boolean expected) {
        try {
            ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
            assertEquals(expected, BoundedRangeDetectionVisitor.mustExpandBoundedRange(config, helper, script));
        } catch (ParseException e) {
            fail("Failed to parse query: " + query);
        }
    }

}
