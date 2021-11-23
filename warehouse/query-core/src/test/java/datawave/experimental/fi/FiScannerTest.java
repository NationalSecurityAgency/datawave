package datawave.experimental.fi;

import com.google.common.base.Joiner;
import com.google.common.collect.Sets;
import datawave.data.type.NumberType;
import datawave.data.type.Type;
import datawave.query.jexl.JexlASTHelper;
import datawave.util.TableName;
import datawave.experimental.QueryTermVisitor;
import datawave.experimental.util.AccumuloUtil;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

/**
 * Test scanners against an in-memory accumulo instance
 */
public class FiScannerTest {
    
    protected static AccumuloUtil util;
    
    private static final Type<?> numberType = new NumberType();
    
    private final Set<String> aliceUids = util.getAliceUids();
    private final Set<String> bobUids = util.getBobUids();
    private final Set<String> eveUids = util.getEveUids();
    private final Set<String> extraUids = util.getExtraUids();
    private final Set<String> oberonUids = util.getOberonUids();
    
    @BeforeClass
    public static void setup() throws Exception {
        util = new AccumuloUtil();
        util.create(FiScannerTest.class.getSimpleName());
        util.loadData();
    }
    
    @Test
    public void testEq() {
        String query = "FIRST_NAME == 'oberon'";
        Map<String,Set<String>> expected = new HashMap<>();
        expected.put("FIRST_NAME == 'oberon'", oberonUids);
        test(query, expected);
    }
    
    @Test
    public void testNe() {
        String query = "FIRST_NAME != 'oberon'";
        Map<String,Set<String>> expected = new HashMap<>();
        expected.put("FIRST_NAME != 'oberon'", oberonUids);
        test(query, expected);
        // technically this should fail because we do not look up top level negations
    }
    
    @Test
    public void testEr() {
        String query = "FIRST_NAME =~ 'e.*'";
        Map<String,Set<String>> expected = new HashMap<>();
        Set<String> allExtraUids = new HashSet<>();
        allExtraUids.addAll(eveUids);
        allExtraUids.addAll(extraUids);
        expected.put("FIRST_NAME =~ 'e.*'", allExtraUids);
        test(query, expected);
    }
    
    @Test
    public void testNr() {
        String query = "FIRST_NAME !~ 'e.*'";
        Map<String,Set<String>> expected = new HashMap<>();
        expected.put("FIRST_NAME !~ 'e.*'", Sets.union(eveUids, extraUids));
        test(query, expected);
        // technically this should fail because we do not look up top level negations
    }
    
    // LT, GT, LE, GE nodes should throw an exception. We only lookup bounded ranges.
    
    @Ignore
    @Test
    public void testLessThan() {
        String num = numberType.normalize("8");
        String query = "MSG_SIZE < '" + num + "'";
        Map<String,Set<String>> expected = new HashMap<>();
        expected.put(query, Sets.newHashSet(util.getUid0(), util.getUid1()));
        test(query, expected);
    }
    
    @Ignore
    @Test
    public void testGreaterThan() {
        String num = numberType.normalize("8");
        String query = "MSG_SIZE <= '" + num + "'";
        Map<String,Set<String>> expected = new HashMap<>();
        expected.put(query, Sets.newHashSet(util.getUid0(), util.getUid1(), util.getUid3(), util.getUid4()));
        test(query, expected);
    }
    
    @Ignore
    @Test
    public void testLessThanEqual() {
        String num = numberType.normalize("8");
        String query = "MSG_SIZE <= '" + num + "'";
        Map<String,Set<String>> expected = new HashMap<>();
        expected.put(query, Sets.newHashSet(util.getUid0(), util.getUid1(), util.getUid3(), util.getUid4()));
        test(query, expected);
    }
    
    @Ignore
    @Test
    public void testGreaterThanEqual() {
        String num = numberType.normalize("8");
        String query = "MSG_SIZE >= '" + num + "'";
        Map<String,Set<String>> expected = new HashMap<>();
        expected.put(query, Sets.newHashSet(util.getUid2(), util.getUid3(), util.getUid4()));
        test(query, expected);
    }
    
    // some intersections
    
    @Test
    public void testIntersectionOfTwoTerms() {
        String query = "FIRST_NAME == 'bob' && FIRST_NAME == 'eve'";
        Map<String,Set<String>> expected = new HashMap<>();
        expected.put("FIRST_NAME == 'bob'", bobUids);
        expected.put("FIRST_NAME == 'eve'", eveUids);
        test(query, expected);
    }
    
    // some unions
    
    @Test
    public void testUnionOfTwoTerms() {
        String query = "FIRST_NAME == 'bob' || FIRST_NAME == 'eve'";
        Map<String,Set<String>> expected = new HashMap<>();
        expected.put("FIRST_NAME == 'bob'", bobUids);
        expected.put("FIRST_NAME == 'eve'", eveUids);
        test(query, expected);
    }
    
    // some complex intersections
    
    @Test
    public void testIntersectAllTerms() {
        String query = "FIRST_NAME == 'alice' && FIRST_NAME == 'bob' && FIRST_NAME == 'eve' && FIRST_NAME == 'oberon' && FIRST_NAME =~ 'e.*'";
        Map<String,Set<String>> expected = new HashMap<>();
        expected.put("FIRST_NAME == 'alice'", aliceUids);
        expected.put("FIRST_NAME == 'bob'", bobUids);
        expected.put("FIRST_NAME == 'eve'", eveUids);
        expected.put("FIRST_NAME == 'oberon'", oberonUids);
        expected.put("FIRST_NAME =~ 'e.*'", Sets.union(eveUids, extraUids));
        test(query, expected);
    }
    
    // A && (C || D)
    @Test
    public void testIntersectionOfTermAndNestedUnion() {
        String query = "FIRST_NAME == 'alice' && (FIRST_NAME == 'eve' || FIRST_NAME == 'oberon')";
        Map<String,Set<String>> expected = new HashMap<>();
        expected.put("FIRST_NAME == 'alice'", aliceUids);
        expected.put("FIRST_NAME == 'eve'", eveUids);
        expected.put("FIRST_NAME == 'oberon'", oberonUids);
        test(query, expected);
    }
    
    // (A || B) && (C || D)
    @Test
    public void testIntersectionOfTwoNestedUnions() {
        String query = "(FIRST_NAME == 'alice' || FIRST_NAME == 'bob') && (FIRST_NAME == 'eve' || FIRST_NAME == 'oberon')";
        Map<String,Set<String>> expected = new HashMap<>();
        expected.put("FIRST_NAME == 'alice'", aliceUids);
        expected.put("FIRST_NAME == 'bob'", bobUids);
        expected.put("FIRST_NAME == 'eve'", eveUids);
        expected.put("FIRST_NAME == 'oberon'", oberonUids);
        test(query, expected);
    }
    
    // some complex unions
    
    @Test
    public void testUnionAllTerms() {
        String query = "FIRST_NAME == 'alice' || FIRST_NAME == 'bob' || FIRST_NAME == 'eve' || FIRST_NAME == 'oberon' || FIRST_NAME =~ 'e.*'";
        Map<String,Set<String>> expected = new HashMap<>();
        expected.put("FIRST_NAME == 'alice'", aliceUids);
        expected.put("FIRST_NAME == 'bob'", bobUids);
        expected.put("FIRST_NAME == 'eve'", eveUids);
        expected.put("FIRST_NAME == 'oberon'", oberonUids);
        expected.put("FIRST_NAME =~ 'e.*'", Sets.union(eveUids, extraUids));
        test(query, expected);
    }
    
    // A || (C && D)
    @Test
    public void testUnionOfTermAndNestedIntersection() {
        String query = "FIRST_NAME == 'alice' || (FIRST_NAME == 'eve' || FIRST_NAME == 'oberon')";
        Map<String,Set<String>> expected = new HashMap<>();
        expected.put("FIRST_NAME == 'alice'", aliceUids);
        expected.put("FIRST_NAME == 'eve'", eveUids);
        expected.put("FIRST_NAME == 'oberon'", oberonUids);
        test(query, expected);
    }
    
    // (A && B) || (C && D)
    @Test
    public void testUnionOfTwoNestedIntersections() {
        String query = "(FIRST_NAME == 'alice' && FIRST_NAME == 'bob') || (FIRST_NAME == 'eve' || FIRST_NAME == 'oberon')";
        Map<String,Set<String>> expected = new HashMap<>();
        expected.put("FIRST_NAME == 'alice'", aliceUids);
        expected.put("FIRST_NAME == 'bob'", bobUids);
        expected.put("FIRST_NAME == 'eve'", eveUids);
        expected.put("FIRST_NAME == 'oberon'", oberonUids);
        test(query, expected);
    }
    
    @Test
    public void testLargeOr() {
        List<String> terms = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            terms.add("FIRST_NAME == 'bob'");
            terms.add("FIRST_NAME == 'eve'");
        }
        String query = Joiner.on(" || ").join(terms);
        
        Map<String,Set<String>> expected = new HashMap<>();
        expected.put("FIRST_NAME == 'bob'", bobUids);
        expected.put("FIRST_NAME == 'eve'", eveUids);
        test(query, expected);
    }
    
    // test some functions
    
    @Test
    public void testContentPhraseFunction() {
        String query = "content:phrase(termOffsetMap,'TOK') && (TOK == 'brute' && TOK == 'forced')";
        Map<String,Set<String>> expected = new HashMap<>();
        expected.put("TOK == 'brute'", Collections.singleton(util.getUid4()));
        expected.put("TOK == 'forced'", Collections.singleton(util.getUid4()));
        test(query, expected);
    }
    
    @Test
    public void testContentAdjacentFunction() {
        
    }
    
    @Test
    public void testContentWithinFunction() {
        
    }
    
    // various bounded ranges
    
    @Ignore
    @Test
    public void testBoundedRangeCase_numeric() {
        String min = numberType.normalize("8");
        String max = numberType.normalize("9");
        String query = "((_Bounded_ = true) && (MSG_SIZE > '" + min + "' && MSG_SIZE < '" + max + "'))";
        Map<String,Set<String>> expected = new HashMap<>();
        expected.put(query, Collections.singleton(util.getUid3()));
        test(query, expected);
    }
    
    @Test
    public void testBoundedRangeCase_character() {
        // this range will hit on 'bob'
        String query = "((_Bounded_ = true) && (FIRST_NAME >= 'ba' && FIRST_NAME <= 'bz'))";
        Map<String,Set<String>> expected = new HashMap<>();
        expected.put("(_Bounded_ = true) && (FIRST_NAME >= 'ba' && FIRST_NAME <= 'bz')", bobUids);
        test(query, expected);
    }
    
    @Test
    public void testEventOnlyScan() {
        // event only field does not exist in the field index
        String query = "EVENT_ONLY == 'abc'";
        test(query, new HashMap<>());
    }
    
    @Test
    public void testIntersectionOfEventOnlyWithIndexedTerm() {
        String query = "FIRST_NAME == 'alice' && EVENT_ONLY == 'abc'";
        Map<String,Set<String>> expected = new HashMap<>();
        expected.put("FIRST_NAME == 'alice'", aliceUids);
        test(query, expected);
    }
    
    @Test
    public void testUnionOfEventOnlyWithIndexedTerm() {
        String query = "FIRST_NAME == 'alice' || EVENT_ONLY == 'abc'";
        Map<String,Set<String>> expected = new HashMap<>();
        expected.put("FIRST_NAME == 'alice'", aliceUids);
        test(query, expected);
    }
    
    protected void test(String query, Map<String,Set<String>> expected) {
        FiScanner scan = new FiScanner("scanId", util.getConnector(), TableName.SHARD, util.getAuths(), util.getMetadataHelper());
        test(scan, query, expected);
    }
    
    protected void test(FiScannerStrategy scan, String query, Map<String,Set<String>> expected) {
        try {
            // extract query nodes
            ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery(query);
            Set<JexlNode> terms = QueryTermVisitor.parse(script);
            Map<String,Set<String>> nodesToUids = scan.scanFieldIndexForTerms("20201212_0", terms, Sets.newHashSet("TOK", "FIRST_NAME", "MSG_SIZE"));
            
            assertEquals("Expected node keys did not match", expected.keySet(), nodesToUids.keySet());
            for (String key : expected.keySet()) {
                Set<String> expectedUids = expected.get(key);
                Set<String> dtAndUids = new HashSet<>();
                for (String dtAndUid : nodesToUids.get(key)) {
                    int index = dtAndUid.indexOf('\u0000');
                    dtAndUids.add(dtAndUid.substring(index + 1));
                }
                
                assertEquals("uids did not match for key: " + key, expectedUids, dtAndUids);
            }
        } catch (ParseException e) {
            Assert.fail("Failed to parse query: " + query);
        }
    }
    
}
