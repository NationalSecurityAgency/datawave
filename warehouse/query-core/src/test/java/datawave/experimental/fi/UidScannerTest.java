package datawave.experimental.fi;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.base.Joiner;
import com.google.common.collect.Sets;

import datawave.data.type.NumberType;
import datawave.data.type.Type;
import datawave.experimental.QueryTermVisitor;
import datawave.experimental.util.AccumuloUtil;
import datawave.query.jexl.JexlASTHelper;
import datawave.util.TableName;

/**
 * Test scanners against an in-memory accumulo instance
 */
public class UidScannerTest {

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
        util.create(UidScannerTest.class.getSimpleName());
        util.loadData();
    }

    @Test
    public void testEq() {
        String query = "FIRST_NAME == 'oberon'";
        test(query, oberonUids);
    }

    @Test
    public void testNe() {
        String query = "FIRST_NAME != 'oberon'";
        test(query, oberonUids);
        // technically this should fail because we do not look up top level negations
    }

    @Test
    public void testEr() {
        String query = "FIRST_NAME =~ 'e.*'";
        test(query, Sets.union(eveUids, extraUids));
    }

    @Test
    public void testNr() {
        String query = "FIRST_NAME !~ 'e.*'";
        test(query, Sets.union(eveUids, extraUids));
        // technically this should fail because we do not look up top level negations
    }

    // LT, GT, LE, GE nodes should throw an exception. We only lookup bounded ranges.

    @Ignore
    @Test
    public void testLessThan() {
        String num = numberType.normalize("8");
        String query = "MSG_SIZE < '" + num + "'";
        test(query, Sets.newHashSet(util.getUid0(), util.getUid1()));
    }

    @Ignore
    @Test
    public void testGreaterThan() {
        String num = numberType.normalize("8");
        String query = "MSG_SIZE <= '" + num + "'";
        test(query, Sets.newHashSet(util.getUid0(), util.getUid1(), util.getUid3(), util.getUid4()));
    }

    @Ignore
    @Test
    public void testLessThanEqual() {
        String num = numberType.normalize("8");
        String query = "MSG_SIZE <= '" + num + "'";
        test(query, Sets.newHashSet(util.getUid0(), util.getUid1(), util.getUid3(), util.getUid4()));
    }

    @Ignore
    @Test
    public void testGreaterThanEqual() {
        String num = numberType.normalize("8");
        String query = "MSG_SIZE >= '" + num + "'";
        test(query, Sets.newHashSet(util.getUid2(), util.getUid3(), util.getUid4()));
    }

    // some intersections

    @Test
    public void testIntersectionOfTwoTerms() {
        String query = "FIRST_NAME == 'bob' && FIRST_NAME == 'eve'";
        test(query, Sets.intersection(bobUids, eveUids));
    }

    // some unions

    @Test
    public void testUnionOfTwoTerms() {
        String query = "FIRST_NAME == 'bob' || FIRST_NAME == 'eve'";
        test(query, Sets.union(bobUids, eveUids));
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
        test(query, Collections.emptySet());
    }

    // A && (C || D)
    @Test
    public void testIntersectionOfTermAndNestedUnion() {
        String query = "FIRST_NAME == 'alice' && (FIRST_NAME == 'eve' || FIRST_NAME == 'oberon')";
        Set<String> expected = Sets.union(util.getEveUids(), util.getOberonUids());
        expected = Sets.intersection(util.getAliceUids(), expected);
        test(query, expected);
    }

    // (A || B) && (C || D)
    @Test
    public void testIntersectionOfTwoNestedUnions() {
        String query = "(FIRST_NAME == 'alice' || FIRST_NAME == 'bob') && (FIRST_NAME == 'eve' || FIRST_NAME == 'oberon')";
        Set<String> expected = Sets.intersection(Sets.union(util.getAliceUids(), util.getBobUids()), Sets.union(util.getEveUids(), util.getOberonUids()));
        test(query, expected);
    }

    // some complex unions

    @Test
    public void testUnionAllTerms() {
        String query = "FIRST_NAME == 'alice' || FIRST_NAME == 'bob' || FIRST_NAME == 'eve' || FIRST_NAME == 'oberon' || FIRST_NAME =~ 'e.*'";
        Set<String> expected = Sets.union(util.getAliceUids(), util.getBobUids());
        expected = Sets.union(expected, Sets.union(util.getEveUids(), util.getOberonUids()));
        expected = Sets.union(expected, Sets.union(util.getEveUids(), util.getExtraUids()));
        test(query, expected);
    }

    // A || (C && D)
    @Test
    public void testUnionOfTermAndNestedIntersection() {
        String query = "FIRST_NAME == 'alice' || (FIRST_NAME == 'eve' && FIRST_NAME == 'oberon')";
        Set<String> expected = Sets.union(util.getAliceUids(), Sets.intersection(util.getEveUids(), util.getOberonUids()));
        test(query, expected);
    }

    // (A && B) || (C && D)
    @Test
    public void testUnionOfTwoNestedIntersections() {
        String query = "(FIRST_NAME == 'alice' && FIRST_NAME == 'bob') || (FIRST_NAME == 'eve' && FIRST_NAME == 'oberon')";
        Set<String> expected = Sets.union(Sets.intersection(util.getAliceUids(), util.getBobUids()),
                        Sets.intersection(util.getEveUids(), util.getOberonUids()));
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

        test(query, Sets.union(bobUids, eveUids));
    }

    // test some functions

    @Test
    public void testContentPhraseFunction() {
        String query = "content:phrase(termOffsetMap,'TOK') && (TOK == 'brute' && TOK == 'forced')";
        test(query, Collections.singleton(util.getUid4()));
    }

    @Test
    public void testContentAdjacentFunction() {

    }

    @Test
    public void testContentWithinFunction() {

    }

    // various bounded ranges

    // @Ignore
    @Test
    public void testBoundedRangeCase_numeric() {
        String min = numberType.normalize("8");
        String max = numberType.normalize("9");
        String query = "((_Bounded_ = true) && (MSG_SIZE > '" + min + "' && MSG_SIZE < '" + max + "'))";
        test(query, Set.of(util.getUid2(), util.getUid3(), util.getUid4()));
    }

    @Test
    public void testBoundedRangeCase_character() {
        // this range will hit on 'bob'
        String query = "((_Bounded_ = true) && (FIRST_NAME >= 'ba' && FIRST_NAME <= 'bz'))";
        test(query, bobUids);
    }

    @Test
    public void testEventOnlyScan() {
        // event only field does not exist in the field index
        String query = "EVENT_ONLY == 'abc'";
        test(query, Collections.emptySet());
    }

    @Test
    public void testIntersectionOfEventOnlyWithIndexedTerm() {
        String query = "FIRST_NAME == 'alice' && EVENT_ONLY == 'abc'";
        test(query, aliceUids);
    }

    @Test
    public void testUnionOfEventOnlyWithIndexedTerm() {
        String query = "FIRST_NAME == 'alice' || EVENT_ONLY == 'abc'";
        test(query, aliceUids);
    }

    protected void test(String query, Set<String> expected) {
        SerialUidScanner scan = new SerialUidScanner(util.getClient(), util.getAuths(), TableName.SHARD, "scanId");
        test(scan, query, expected);
    }

    protected void test(UidScanner scanner, String query, Set<String> expected) {
        try {
            // extract query nodes
            ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery(query);
            Set<JexlNode> terms = QueryTermVisitor.parse(script);
            Set<String> scannedUids = scanner.scan(script, "20201212_0", Sets.newHashSet("TOK", "FIRST_NAME", "MSG_SIZE"));
            assertEquals(prependDatatypeToUids(expected), scannedUids);
        } catch (ParseException e) {
            Assert.fail("Failed to parse query: " + query);
        }
    }

    private Set<String> prependDatatypeToUids(Set<String> uids) {
        Set<String> dtUids = new HashSet<>();
        for (String uid : uids) {
            dtUids.add("dt\0" + uid);
        }
        return dtUids;
    }

}
