package datawave.query.testframework;

import datawave.data.normalizer.Normalizer;
import datawave.data.normalizer.NumberNormalizer;
import datawave.query.jexl.JexlASTHelper;
import org.apache.commons.jexl2.Expression;
import org.apache.commons.jexl2.ExpressionImpl;
import org.apache.commons.jexl2.JexlContext;
import org.apache.commons.jexl2.JexlEngine;
import org.apache.commons.jexl2.MapContext;
import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTIdentifier;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ASTNRNode;
import org.apache.commons.jexl2.parser.ASTNumberLiteral;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.ASTReferenceExpression;
import org.apache.commons.jexl2.parser.ASTStringLiteral;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.SimpleNode;
import org.apache.commons.jexl2.parser.TokenMgrError;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Provides for the parsing and execution of a Jexl query string for test execution only. The {@link #evaluate()} method should produce the same results that
 * are produced from the production code base.
 * <p>
 * </P>
 * <b>Current Limitations</b><br>
 * <ul>
 * <li>Differences in the manner in which the Datawave Interpreter and the Jexl Interpreter work may result in the results. One difference in with multivalue
 * fields where the size() method will return different results.</li>
 * <li>The not equal (!=) and not regex(!~) relationships do not work properly for multivalue fields that contain multiple values. A single value for a
 * multivalue field will work correctly. Either create a different expect query or hardcode the expected results.</li>
 * <li>Only one data manager is available for processing results.</li>
 * <li>For any query that contains embedded "()", this will not be evaluate properly. The ExpressionImpl object does not return the same variable list. This
 * variable list is used to set the context and thus the evaluation fails. Here is an example:<br>
 * a == 'b' => this works properly<br>
 * (a == 'b') => this works properly<br>
 * ((a == 'b')) => this will fail; the variable "a" is not returned from the getVariables() method
 * <p>
 * The problem appears to be a bug in the Jexl code in the getVariables() method. This code traverses the script, extracting all variables from the tree. This
 * could be done here but this seems to be too much effort at this time.
 * </p>
 * </li>
 * </ul>
 */
public class QueryJexl {
    
    private static final Logger log = Logger.getLogger(QueryJexl.class);
    
    private static final JexlEngine jEngine = new JexlEngine();
    private final RawDataManager manager;
    private final Date startDate;
    private final Date endDate;
    private final Expression jExpr;
    
    /**
     * @param queryStr
     *            query for test
     * @param dataManager
     *            manager of raw data
     * @param start
     *            start date
     * @param end
     *            end date
     */
    public QueryJexl(final String queryStr, final RawDataManager dataManager, final Date start, final Date end) {
        this.manager = dataManager;
        this.startDate = start;
        this.endDate = end;
        
        log.debug("query[" + queryStr + "] start(" + this.startDate + ") + end(" + this.endDate + ")");
        this.jExpr = createNormalizedExpression(queryStr);
    }
    
    /**
     * Performs the evaluation of a Jexl query.
     * 
     * @return matching entries for the current manager
     */
    public Set<Map<String,String>> evaluate() {
        final JexlContext jCtx = new MapContext();
        final Set<Map<String,String>> response = new HashSet<>();
        Iterator<Map<String,String>> entries = this.manager.rangeData(this.startDate, this.endDate);
        
        // one can either normalize the query string or match the variables in the expression
        // for simplicity it is easier just to match the variables to the correct field
        final ExpressionImpl exp = (ExpressionImpl) this.jExpr;
        // see limitations in the javadoc
        // an expression that has "((...))" will not return the correct variables
        final Set<List<String>> vars = exp.getVariables();
        
        while (entries.hasNext()) {
            Map<String,String> mapping = entries.next();
            for (final Map.Entry<String,String> entry : mapping.entrySet()) {
                for (final List<String> queryVars : vars) {
                    for (final String v : queryVars) {
                        if (v.equalsIgnoreCase(entry.getKey())) {
                            jCtx.set(entry.getKey(), entry.getValue());
                        }
                    }
                }
            }
            Boolean match = (Boolean) jExpr.evaluate(jCtx);
            if (match) {
                response.add(mapping);
            }
        }
        
        if (log.isTraceEnabled()) {
            log.trace("    ======  expected response data  ======");
            for (Map<String,String> data : response) {
                for (Map.Entry<String,String> entry : data.entrySet()) {
                    log.debug("key(" + entry.getKey() + ") value(" + entry.getValue() + ")");
                }
            }
        }
        
        return response;
    }
    
    // =================================
    // private methods
    private Expression createNormalizedExpression(final String query) {
        try {
            ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
            Deque<SimpleNode> nodes = new LinkedList<>();
            normalizeScript(script, nodes);
            return new NormalizedExpression(jEngine, query, script);
        } catch (TokenMgrError | org.apache.commons.jexl2.parser.ParseException pe) {
            throw new AssertionError(pe);
        }
    }
    
    /**
     * Normalizes all of the {@link ASTIdentifier}, {@link ASTStringLiteral}, and {@link ASTNumberLiteral} entries that exist in a {@link ASTJexlScript}.
     *
     * @param node
     *            current node for normalization
     * @param nodes
     *            queue of nodes used as a stack for normalization
     */
    private void normalizeScript(final SimpleNode node, final Deque<SimpleNode> nodes) {
        int num = node.jjtGetNumChildren();
        for (int n = 0; n < num; n++) {
            SimpleNode child = node.jjtGetChild(n);
            if (0 < child.jjtGetNumChildren()) {
                if (!(child instanceof ASTReference || child instanceof ASTReferenceExpression)) {
                    // this may be an op node (e.g. ASTEQNode) or some other node
                    nodes.addFirst(child);
                }
                // recursive processing of child nodes
                normalizeScript(child, nodes);
            } else {
                // the tree is expected to have an op node that includes 2 reference nodes
                // one node will contain the identifier, the other will contain the literal
                // the order for the identifier and literal nodes may vary
                if (child instanceof ASTIdentifier) {
                    ASTIdentifier id = (ASTIdentifier) child;
                    // change identifier to lower case
                    id.image = id.image.toLowerCase();
                    
                    // check for string or numeric literal on node stack
                    SimpleNode entry = nodes.removeFirst();
                    Assert.assertNotNull(entry);
                    if (entry instanceof ASTStringLiteral || entry instanceof ASTNumberLiteral) {
                        // exp is "value OP field"
                        // remove op node
                        SimpleNode opNode = nodes.removeFirst();
                        normalizeField(id.image, (JexlNode) entry, opNode);
                    } else {
                        // push entry back on stack and add identifier
                        nodes.addFirst(entry);
                        nodes.addFirst(child);
                    }
                } else if (child instanceof ASTStringLiteral || child instanceof ASTNumberLiteral) {
                    // check for identifier on node stack
                    SimpleNode entry = nodes.removeFirst();
                    Assert.assertNotNull(entry);
                    if (entry instanceof ASTIdentifier) {
                        // exp is "field OP value"
                        SimpleNode opNode = nodes.removeFirst();
                        normalizeField(((JexlNode) entry).image, (JexlNode) child, opNode);
                    } else {
                        // push entry back on stack and add literal
                        nodes.addFirst(entry);
                        nodes.addFirst(child);
                    }
                }
            }
        }
    }
    
    private void normalizeField(final String field, final JexlNode value, final SimpleNode opNode) {
        Assert.assertNotNull(opNode);
        Normalizer<?> norm = this.manager.getNormalizer(field);
        if (null != norm) {
            if (norm instanceof NumberNormalizer) {
                try {
                    Integer.parseInt(value.image);
                } catch (NumberFormatException nfe) {
                    throw new AssertionError("invalid integer(" + value.image + ")", nfe);
                }
            } else {
                // normalize all other values
                // check for regex nodes
                if (opNode instanceof ASTERNode || opNode instanceof ASTNRNode) {
                    value.image = norm.normalizeRegex(value.image);
                } else {
                    value.image = norm.normalize(value.image);
                }
            }
        }
    }
    
    /**
     * Test class used to create an expression from the normalized JEXL script. <br>
     * NOTE: The query string wil not be changed but the script will represent the normalized expression.
     */
    private static class NormalizedExpression extends ExpressionImpl {
        NormalizedExpression(JexlEngine engine, String query, ASTJexlScript script) {
            super(engine, query, script);
        }
        
    }
    
    // =============================
    // classes for testing purposes
    public static class QueryJexlTest {
        
        private static final int TEST_ENTRIES = 4;
        private static final String testDate = "20151010";
        private final RawDataManager manager = new TestManager();
        private Date date;
        
        public QueryJexlTest() {
            try {
                this.date = DataTypeHadoopConfig.YMD_DateFormat.parse(testDate);
            } catch (ParseException pe) {
                Assert.fail("invalid test date (" + testDate + ")");
            }
        }
        
        @Test
        public void testSimple() {
            log.info("valid simple jexl parser/evaluation");
            for (int n = 1; n <= TEST_ENTRIES; n++) {
                String val = "hOME-" + n;
                String q = "HOME == '" + val + "' or xxx == 'xxx'";
                QueryJexl p = new QueryJexl(q, manager, date, date);
                Set<Map<String,String>> resp = p.evaluate();
                Assert.assertEquals(1, resp.size());
                Map<String,String> entry = resp.iterator().next();
                Assert.assertEquals(val.toLowerCase(), entry.get(TestHeader.home.name()));
                Assert.assertEquals("away-" + n, entry.get(TestHeader.away.name()));
                Assert.assertEquals("" + n, entry.get(TestHeader.num.name()));
            }
        }
        
        @Test
        public void testRegex() {
            log.info("valid regex jexl parser/evaluation");
            for (int n = 1; n <= TEST_ENTRIES; n++) {
                String val = "Ho.*";
                String q = "HOME =~ '" + val + "'";
                QueryJexl p = new QueryJexl(q, manager, date, date);
                Set<Map<String,String>> resp = p.evaluate();
                Assert.assertEquals(TEST_ENTRIES, resp.size());
            }
        }
        
        @Test
        public void testNotRegex() {
            log.info("valid NOT regex jexl parser/evaluation");
            for (int n = 1; n <= TEST_ENTRIES; n++) {
                String val = ".*E-" + n;
                String q = "HOME !~ '" + val + "'";
                QueryJexl p = new QueryJexl(q, manager, date, date);
                Set<Map<String,String>> resp = p.evaluate();
                Assert.assertEquals(TEST_ENTRIES - 1, resp.size());
                for (Map<String,String> entry : resp) {
                    Assert.assertNotEquals("home-" + n, entry.get(TestHeader.home.name()));
                    Assert.assertNotEquals("away-" + n, entry.get(TestHeader.away.name()));
                    Assert.assertNotEquals("" + n, entry.get(TestHeader.num.name()));
                }
            }
        }
        
        @Test
        public void testAnd() {
            log.info("jexl AND test");
            String q = "HOME == 'hoME-2' and away == 'aWAy-2'";
            QueryJexl p = new QueryJexl(q, manager, date, date);
            Set<Map<String,String>> resp = p.evaluate();
            Assert.assertEquals(1, resp.size());
            Map<String,String> entry = resp.iterator().next();
            Assert.assertEquals("away-2", entry.get(TestHeader.away.name()));
        }
        
        @Test
        public void testOr() {
            log.info("jexl OR test");
            String q = "hOmE == 'hoMe-2' or Home == 'HOME-2' or Away == 'aWAy-3'";
            QueryJexl p = new QueryJexl(q, manager, date, date);
            Set<Map<String,String>> resp = p.evaluate();
            Assert.assertEquals(2, resp.size());
            for (Map<String,String> entry : resp) {
                String away = entry.get(TestHeader.away.name());
                Assert.assertTrue(away.equals("away-2") || away.equals("away-3"));
            }
        }
        
        @Test
        public void testNoMatch() {
            log.info("jexl OR/AND test");
            String q = "(hOmE == 'hoMe-2' or Home == 'HOME-2') and (Away == 'aWAy-3' or NuM == 1)";
            QueryJexl p = new QueryJexl(q, manager, date, date);
            Set<Map<String,String>> resp = p.evaluate();
            Assert.assertEquals(0, resp.size());
            
        }
        
        @Test
        public void testCompound() {
            log.info("jexl compound test");
            String q = "(hOmE == 'hoMe-2' or Home == 'HOME-3' or 'hOME-1' == HoMe) and (Away == 'aWAy-3' or NuM == 1)";
            QueryJexl p = new QueryJexl(q, manager, date, date);
            Set<Map<String,String>> resp = p.evaluate();
            Assert.assertEquals(2, resp.size());
            for (Map<String,String> entry : resp) {
                String away = entry.get(TestHeader.away.name());
                Assert.assertTrue(away.equals("away-3") || away.equals("away-1"));
            }
        }
        
        @Test
        public void testAny() {
            for (int n = 1; n <= TEST_ENTRIES; n++) {
                String phrase = " == 'away-" + n + "'";
                String q = this.manager.convertAnyField(phrase);
                QueryJexl p = new QueryJexl(q, manager, date, date);
                Set<Map<String,String>> resp = p.evaluate();
                Assert.assertEquals(1, resp.size());
                Map<String,String> entry = resp.iterator().next();
                Assert.assertEquals("" + n, entry.get(TestHeader.num.name()));
            }
        }
        
        @Test
        public void testAnyNumeric() {
            for (int n = 1; n <= TEST_ENTRIES; n++) {
                String phrase = " == '" + n + "'";
                String q = this.manager.convertAnyField(phrase);
                QueryJexl p = new QueryJexl(q, manager, date, date);
                Set<Map<String,String>> resp = p.evaluate();
                Assert.assertEquals(1, resp.size());
                Map<String,String> entry = resp.iterator().next();
                Assert.assertEquals("" + n, entry.get(TestHeader.num.name()));
            }
        }
        
        @Test
        public void testNumeric() {
            log.info("jexl numeric test");
            for (int n = 1; n < TEST_ENTRIES; n++) {
                String q = "" + n + " == nUm or NUM == " + n;
                QueryJexl p = new QueryJexl(q, manager, date, date);
                Set<Map<String,String>> resp = p.evaluate();
                Assert.assertEquals(1, resp.size());
                Map<String,String> entry = resp.iterator().next();
                Assert.assertEquals("" + n, entry.get(TestHeader.num.name()));
            }
        }
        
        @Test
        public void testOrAnd() {
            log.info("jexl OR/AND test");
            String q = "HOME == 'hOme-2' or ('homE-3' == Home and xXXx == 'xXx')";
            QueryJexl p = new QueryJexl(q, manager, date, date);
            Set<Map<String,String>> resp = p.evaluate();
            Assert.assertEquals(1, resp.size());
            Map<String,String> entry = resp.iterator().next();
            Assert.assertEquals("away-2", entry.get("away"));
        }
    }
    
    private enum TestHeader {
        date(Normalizer.LC_NO_DIACRITICS_NORMALIZER), home(Normalizer.LC_NO_DIACRITICS_NORMALIZER), away(Normalizer.LC_NO_DIACRITICS_NORMALIZER), num(
                        Normalizer.NUMBER_NORMALIZER);
        
        private final Normalizer<?> normalizer;
        
        TestHeader(Normalizer<?> norm) {
            this.normalizer = norm;
        }
        
        static List<String> headers() {
            final List<String> vals = new ArrayList<>();
            for (final TestHeader header : TestHeader.values()) {
                vals.add(header.name());
            }
            return vals;
        }
        
        Normalizer getNormalizer() {
            return this.normalizer;
        }
    }
    
    private static class TestManager extends BaseTestManager {
        
        private static final String dataType = "sample";
        
        private TestManager() {
            super("home", "date", TestHeader.headers());
            createData();
            Set<String> anyIndexes = new HashSet<>();
            anyIndexes.add(TestHeader.away.name());
            anyIndexes.add(TestHeader.home.name());
            anyIndexes.add(TestHeader.num.name());
            this.rawDataIndex.put(dataType, anyIndexes);
        }
        
        private void createData() {
            String[] vals = new String[TestHeader.values().length];
            final Set<RawData> data = new HashSet<>();
            for (int n = 1; n <= QueryJexlTest.TEST_ENTRIES; n++) {
                vals[0] = QueryJexlTest.testDate;
                vals[1] = "home-" + n;
                vals[2] = "away-" + n;
                vals[3] = "" + n;
                final RawData raw = new TestRawData(vals);
                data.add(raw);
            }
            this.rawData.put(dataType, data);
            
            this.metadata = TestRawData.metadata;
        }
    }
    
    static class TestRawData extends BaseRawData {
        
        static final Map<String,RawMetaData> metadata = new HashMap<>();
        
        static {
            for (final TestHeader field : TestHeader.values()) {
                final RawMetaData meta = new RawMetaData(field.name(), field.getNormalizer(), true);
                metadata.put(field.name(), meta);
            }
        }
        
        TestRawData(final String fields[]) {
            super(TestManager.dataType, fields, TestHeader.headers(), metadata);
        }
        
        @Override
        public boolean containsField(final String field) {
            return (TestHeader.headers()).contains(field);
        }
    }
}
