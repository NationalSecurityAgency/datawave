package datawave.query.testframework;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.URI;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * Helper class for parsing a query. Currently, the parser will only work properly on JEXL syntax. Queries that use functions or Lucene must be converted to an
 * equivalent. An invalid query format will be rejected.
 */
public class QueryParserHelper {
    
    private static final Logger log = LoggerFactory.getLogger(QueryParserHelper.class);
    
    // valid states for parsing a JEXL query
    private enum ParseState {
        Field, Op, Value, ValueAdd, LogicalOp;
    }
    
    private static final String LEFT_PAREN = "(";
    private static final String RIGHT_PAREN = ")";
    private static final int QUOTE = '\'';
    private static final String ESCAPE_QUOTE_END = "\' ";
    private static final String QUOTE_STR = "'";
    
    /**
     * Defines a simple query execution. The op field defines the logical operation that occurs between this query result and the next result. Child actions are
     * to be completed as a complete unit before executing the logical operation with the parent. Entries may exist that only have the {@link #op} field
     * populated. This is used to indicate the logical operation between two consecutive entries.
     */
    static class QueryEntry {
        QueryAction action;
        LinkedList<QueryEntry> childActions;
        QueryEntry parent;
        LogicalOp op;
        
        QueryEntry() {
            this.childActions = new LinkedList<>();
        }
        
        @Override
        public String toString() {
            String a = (null == action) ? "null" : action.toString();
            // handle special condition where parent is not null
            String po = (null == parent) ? "null" : parent.op.toString();
            String o = (null == op) ? "null" : op.toString();
            return "QueryEntry{" + "action=" + a + ", childActions=" + childActions + ", parent=" + po + ", op=" + o + '}';
        }
    }
    
    /**
     * Defines the JEXL operators tokens for logical operations (e.g. expression AND expression).
     */
    private enum LogicalOp {
        AND("AND"), AND_SYM("&&"), OR("OR"), OR_SYM("||");
        
        private final String token;
        
        LogicalOp(String op) {
            this.token = op;
        }
        
        static LogicalOp getOp(String op) {
            for (final LogicalOp val : LogicalOp.values()) {
                if (val.token.equalsIgnoreCase(op)) {
                    return val;
                }
            }
            
            throw new AssertionError("logical operator (" + op + ") is not a known operand");
        }
    }
    
    // contains execution tree of simple query entries
    private final LinkedList<QueryEntry> execTree;
    private final IRawDataManager manager;
    private final Date startDate;
    private final Date endDate;
    
    public QueryParserHelper(final String queryStr, final IRawDataManager resultsManager, final Date start, final Date end) {
        this.manager = resultsManager;
        this.startDate = start;
        this.endDate = end;
        this.execTree = parse(queryStr);
    }
    
    /**
     * Executes the query string over the specified data set.
     *
     * @return entries which match the specified query
     */
    public Set<IRawData> findMatchers() {
        return runQuery(this.execTree);
    }
    
    private Set<IRawData> runQuery(final LinkedList<QueryEntry> entries) {
        Set<IRawData> matchers = new HashSet<>();
        QueryEntry previousEntry = null;
        
        for (final QueryEntry entry : entries) {
            if (null == entry.action) {
                // check for logical op with child entries
                if (!entry.childActions.isEmpty()) {
                    // perform child actions recursively
                    final Set<IRawData> childResults = runQuery(entry.childActions);
                    matchers = runLogicalOp(matchers, childResults, entry.op);
                }
            } else {
                Set<IRawData> result = this.manager.findMatchers(entry.action, this.startDate, this.endDate);
                if (!entry.childActions.isEmpty()) {
                    // perform child actions recursively
                    final Set<IRawData> childResults = runQuery(entry.childActions);
                    result = runLogicalOp(result, childResults, entry.op);
                }
                
                // previous entry has the logical op
                if (null != previousEntry) {
                    matchers = runLogicalOp(matchers, result, previousEntry.op);
                } else {
                    // first entry in linked list
                    matchers = result;
                }
            }
            previousEntry = entry;
        }
        
        return matchers;
    }
    
    private Set<IRawData> runLogicalOp(final Set<IRawData> left, final Set<IRawData> right, LogicalOp op) {
        final Set<IRawData> result = new HashSet<>(left);
        switch (op) {
            case AND:
            case AND_SYM:
                for (final IRawData entry : left) {
                    if (!right.contains(entry)) {
                        result.remove(entry);
                    }
                }
                break;
            case OR:
            case OR_SYM:
                result.addAll(right);
                break;
        }
        
        return result;
    }
    
    /**
     * Parses a query string to extract the inner query string. It will find the matching left and right parens and return either a simple query, multiple
     * simple queries, or another compound query that contains additional embedded query strings.
     *
     * @param qs
     *            embedded query string
     * @return simple or compound query string
     * @throws AssertionError
     *             when a paren mismatch is detected
     */
    private String parseInnerQuery(final String qs) {
        int leftLoc = qs.indexOf(LEFT_PAREN);
        int rightLoc = qs.indexOf(RIGHT_PAREN);
        int lastRight = rightLoc;
        int firstLeft = leftLoc;
        // loop for embedded "(...)" pair
        while (0 <= leftLoc && leftLoc < rightLoc) {
            leftLoc = qs.indexOf(LEFT_PAREN, leftLoc + 1);
            if (leftLoc < rightLoc) {
                // found embedded "(...)"
                lastRight = rightLoc;
                rightLoc = qs.indexOf(RIGHT_PAREN, rightLoc + 1);
            }
        }
        
        Assert.assertTrue("missing match paren to query string(" + qs + ")", lastRight > 0);
        
        return qs.substring(firstLeft + 1, lastRight);
    }
    
    /**
     * Parses a query string into an executable tree. The query is parsed into one or simple queries into a hierarchical tree for execution.
     *
     * @param query
     *            query string for parsing
     * @return execution tree for query
     */
    private LinkedList<QueryEntry> parse(final String query) {
        log.debug("---------  query({})  ---------", query);
        LinkedList<QueryEntry> actions = new LinkedList<>();
        QueryEntry entry = null;
        String str = query.trim();
        ParseState nextState = ParseState.Field;
        while (!str.isEmpty()) {
            if (str.startsWith(LEFT_PAREN)) {
                // extract inner query string within parens '(...)'
                String q = parseInnerQuery(str);
                LinkedList<QueryEntry> sub = parse(q);
                if (actions.isEmpty()) {
                    // when query string starts with '('
                    actions = sub;
                } else {
                    entry.childActions = sub;
                    // link child to parent
                    for (QueryEntry qe : sub) {
                        qe.parent = entry;
                    }
                }
                // completion of '( ... )' marks end of entry
                entry = null;
                int loc = str.indexOf(q);
                str = str.substring(loc + q.length());
                loc = str.indexOf(RIGHT_PAREN);
                str = str.substring(loc + 1).trim();
                nextState = ParseState.LogicalOp;
            } else {
                Type fieldType = null;
                int loc = 0;
                String field = "";
                String op = "";
                StringBuilder value = new StringBuilder();
                
                while (ParseState.LogicalOp != nextState) {
                    loc = str.indexOf(' ');
                    switch (nextState) {
                        case Field:
                            Assert.assertTrue(0 < loc);
                            nextState = ParseState.Op;
                            fieldType = this.manager.getFieldType(str.substring(0, loc));
                            field = str.substring(0, loc);
                            str = str.substring(loc).trim();
                            break;
                        case Op:
                            Assert.assertTrue(0 < loc);
                            nextState = ParseState.Value;
                            op = str.substring(0, loc);
                            str = str.substring(loc).trim();
                            break;
                        case Value:
                            if (0 < loc) {
                                final String val = str.substring(0, loc);
                                if (QUOTE == val.charAt(0)) {
                                    if (val.endsWith(ESCAPE_QUOTE_END)) {
                                        nextState = ParseState.ValueAdd;
                                    } else if (val.endsWith(QUOTE_STR)) {
                                        nextState = ParseState.LogicalOp;
                                    } else {
                                        nextState = ParseState.ValueAdd;
                                    }
                                } else {
                                    // numeric value
                                    nextState = ParseState.LogicalOp;
                                }
                                
                                value.append(val);
                                str = str.substring(loc).trim();
                            } else {
                                value.append(str);
                                str = "";
                                nextState = ParseState.LogicalOp;
                            }
                            break;
                        case ValueAdd:
                            if (0 < loc) {
                                final String val = str.substring(0, loc);
                                value.append(" ").append(val);
                                str = str.substring(loc).trim();
                                if (!val.endsWith(ESCAPE_QUOTE_END) && val.endsWith(QUOTE_STR)) {
                                    nextState = ParseState.LogicalOp;
                                }
                            } else {
                                value.append(" ").append(str);
                                str = "";
                                nextState = ParseState.LogicalOp;
                            }
                            break;
                    }
                }
                
                // create new base entry
                entry = new QueryEntry();
                entry.action = new QueryAction(field, op, value.toString(), fieldType, true);
            }
            
            // an entry with just an op may exist for certain conditions
            // e.g. a == b and (x > y and z < y) or b > q
            if (!str.isEmpty()) {
                if (null == entry) {
                    entry = new QueryEntry();
                }
                int loc = str.indexOf(' ');
                entry.op = LogicalOp.getOp(str.substring(0, loc));
                str = str.substring(loc + 1).trim();
                nextState = ParseState.Field;
            }
            
            if (null != entry) {
                actions.add(entry);
            }
        }
        return actions;
    }
    
    // ===================================================
    // test case section - helpful for any modifications
    public static class TestParser {
        @Test
        public void test() {
            Date date = new Date();
            final IRawDataManager manager = new TestManager();
            QueryParserHelper.QueryEntry qe, cqe;
            String q = "axx == 'aads'";
            QueryParserHelper p = new QueryParserHelper(q, manager, date, date);
            Assert.assertEquals(1, p.execTree.size());
            for (QueryEntry entry : p.execTree) {
                Assert.assertNotNull(entry.action);
                Assert.assertNotNull(entry.childActions);
                Assert.assertTrue(entry.childActions.isEmpty());
                Assert.assertNull(entry.parent);
                Assert.assertNull(entry.op);
            }
            q = "  ab  ==  'bccc'    &&  ccc  ==   'xc' ";
            p = new QueryParserHelper(q, manager, date, date);
            Assert.assertEquals(2, p.execTree.size());
            for (QueryEntry entry : p.execTree) {
                Assert.assertNotNull(entry.action);
                Assert.assertNotNull(entry.childActions);
                Assert.assertTrue(entry.childActions.isEmpty());
                Assert.assertNull(entry.parent);
            }
            
            q = "  ab  ==  'bccc'    &&  (ccc  ==   'xc'  or xx == 'wdd' )";
            p = new QueryParserHelper(q, manager, date, date);
            Assert.assertEquals(1, p.execTree.size());
            for (QueryEntry entry : p.execTree) {
                Assert.assertNotNull(entry.action);
                Assert.assertNotNull(entry.childActions);
                Assert.assertEquals(2, entry.childActions.size());
                Assert.assertNull(entry.parent);
            }
            
            q = "(a > 'b') && (c < 'd') || a != 'q'";
            p = new QueryParserHelper(q, manager, date, date);
            Assert.assertEquals(4, p.execTree.size());
            qe = p.execTree.remove();
            Assert.assertNotNull(qe.action);
            Assert.assertNotNull(qe.childActions);
            Assert.assertNull(qe.op);
            Assert.assertTrue(qe.childActions.isEmpty());
            qe = p.execTree.remove();
            Assert.assertNull(qe.action);
            Assert.assertNotNull(qe.childActions);
            Assert.assertEquals(1, qe.childActions.size());
            Assert.assertNotNull(qe.op);
            cqe = qe.childActions.getFirst();
            Assert.assertNotNull(cqe.action);
            Assert.assertNotNull(cqe.childActions);
            Assert.assertTrue(cqe.childActions.isEmpty());
            Assert.assertNull(cqe.op);
            qe = p.execTree.remove();
            Assert.assertNull(qe.action);
            Assert.assertNotNull(qe.childActions);
            Assert.assertTrue(qe.childActions.isEmpty());
            Assert.assertNotNull(qe.op);
            qe = p.execTree.remove();
            Assert.assertNotNull(qe.action);
            Assert.assertNotNull(qe.childActions);
            Assert.assertNull(qe.op);
            Assert.assertTrue(qe.childActions.isEmpty());
            
            q = "(a > 'b') && (c < 'd')";
            p = new QueryParserHelper(q, manager, date, date);
            Assert.assertEquals(2, p.execTree.size());
            qe = p.execTree.remove();
            Assert.assertNotNull(qe.action);
            Assert.assertNotNull(qe.childActions);
            Assert.assertNull(qe.op);
            Assert.assertTrue(qe.childActions.isEmpty());
            qe = p.execTree.remove();
            Assert.assertNull(qe.action);
            Assert.assertNotNull(qe.childActions);
            Assert.assertEquals(1, qe.childActions.size());
            Assert.assertNotNull(qe.op);
            cqe = qe.childActions.getFirst();
            Assert.assertNotNull(cqe.action);
            Assert.assertNotNull(cqe.childActions);
            Assert.assertTrue(cqe.childActions.isEmpty());
            Assert.assertNull(cqe.op);
            
            q = "  ab  ==  'bccc'    &&  (ccc  ==   'xc'  or xx == 'wdd') && x == '123'";
            p = new QueryParserHelper(q, manager, date, date);
            Assert.assertEquals(3, p.execTree.size());
            qe = p.execTree.getFirst();
            Assert.assertNotNull(qe.action);
            Assert.assertNotNull(qe.childActions);
            Assert.assertEquals(2, qe.childActions.size());
            Assert.assertNull(qe.parent);
            qe = p.execTree.getLast();
            Assert.assertNotNull(qe.action);
            Assert.assertNotNull(qe.childActions);
            Assert.assertNull(qe.op);
            Assert.assertEquals(0, qe.childActions.size());
            Assert.assertNull(qe.parent);
            
            q = "  ab  ==  'bccc'    &&  ( ( ccc  ==   'xc' or  ( d == 'a' or xx == 'wdd') ) && x == '123')";
            p = new QueryParserHelper(q, manager, date, date);
            Assert.assertEquals(1, p.execTree.size());
            qe = p.execTree.getFirst();
            Assert.assertNotNull(qe.action);
            Assert.assertNotNull(qe.childActions);
            Assert.assertNotNull(qe.op);
            Assert.assertNull(qe.parent);
            List<QueryEntry> ql = p.execTree.getFirst().childActions;
            Assert.assertEquals(3, ql.size());
            QueryEntry c = ((LinkedList<QueryEntry>) ql).getFirst();
            Assert.assertNotNull(c.action);
            Assert.assertNotNull(c.childActions);
            Assert.assertNotNull(c.parent);
            Assert.assertNotNull(c.op);
            Assert.assertEquals(2, c.childActions.size());
            
            q = "  (ab == 'bccc' && (ccc == 'xc' or sdb ==  'x')) ";
            new QueryParserHelper(q, manager, date, date);
        }
        
        private static class TestManager implements IRawDataManager {
            Type testType = String.class;
            
            @Override
            public void addTestData(URI file) throws IOException {
                
            }
            
            @Override
            public Set<IRawData> findMatchers(QueryAction action, Date startDate, Date endDate) {
                return null;
            }
            
            @Override
            public Type getFieldType(String field) {
                return this.testType;
            }
            
            @Override
            public Set<String> getKeyField(Set<IRawData> entries) {
                return null;
            }
            
            @Override
            public Date[] getRandomStartEndDate() {
                return new Date[0];
            }
        }
    }
    
}
