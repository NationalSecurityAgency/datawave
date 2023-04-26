package datawave.query.jexl.visitors;

import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.util.MetadataHelper;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.commons.jexl2.parser.ASTEvaluationOnly;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.ASTReferenceExpression;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.JexlNodes;
import org.apache.commons.jexl2.parser.ParseException;
import org.apache.commons.jexl2.parser.ParserTreeConstants;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

public class ExecutableDeterminationVisitorTest extends EasyMockSupport {
    private ShardQueryConfiguration config;
    private MetadataHelper helper;

    @Before
    public void setup() {
        config = createMock(ShardQueryConfiguration.class);
        helper = createMock(MetadataHelper.class);

        EasyMock.expect(config.getDatatypeFilter()).andReturn(null).anyTimes();
    }

    @Test
    public void testNegationErrorCheck() throws ParseException, TableNotFoundException {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery("FOO == 'bar' && !(INDEXONLYFIELD == null)");

        HashSet indexedFields = new HashSet();
        indexedFields.add("INDEXONLYFIELD");
        indexedFields.add("INDEXEDFIELD");
        HashSet<String> indexOnlyFields = new HashSet<>();
        indexOnlyFields.add("INDEXONLYFIELD");

        EasyMock.expect(config.getIndexedFields()).andReturn(indexedFields).anyTimes();
        EasyMock.expect(helper.getIndexOnlyFields(null)).andReturn(indexOnlyFields).anyTimes();

        replayAll();

        LinkedList<String> output = new LinkedList<>();

        boolean executable = ExecutableDeterminationVisitor.isExecutable(query, config, helper, output);
        Assert.assertFalse(executable);
        Assert.assertEquals("Summary: ERROR:[INDEXONLYFIELD == null]", output.get(0));

        output.clear();
        boolean fiExecutable = ExecutableDeterminationVisitor.isExecutable(query, config, helper, true, output);
        Assert.assertFalse(fiExecutable);
        Assert.assertEquals("Summary: ERROR:[INDEXONLYFIELD == null]", output.get(0));

        output.clear();
        ExecutableDeterminationVisitor.STATE state = ExecutableDeterminationVisitor.getState(query, config, helper, output);
        Assert.assertEquals(ExecutableDeterminationVisitor.STATE.ERROR, state);
        Assert.assertEquals("Summary: ERROR:[INDEXONLYFIELD == null]", output.get(0));

        output.clear();
        ExecutableDeterminationVisitor.STATE fiState = ExecutableDeterminationVisitor.getState(query, config, helper, true, output);
        Assert.assertEquals(ExecutableDeterminationVisitor.STATE.ERROR, fiState);
        Assert.assertEquals("Summary: ERROR:[INDEXONLYFIELD == null]", output.get(0));

        verifyAll();
    }

    @Test
    public void testIndexOnlyEqNull() throws ParseException, TableNotFoundException {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery("INDEXONLYFIELD == null");

        HashSet indexedFields = new HashSet();
        indexedFields.add("INDEXONLYFIELD");

        HashSet<String> indexOnlyFields = new HashSet<>();
        indexOnlyFields.add("INDEXONLYFIELD");

        EasyMock.expect(config.getIndexedFields()).andReturn(indexedFields).anyTimes();
        EasyMock.expect(helper.getIndexOnlyFields(null)).andReturn(indexOnlyFields).anyTimes();

        LinkedList<String> output = new LinkedList<>();

        replayAll();

        boolean executable = ExecutableDeterminationVisitor.isExecutable(query, config, helper, output);
        Assert.assertFalse(executable);
        Assert.assertEquals("Summary: ERROR:[INDEXONLYFIELD == null]", output.get(0));

        output.clear();
        boolean fiExecutable = ExecutableDeterminationVisitor.isExecutable(query, config, helper, true, output);
        Assert.assertFalse(fiExecutable);
        Assert.assertEquals("Summary: ERROR:[INDEXONLYFIELD == null]", output.get(0));

        output.clear();
        ExecutableDeterminationVisitor.STATE state = ExecutableDeterminationVisitor.getState(query, config, helper, output);
        Assert.assertEquals(ExecutableDeterminationVisitor.STATE.ERROR, state);
        Assert.assertEquals("Summary: ERROR:[INDEXONLYFIELD == null]", output.get(0));

        output.clear();
        ExecutableDeterminationVisitor.STATE fiState = ExecutableDeterminationVisitor.getState(query, config, helper, true, output);
        Assert.assertEquals(ExecutableDeterminationVisitor.STATE.ERROR, fiState);
        Assert.assertEquals("Summary: ERROR:[INDEXONLYFIELD == null]", output.get(0));

        verifyAll();
    }

    @Test
    public void testIndexOnlyNeNull() throws ParseException, TableNotFoundException {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery("INDEXONLYFIELD != null");

        LinkedList<String> output = new LinkedList<>();

        HashSet indexedFields = new HashSet();
        indexedFields.add("INDEXONLYFIELD");

        HashSet<String> indexOnlyFields = new HashSet<>();
        indexOnlyFields.add("INDEXONLYFIELD");

        EasyMock.expect(config.getIndexedFields()).andReturn(indexedFields).anyTimes();
        EasyMock.expect(helper.getIndexOnlyFields(null)).andReturn(indexOnlyFields).anyTimes();

        replayAll();

        boolean executable = ExecutableDeterminationVisitor.isExecutable(query, config, helper, output);
        Assert.assertFalse(executable);
        Assert.assertEquals("Summary: ERROR:[INDEXONLYFIELD != null]", output.get(0));

        output.clear();
        boolean fiExecutable = ExecutableDeterminationVisitor.isExecutable(query, config, helper, true, output);
        Assert.assertFalse(fiExecutable);
        Assert.assertEquals("Summary: ERROR:[INDEXONLYFIELD != null]", output.get(0));

        output.clear();
        ExecutableDeterminationVisitor.STATE state = ExecutableDeterminationVisitor.getState(query, config, helper, output);
        Assert.assertEquals(ExecutableDeterminationVisitor.STATE.ERROR, state);
        Assert.assertEquals("Summary: ERROR:[INDEXONLYFIELD != null]", output.get(0));

        output.clear();
        ExecutableDeterminationVisitor.STATE fiState = ExecutableDeterminationVisitor.getState(query, config, helper, true, output);
        Assert.assertEquals(ExecutableDeterminationVisitor.STATE.ERROR, fiState);
        Assert.assertEquals("Summary: ERROR:[INDEXONLYFIELD != null]", output.get(0));

        verifyAll();
    }

    @Test
    public void testIndexedEqNull() throws ParseException, TableNotFoundException {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery("INDEXEDFIELD == null");

        LinkedList<String> output = new LinkedList<>();

        HashSet indexedFields = new HashSet();
        indexedFields.add("INDEXONLYFIELD");
        indexedFields.add("INDEXEDFIELD");

        HashSet<String> indexOnlyFields = new HashSet<>();
        indexOnlyFields.add("INDEXONLYFIELD");

        EasyMock.expect(config.getIndexedFields()).andReturn(indexedFields).anyTimes();
        EasyMock.expect(helper.getIndexOnlyFields(null)).andReturn(indexOnlyFields).anyTimes();

        replayAll();

        boolean executable = ExecutableDeterminationVisitor.isExecutable(query, config, helper, output);
        Assert.assertFalse(executable);
        Assert.assertEquals("Summary: NON_EXECUTABLE:[INDEXEDFIELD == null]", output.get(0));

        output.clear();
        boolean fiExecutable = ExecutableDeterminationVisitor.isExecutable(query, config, helper, true, output);
        Assert.assertFalse(fiExecutable);
        Assert.assertEquals("Summary: NON_EXECUTABLE:[INDEXEDFIELD == null]", output.get(0));

        output.clear();
        ExecutableDeterminationVisitor.STATE state = ExecutableDeterminationVisitor.getState(query, config, helper, output);
        Assert.assertEquals(ExecutableDeterminationVisitor.STATE.NON_EXECUTABLE, state);
        Assert.assertEquals("Summary: NON_EXECUTABLE:[INDEXEDFIELD == null]", output.get(0));

        output.clear();
        ExecutableDeterminationVisitor.STATE fiState = ExecutableDeterminationVisitor.getState(query, config, helper, true, output);
        Assert.assertEquals(ExecutableDeterminationVisitor.STATE.NON_EXECUTABLE, fiState);
        Assert.assertEquals("Summary: NON_EXECUTABLE:[INDEXEDFIELD == null]", output.get(0));

        verifyAll();
    }

    @Test
    public void testIndexedNeNull() throws ParseException, TableNotFoundException {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery("INDEXEDFIELD != null");

        LinkedList<String> output = new LinkedList<>();

        HashSet indexedFields = new HashSet();
        indexedFields.add("INDEXONLYFIELD");
        indexedFields.add("INDEXEDFIELD");

        HashSet<String> indexOnlyFields = new HashSet<>();
        indexOnlyFields.add("INDEXONLYFIELD");

        EasyMock.expect(config.getIndexedFields()).andReturn(indexedFields).anyTimes();
        EasyMock.expect(helper.getIndexOnlyFields(null)).andReturn(indexOnlyFields).anyTimes();

        replayAll();

        boolean executable = ExecutableDeterminationVisitor.isExecutable(query, config, helper, output);
        Assert.assertFalse(executable);
        Assert.assertEquals("Summary: NON_EXECUTABLE:[INDEXEDFIELD != null]", output.get(0));

        output.clear();
        boolean fiExecutable = ExecutableDeterminationVisitor.isExecutable(query, config, helper, true, output);
        Assert.assertFalse(fiExecutable);
        Assert.assertEquals("Summary: NON_EXECUTABLE:[INDEXEDFIELD != null]", output.get(0));

        output.clear();
        ExecutableDeterminationVisitor.STATE state = ExecutableDeterminationVisitor.getState(query, config, helper, output);
        Assert.assertEquals(ExecutableDeterminationVisitor.STATE.NON_EXECUTABLE, state);
        Assert.assertEquals("Summary: NON_EXECUTABLE:[INDEXEDFIELD != null]", output.get(0));

        output.clear();
        ExecutableDeterminationVisitor.STATE fiState = ExecutableDeterminationVisitor.getState(query, config, helper, true, output);
        Assert.assertEquals(ExecutableDeterminationVisitor.STATE.NON_EXECUTABLE, fiState);
        Assert.assertEquals("Summary: NON_EXECUTABLE:[INDEXEDFIELD != null]", output.get(0));

        verifyAll();
    }

    /**
     * Expected outputs Global Index: EXECUTABLE || NOT_EXECUTABLE == PARTIAL Field Index: EXECUTABLE || !(EXECUTABLE && EXECUTABLE) == EXECUTABLE || EXECUTABLE
     * == EXECUTABLE
     *
     * @throws Exception if there is an issue
     */
    @Test
    public void testNegatedAndExecutable() throws Exception {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery("INDEXEDFIELD == 'a' || !(INDEXONLYFIELD == 'b' && INDEXEDFIELD == 'c')");

        LinkedList<String> output = new LinkedList<>();

        HashSet indexedFields = new HashSet();
        indexedFields.add("INDEXONLYFIELD");
        indexedFields.add("INDEXEDFIELD");

        HashSet<String> indexOnlyFields = new HashSet<>();
        indexOnlyFields.add("INDEXONLYFIELD");

        EasyMock.expect(config.getIndexedFields()).andReturn(indexedFields).anyTimes();
        EasyMock.expect(helper.getIndexOnlyFields(null)).andReturn(indexOnlyFields).anyTimes();

        replayAll();

        boolean executable = ExecutableDeterminationVisitor.isExecutable(query, config, helper, output);
        Assert.assertFalse(executable);
        Assert.assertEquals("Summary: NON_EXECUTABLE:[INDEXONLYFIELD == 'b', INDEXEDFIELD == 'c', INDEXEDFIELD == 'a']", output.get(0));

        output.clear();
        boolean fiExecutable = ExecutableDeterminationVisitor.isExecutable(query, config, helper, true, output);
        Assert.assertFalse(fiExecutable);
        Assert.assertEquals("Summary: NEGATED_EXECUTABLE:[INDEXONLYFIELD == 'b', INDEXEDFIELD == 'c', INDEXEDFIELD == 'a']", output.get(0));

        output.clear();
        ExecutableDeterminationVisitor.STATE state = ExecutableDeterminationVisitor.getState(query, config, helper, output);
        Assert.assertEquals(ExecutableDeterminationVisitor.STATE.PARTIAL, state);
        Assert.assertEquals("Summary: NON_EXECUTABLE:[INDEXONLYFIELD == 'b', INDEXEDFIELD == 'c', INDEXEDFIELD == 'a']", output.get(0));

        output.clear();
        ExecutableDeterminationVisitor.STATE fiState = ExecutableDeterminationVisitor.getState(query, config, helper, true, output);
        Assert.assertEquals(ExecutableDeterminationVisitor.STATE.NEGATED_EXECUTABLE, fiState);
        Assert.assertEquals("Summary: NEGATED_EXECUTABLE:[INDEXONLYFIELD == 'b', INDEXEDFIELD == 'c', INDEXEDFIELD == 'a']", output.get(0));

        verifyAll();
    }

    /**
     * Expected outputs Global Index: EXECUTABLE || NOT_EXECUTABLE == PARTIAL Field Index: EXECUTABLE || !!(EXECUTABLE && EXECUTABLE) == EXECUTABLE ||
     * EXECUTABLE == EXECUTABLE
     *
     * @throws Exception if there is an issue
     */
    @Test
    public void testDoubleNegatedAndExecutable() throws Exception {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery("INDEXEDFIELD == 'a' || !!(INDEXONLYFIELD == 'b' && INDEXEDFIELD == 'c')");

        LinkedList<String> output = new LinkedList<>();

        HashSet indexedFields = new HashSet();
        indexedFields.add("INDEXONLYFIELD");
        indexedFields.add("INDEXEDFIELD");

        HashSet<String> indexOnlyFields = new HashSet<>();
        indexOnlyFields.add("INDEXONLYFIELD");

        EasyMock.expect(config.getIndexedFields()).andReturn(indexedFields).anyTimes();
        EasyMock.expect(helper.getIndexOnlyFields(null)).andReturn(indexOnlyFields).anyTimes();

        replayAll();

        boolean executable = ExecutableDeterminationVisitor.isExecutable(query, config, helper, output);
        Assert.assertTrue(executable);
        Assert.assertEquals("Summary: EXECUTABLE:[INDEXONLYFIELD == 'b', INDEXEDFIELD == 'c', INDEXEDFIELD == 'a']", output.get(0));

        output.clear();
        boolean fiExecutable = ExecutableDeterminationVisitor.isExecutable(query, config, helper, true, output);
        Assert.assertTrue(fiExecutable);
        Assert.assertEquals("Summary: EXECUTABLE:[INDEXONLYFIELD == 'b', INDEXEDFIELD == 'c', INDEXEDFIELD == 'a']", output.get(0));

        output.clear();
        ExecutableDeterminationVisitor.STATE state = ExecutableDeterminationVisitor.getState(query, config, helper, output);
        Assert.assertEquals(ExecutableDeterminationVisitor.STATE.EXECUTABLE, state);
        Assert.assertEquals("Summary: EXECUTABLE:[INDEXONLYFIELD == 'b', INDEXEDFIELD == 'c', INDEXEDFIELD == 'a']", output.get(0));

        output.clear();
        ExecutableDeterminationVisitor.STATE fiState = ExecutableDeterminationVisitor.getState(query, config, helper, true, output);
        Assert.assertEquals(ExecutableDeterminationVisitor.STATE.EXECUTABLE, fiState);
        Assert.assertEquals("Summary: EXECUTABLE:[INDEXONLYFIELD == 'b', INDEXEDFIELD == 'c', INDEXEDFIELD == 'a']", output.get(0));

        verifyAll();
    }

    /**
     * Expected outputs Global Index: EXECUTABLE || NOT_EXECUTABLE == PARTIAL Field Index: EXECUTABLE || !(EXECUTABLE && NOT_EXECUTABLE) == EXECUTABLE ||
     * NOT_EXECUTABLE == PARTIAL
     *
     * @throws Exception if there is an issue
     */
    @Test
    public void testNegatedAndNotExecutable() throws Exception {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery("INDEXEDFIELD == 'a' || !(INDEXONLYFIELD == 'b' && filter:includeRegex(INDEXEDFIELD,'.*'))");

        LinkedList<String> output = new LinkedList<>();

        HashSet indexedFields = new HashSet();
        indexedFields.add("INDEXONLYFIELD");
        indexedFields.add("INDEXEDFIELD");

        HashSet<String> indexOnlyFields = new HashSet<>();
        indexOnlyFields.add("INDEXONLYFIELD");

        EasyMock.expect(config.getIndexedFields()).andReturn(indexedFields).anyTimes();
        EasyMock.expect(helper.getIndexOnlyFields(null)).andReturn(indexOnlyFields).anyTimes();

        replayAll();

        boolean executable = ExecutableDeterminationVisitor.isExecutable(query, config, helper, output);
        Assert.assertFalse(executable);
        Assert.assertEquals("Summary: NON_EXECUTABLE:[INDEXONLYFIELD == 'b', INDEXEDFIELD == 'a', filter:includeRegex(INDEXEDFIELD, '.*')]", output.get(0));

        output.clear();
        boolean fiExecutable = ExecutableDeterminationVisitor.isExecutable(query, config, helper, true, output);
        Assert.assertFalse(fiExecutable);
        Assert.assertEquals("Summary: NON_EXECUTABLE:[filter:includeRegex(INDEXEDFIELD, '.*')]", output.get(0));

        output.clear();
        ExecutableDeterminationVisitor.STATE state = ExecutableDeterminationVisitor.getState(query, config, helper, output);
        Assert.assertEquals(ExecutableDeterminationVisitor.STATE.PARTIAL, state);
        Assert.assertEquals("Summary: NON_EXECUTABLE:[INDEXONLYFIELD == 'b', INDEXEDFIELD == 'a', filter:includeRegex(INDEXEDFIELD, '.*')]", output.get(0));

        output.clear();
        ExecutableDeterminationVisitor.STATE fiState = ExecutableDeterminationVisitor.getState(query, config, helper, true, output);
        Assert.assertEquals(ExecutableDeterminationVisitor.STATE.PARTIAL, fiState);
        Assert.assertEquals("Summary: NON_EXECUTABLE:[filter:includeRegex(INDEXEDFIELD, '.*')]", output.get(0));

        verifyAll();
    }

    /**
     * Expected outputs Global Index: EXECUTABLE || NOT_EXECUTABLE == PARTIAL Field Index: EXECUTABLE || !!(EXECUTABLE && NOT_EXECUTABLE) == EXECUTABLE ||
     * EXECUTABLE == EXECUTABLE
     *
     * @throws Exception if there is an issue
     */
    @Test
    public void testDoubleNegatedAndNotExecutable() throws Exception {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery("INDEXEDFIELD == 'a' || !!(INDEXONLYFIELD == 'b' && filter:includeRegex(INDEXEDFIELD,'.*'))");

        LinkedList<String> output = new LinkedList<>();

        HashSet indexedFields = new HashSet();
        indexedFields.add("INDEXONLYFIELD");
        indexedFields.add("INDEXEDFIELD");

        HashSet<String> indexOnlyFields = new HashSet<>();
        indexOnlyFields.add("INDEXONLYFIELD");

        EasyMock.expect(config.getIndexedFields()).andReturn(indexedFields).anyTimes();
        EasyMock.expect(helper.getIndexOnlyFields(null)).andReturn(indexOnlyFields).anyTimes();

        replayAll();

        boolean executable = ExecutableDeterminationVisitor.isExecutable(query, config, helper, output);
        Assert.assertTrue(executable);
        Assert.assertEquals("Summary: EXECUTABLE:[INDEXONLYFIELD == 'b', INDEXEDFIELD == 'a']", output.get(0));

        output.clear();
        boolean fiExecutable = ExecutableDeterminationVisitor.isExecutable(query, config, helper, true, output);
        Assert.assertTrue(fiExecutable);
        Assert.assertEquals("Summary: EXECUTABLE:[INDEXONLYFIELD == 'b', INDEXEDFIELD == 'a']", output.get(0));

        output.clear();
        ExecutableDeterminationVisitor.STATE state = ExecutableDeterminationVisitor.getState(query, config, helper, output);
        Assert.assertEquals(ExecutableDeterminationVisitor.STATE.EXECUTABLE, state);
        Assert.assertEquals("Summary: EXECUTABLE:[INDEXONLYFIELD == 'b', INDEXEDFIELD == 'a']", output.get(0));

        output.clear();
        ExecutableDeterminationVisitor.STATE fiState = ExecutableDeterminationVisitor.getState(query, config, helper, true, output);
        Assert.assertEquals(ExecutableDeterminationVisitor.STATE.EXECUTABLE, fiState);
        Assert.assertEquals("Summary: EXECUTABLE:[INDEXONLYFIELD == 'b', INDEXEDFIELD == 'a']", output.get(0));

        verifyAll();
    }

    /**
     * Expected outputs Global Index: EXECUTABLE || NOT_EXECUTABLE == PARTIAL Field Index: EXECUTABLE || !(EXECUTABLE || NOT_EXECUTABLE) == EXECUTABLE ||
     * EXECUTABLE == EXECUTABLE
     *
     * @throws Exception if there is an issue
     */
    @Test
    public void testNegatedOrExecutable() throws Exception {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery("INDEXEDFIELD == 'a' || !(INDEXONLYFIELD == 'b' || filter:includeRegex(INDEXEDFIELD,'.*'))");

        LinkedList<String> output = new LinkedList<>();

        HashSet indexedFields = new HashSet();
        indexedFields.add("INDEXONLYFIELD");
        indexedFields.add("INDEXEDFIELD");

        HashSet<String> indexOnlyFields = new HashSet<>();
        indexOnlyFields.add("INDEXONLYFIELD");

        EasyMock.expect(config.getIndexedFields()).andReturn(indexedFields).anyTimes();
        EasyMock.expect(helper.getIndexOnlyFields(null)).andReturn(indexOnlyFields).anyTimes();

        replayAll();

        boolean executable = ExecutableDeterminationVisitor.isExecutable(query, config, helper, output);
        Assert.assertFalse(executable);
        Assert.assertEquals("Summary: NON_EXECUTABLE:[INDEXONLYFIELD == 'b', filter:includeRegex(INDEXEDFIELD, '.*')]", output.get(0));

        output.clear();
        boolean fiExecutable = ExecutableDeterminationVisitor.isExecutable(query, config, helper, true, output);
        Assert.assertFalse(fiExecutable);
        Assert.assertTrue(Arrays.asList(
                new String[]{"Summary: EXECUTABLE:[INDEXEDFIELD == 'a']; NEGATED_EXECUTABLE:[INDEXONLYFIELD == 'b']",
                        "Summary: NEGATED_EXECUTABLE:[INDEXONLYFIELD == 'b']; EXECUTABLE:[INDEXEDFIELD == 'a']"}).contains(output.get(0)));

        output.clear();
        ExecutableDeterminationVisitor.STATE state = ExecutableDeterminationVisitor.getState(query, config, helper, output);
        Assert.assertEquals(ExecutableDeterminationVisitor.STATE.PARTIAL, state);
        Assert.assertEquals("Summary: NON_EXECUTABLE:[INDEXONLYFIELD == 'b', filter:includeRegex(INDEXEDFIELD, '.*')]", output.get(0));

        output.clear();
        ExecutableDeterminationVisitor.STATE fiState = ExecutableDeterminationVisitor.getState(query, config, helper, true, output);
        Assert.assertEquals(ExecutableDeterminationVisitor.STATE.NEGATED_EXECUTABLE, fiState);
        Assert.assertTrue(Arrays.asList(
                new String[]{"Summary: EXECUTABLE:[INDEXEDFIELD == 'a']; NEGATED_EXECUTABLE:[INDEXONLYFIELD == 'b']",
                        "Summary: NEGATED_EXECUTABLE:[INDEXONLYFIELD == 'b']; EXECUTABLE:[INDEXEDFIELD == 'a']"}).contains(output.get(0)));

        verifyAll();
    }

    /**
     * Expected outputs Global Index: EXECUTABLE || NOT_EXECUTABLE == PARTIAL Field Index: EXECUTABLE || !!(EXECUTABLE || NOT_EXECUTABLE) == EXECUTABLE ||
     * NON_EXECUTABLE == PARTIAL
     *
     * @throws Exception if there is an issue
     */
    @Test
    public void testDoubleNegatedOrExecutable() throws Exception {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery("INDEXEDFIELD == 'a' || !!(INDEXONLYFIELD == 'b' || filter:includeRegex(INDEXEDFIELD,'.*'))");

        LinkedList<String> output = new LinkedList<>();

        HashSet indexedFields = new HashSet();
        indexedFields.add("INDEXONLYFIELD");
        indexedFields.add("INDEXEDFIELD");

        HashSet<String> indexOnlyFields = new HashSet<>();
        indexOnlyFields.add("INDEXONLYFIELD");

        EasyMock.expect(config.getIndexedFields()).andReturn(indexedFields).anyTimes();
        EasyMock.expect(helper.getIndexOnlyFields(null)).andReturn(indexOnlyFields).anyTimes();

        replayAll();

        boolean executable = ExecutableDeterminationVisitor.isExecutable(query, config, helper, output);
        Assert.assertFalse(executable);
        Assert.assertEquals("Summary: NON_EXECUTABLE:[filter:includeRegex(INDEXEDFIELD, '.*')]", output.get(0));

        output.clear();
        boolean fiExecutable = ExecutableDeterminationVisitor.isExecutable(query, config, helper, true, output);
        Assert.assertFalse(fiExecutable);
        Assert.assertEquals("Summary: NON_EXECUTABLE:[filter:includeRegex(INDEXEDFIELD, '.*')]", output.get(0));

        output.clear();
        ExecutableDeterminationVisitor.STATE state = ExecutableDeterminationVisitor.getState(query, config, helper, output);
        Assert.assertEquals(ExecutableDeterminationVisitor.STATE.PARTIAL, state);
        Assert.assertEquals("Summary: NON_EXECUTABLE:[filter:includeRegex(INDEXEDFIELD, '.*')]", output.get(0));

        output.clear();
        ExecutableDeterminationVisitor.STATE fiState = ExecutableDeterminationVisitor.getState(query, config, helper, true, output);
        Assert.assertEquals(ExecutableDeterminationVisitor.STATE.PARTIAL, fiState);
        Assert.assertEquals("Summary: NON_EXECUTABLE:[filter:includeRegex(INDEXEDFIELD, '.*')]", output.get(0));

        verifyAll();
    }

    @Test
    public void testNegationEvaluation() {
        Assert.assertFalse(ExecutableDeterminationVisitor.isNegated(null));
        Assert.assertFalse(ExecutableDeterminationVisitor.isNegated(""));
        Assert.assertFalse(ExecutableDeterminationVisitor.isNegated("!!"));
        Assert.assertFalse(ExecutableDeterminationVisitor.isNegated("            !                    !    "));
        Assert.assertFalse(ExecutableDeterminationVisitor.isNegated("!           !   !  !    !        !    !    !"));

        Assert.assertTrue(ExecutableDeterminationVisitor.isNegated("!!!"));
        Assert.assertTrue(ExecutableDeterminationVisitor.isNegated("            !                    !    !"));
        Assert.assertTrue(ExecutableDeterminationVisitor.isNegated("!                                !    !"));
        Assert.assertTrue(ExecutableDeterminationVisitor.isNegated("!           !   !  !    !            !    !"));
        Assert.assertTrue(ExecutableDeterminationVisitor.isNegated("!           !   !  !    !            !    !             "));
    }

    /**
     * Copied from JexlNodeFactory without the AND/OR restrictions
     *
     * @param toWrap the jexl node to wrap
     * @return reference to the wrapped node
     */
    private JexlNode wrap(JexlNode toWrap) {
        ASTReference reference = new ASTReference(ParserTreeConstants.JJTREFERENCE);
        ASTReferenceExpression parens = new ASTReferenceExpression(ParserTreeConstants.JJTREFERENCEEXPRESSION);

        parens.jjtAddChild(toWrap, 0);
        toWrap.jjtSetParent(parens);

        reference.jjtAddChild(parens, 0);
        parens.jjtSetParent(reference);

        return reference;
    }

    @Test
    public void testAllOrSomeEmpty() throws TableNotFoundException {
        List<JexlNode> children = new ArrayList<>();
        JexlNode eqNode = JexlNodeFactory.buildEQNode("INDEXEDFIELD", "v3");
        JexlNode wrappedNode = wrap(eqNode);
        children.add(wrappedNode);

        // remove the eq node
        JexlNodes.removeFromParent(eqNode.jjtGetParent(), eqNode);

        JexlNode andNode = JexlNodeFactory.createAndNode(children);

        LinkedList<String> output = new LinkedList<>();

        HashSet indexedFields = new HashSet();
        indexedFields.add("INDEXONLYFIELD");
        indexedFields.add("INDEXEDFIELD");

        HashSet<String> indexOnlyFields = new HashSet<>();
        indexOnlyFields.add("INDEXONLYFIELD");

        EasyMock.expect(config.getIndexedFields()).andReturn(indexedFields).anyTimes();
        EasyMock.expect(helper.getIndexOnlyFields(null)).andReturn(indexOnlyFields).anyTimes();

        replayAll();

        Assert.assertEquals(ExecutableDeterminationVisitor.STATE.IGNORABLE, ExecutableDeterminationVisitor.getState(andNode, config, helper, output));
        Assert.assertEquals("Summary: ", output.get(0)); // no summary because the query is empty

        verifyAll();
    }

    @Test
    public void testAllOrSomeEmptyPortion() throws TableNotFoundException {
        List<JexlNode> children = new ArrayList<>();
        children.add(wrap(JexlNodeFactory.buildEQNode("INDEXEDFIELD", "v1")));
        children.add(wrap(JexlNodeFactory.buildEQNode("INDEXEDFIELD", "v2")));
        JexlNode eqNode = JexlNodeFactory.buildEQNode("INDEXEDFIELD", "v3");
        JexlNode wrappedNode = wrap(eqNode);
        children.add(wrappedNode);

        // remove the eq node
        JexlNodes.removeFromParent(eqNode.jjtGetParent(), eqNode);

        JexlNode andNode = JexlNodeFactory.createAndNode(children);

        LinkedList<String> output = new LinkedList<>();

        HashSet indexedFields = new HashSet();
        indexedFields.add("INDEXONLYFIELD");
        indexedFields.add("INDEXEDFIELD");

        HashSet<String> indexOnlyFields = new HashSet<>();
        indexOnlyFields.add("INDEXONLYFIELD");

        EasyMock.expect(config.getIndexedFields()).andReturn(indexedFields).anyTimes();
        EasyMock.expect(helper.getIndexOnlyFields(null)).andReturn(indexOnlyFields).anyTimes();

        replayAll();

        Assert.assertEquals(ExecutableDeterminationVisitor.STATE.EXECUTABLE, ExecutableDeterminationVisitor.getState(andNode, config, helper, output));
        Assert.assertEquals("Summary: EXECUTABLE:[INDEXEDFIELD == 'v2', INDEXEDFIELD == 'v1']", output.get(0));

        verifyAll();
    }

    @Test
    public void testAllOrNoneEmpty() throws TableNotFoundException {
        List<JexlNode> children = new ArrayList<>();
        JexlNode eqNode = JexlNodeFactory.buildEQNode("INDEXEDFIELD", "v3");
        JexlNode wrappedNode = wrap(eqNode);
        children.add(wrappedNode);

        // remove the eq node
        JexlNodes.removeFromParent(eqNode.jjtGetParent(), eqNode);

        JexlNode orNode = JexlNodeFactory.createOrNode(children);

        LinkedList<String> output = new LinkedList<>();

        HashSet indexedFields = new HashSet();
        indexedFields.add("INDEXONLYFIELD");
        indexedFields.add("INDEXEDFIELD");

        HashSet<String> indexOnlyFields = new HashSet<>();
        indexOnlyFields.add("INDEXONLYFIELD");

        EasyMock.expect(config.getIndexedFields()).andReturn(indexedFields).anyTimes();
        EasyMock.expect(helper.getIndexOnlyFields(null)).andReturn(indexOnlyFields).anyTimes();

        replayAll();

        Assert.assertEquals(ExecutableDeterminationVisitor.STATE.IGNORABLE, ExecutableDeterminationVisitor.getState(orNode, config, helper, output));
        Assert.assertEquals("Summary: ", output.get(0)); // empty summary because the query is empty

        verifyAll();
    }

    @Test
    public void testAllOrNoneEmptyPortion() throws TableNotFoundException {
        List<JexlNode> children = new ArrayList<>();
        children.add(wrap(JexlNodeFactory.buildEQNode("INDEXEDFIELD", "v1")));
        children.add(wrap(JexlNodeFactory.buildEQNode("INDEXEDFIELD", "v2")));
        JexlNode eqNode = JexlNodeFactory.buildEQNode("INDEXEDFIELD", "v3");
        JexlNode wrappedNode = wrap(eqNode);
        children.add(wrappedNode);

        // remove the eq node
        JexlNodes.removeFromParent(eqNode.jjtGetParent(), eqNode);

        JexlNode orNode = JexlNodeFactory.createOrNode(children);

        LinkedList<String> output = new LinkedList<>();

        HashSet indexedFields = new HashSet();
        indexedFields.add("INDEXONLYFIELD");
        indexedFields.add("INDEXEDFIELD");

        HashSet<String> indexOnlyFields = new HashSet<>();
        indexOnlyFields.add("INDEXONLYFIELD");

        EasyMock.expect(config.getIndexedFields()).andReturn(indexedFields).anyTimes();
        EasyMock.expect(helper.getIndexOnlyFields(null)).andReturn(indexOnlyFields).anyTimes();

        replayAll();

        Assert.assertEquals(ExecutableDeterminationVisitor.STATE.EXECUTABLE, ExecutableDeterminationVisitor.getState(orNode, config, helper, output));
        Assert.assertEquals("Summary: EXECUTABLE:[INDEXEDFIELD == 'v2', INDEXEDFIELD == 'v1']", output.get(0));

        verifyAll();
    }

    @Test
    public void testEvaluationOnlyReferenceNode() throws ParseException, TableNotFoundException {
        EasyMock.expect(helper.getNonEventFields(null)).andReturn(Collections.emptySet());

        LinkedList<String> output = new LinkedList<>();

        replayAll();

        JexlNode query = ASTEvaluationOnly.create(JexlASTHelper.parseJexlQuery("FOO == FOO2"));
        Assert.assertEquals(ExecutableDeterminationVisitor.STATE.NON_EXECUTABLE, ExecutableDeterminationVisitor.getState(query, config, helper, output));
        Assert.assertEquals("Summary: NON_EXECUTABLE:[ASTEvaluationOnly( delayed/eval only predicate )]", output.get(0));
    }
}
