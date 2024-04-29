package datawave.query.jexl.visitors;

import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.EVALUATION_ONLY;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ASTReference;
import org.apache.commons.jexl3.parser.ASTReferenceExpression;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.JexlNodes;
import org.apache.commons.jexl3.parser.ParseException;
import org.apache.commons.jexl3.parser.ParserTreeConstants;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.BeforeEach;

import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import datawave.query.util.MetadataHelper;

public class ExecutableDeterminationVisitorTest extends EasyMockSupport {
    private ShardQueryConfiguration config;
    private MetadataHelper helper;

    private final int maxTermsToPrint = 100;

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
        EasyMock.expect(config.getMaxTermsToPrint()).andReturn(maxTermsToPrint).anyTimes();
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
        EasyMock.expect(config.getMaxTermsToPrint()).andReturn(maxTermsToPrint).anyTimes();
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
        EasyMock.expect(config.getMaxTermsToPrint()).andReturn(maxTermsToPrint).anyTimes();
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
        EasyMock.expect(config.getMaxTermsToPrint()).andReturn(maxTermsToPrint).anyTimes();
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
        EasyMock.expect(config.getMaxTermsToPrint()).andReturn(maxTermsToPrint).anyTimes();
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
     * @throws Exception
     *             if there is an issue
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
        EasyMock.expect(config.getMaxTermsToPrint()).andReturn(maxTermsToPrint).anyTimes();
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
     * @throws Exception
     *             if there is an issue
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
        EasyMock.expect(config.getMaxTermsToPrint()).andReturn(maxTermsToPrint).anyTimes();
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
     * @throws Exception
     *             if there is an issue
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
        EasyMock.expect(config.getMaxTermsToPrint()).andReturn(maxTermsToPrint).anyTimes();
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
     * @throws Exception
     *             if there is an issue
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
        EasyMock.expect(config.getMaxTermsToPrint()).andReturn(maxTermsToPrint).anyTimes();
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
     * @throws Exception
     *             if there is an issue
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
        EasyMock.expect(config.getMaxTermsToPrint()).andReturn(maxTermsToPrint).anyTimes();
        EasyMock.expect(helper.getIndexOnlyFields(null)).andReturn(indexOnlyFields).anyTimes();

        replayAll();

        boolean executable = ExecutableDeterminationVisitor.isExecutable(query, config, helper, output);
        Assert.assertFalse(executable);
        Assert.assertEquals("Summary: NON_EXECUTABLE:[INDEXONLYFIELD == 'b', filter:includeRegex(INDEXEDFIELD, '.*')]", output.get(0));

        output.clear();
        boolean fiExecutable = ExecutableDeterminationVisitor.isExecutable(query, config, helper, true, output);
        Assert.assertFalse(fiExecutable);
        Assert.assertTrue(Arrays.asList(new String[] {"Summary: EXECUTABLE:[INDEXEDFIELD == 'a']; NEGATED_EXECUTABLE:[INDEXONLYFIELD == 'b']",
                "Summary: NEGATED_EXECUTABLE:[INDEXONLYFIELD == 'b']; EXECUTABLE:[INDEXEDFIELD == 'a']"}).contains(output.get(0)));

        output.clear();
        ExecutableDeterminationVisitor.STATE state = ExecutableDeterminationVisitor.getState(query, config, helper, output);
        Assert.assertEquals(ExecutableDeterminationVisitor.STATE.PARTIAL, state);
        Assert.assertEquals("Summary: NON_EXECUTABLE:[INDEXONLYFIELD == 'b', filter:includeRegex(INDEXEDFIELD, '.*')]", output.get(0));

        output.clear();
        ExecutableDeterminationVisitor.STATE fiState = ExecutableDeterminationVisitor.getState(query, config, helper, true, output);
        Assert.assertEquals(ExecutableDeterminationVisitor.STATE.NEGATED_EXECUTABLE, fiState);
        Assert.assertTrue(Arrays.asList(new String[] {"Summary: EXECUTABLE:[INDEXEDFIELD == 'a']; NEGATED_EXECUTABLE:[INDEXONLYFIELD == 'b']",
                "Summary: NEGATED_EXECUTABLE:[INDEXONLYFIELD == 'b']; EXECUTABLE:[INDEXEDFIELD == 'a']"}).contains(output.get(0)));

        verifyAll();
    }

    /**
     * Expected outputs Global Index: EXECUTABLE || NOT_EXECUTABLE == PARTIAL Field Index: EXECUTABLE || !!(EXECUTABLE || NOT_EXECUTABLE) == EXECUTABLE ||
     * NON_EXECUTABLE == PARTIAL
     *
     * @throws Exception
     *             if there is an issue
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
        EasyMock.expect(config.getMaxTermsToPrint()).andReturn(maxTermsToPrint).anyTimes();
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
     * @param toWrap
     *            the jexl node to wrap
     * @return reference to the wrapped node
     */
    private JexlNode wrap(JexlNode toWrap) {
        ASTReference reference = new ASTReference(ParserTreeConstants.JJTREFERENCE);
        ASTReferenceExpression parens = JexlNodes.makeRefExp();

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
        EasyMock.expect(config.getMaxTermsToPrint()).andReturn(maxTermsToPrint).anyTimes();
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
        EasyMock.expect(config.getMaxTermsToPrint()).andReturn(maxTermsToPrint).anyTimes();
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
        EasyMock.expect(config.getMaxTermsToPrint()).andReturn(maxTermsToPrint).anyTimes();
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
        EasyMock.expect(config.getMaxTermsToPrint()).andReturn(maxTermsToPrint).anyTimes();
        EasyMock.expect(helper.getIndexOnlyFields(null)).andReturn(indexOnlyFields).anyTimes();

        replayAll();

        Assert.assertEquals(ExecutableDeterminationVisitor.STATE.EXECUTABLE, ExecutableDeterminationVisitor.getState(orNode, config, helper, output));
        Assert.assertEquals("Summary: EXECUTABLE:[INDEXEDFIELD == 'v2', INDEXEDFIELD == 'v1']", output.get(0));

        verifyAll();
    }

    @Test
    public void testEvaluationOnlyReferenceNode() throws ParseException, TableNotFoundException {
        EasyMock.expect(config.getMaxTermsToPrint()).andReturn(maxTermsToPrint).anyTimes();
        EasyMock.expect(helper.getIndexOnlyFields(null)).andReturn(Collections.emptySet());

        LinkedList<String> output = new LinkedList<>();

        replayAll();

        JexlNode query = QueryPropertyMarker.create(JexlASTHelper.parseJexlQuery("FOO == FOO2"), EVALUATION_ONLY);
        Assert.assertEquals(ExecutableDeterminationVisitor.STATE.NON_EXECUTABLE, ExecutableDeterminationVisitor.getState(query, config, helper, output));
        Assert.assertEquals("Summary: NON_EXECUTABLE:[AndNode( delayed/eval only predicate )]", output.get(0));
    }

    @Test
    public void testHugeQueryTermEvaluationOnly() throws ParseException, TableNotFoundException {
        EasyMock.expect(config.getMaxTermsToPrint()).andReturn(maxTermsToPrint).anyTimes();
        EasyMock.expect(helper.getIndexOnlyFields(null)).andReturn(Collections.emptySet());

        LinkedList<String> output = new LinkedList<>();

        replayAll();

        String QueryString = "v1 == 'foo' || v1 == 'bar' || v1 == 'baz' || " + "v1 == 'qux' || v1 == 'quux' || v1 == 'corge' || "
                        + "v1 == 'grault' || v1 == 'garply' || v1 == 'waldo' || " + "v1 == 'fred' || v1 == 'plugh' || v1 == 'xyzzy' || "
                        + "v1 == 'thud' || v1 == 'mumble' || v1 == 'frotz' || " + "v1 == 'blorb' || v1 == 'plover' || v1 == 'xyzzy' || "
                        + "v1 == 'anteater' || v1 == 'beetle' || v1 == 'cobra' || " + "v1 == 'dingo' || v1 == 'eel' || v1 == 'frog' || "
                        + "v1 == 'gopher' || v1 == 'hippo' || v1 == 'iguana' || " + "v1 == 'jaguar' || v1 == 'koala' || v1 == 'lemur' || "
                        + "v1 == 'mongoose' || v1 == 'newt' || v1 == 'octopus' || " + "v1 == 'python' || v1 == 'quail' || v1 == 'rabbit' || "
                        + "v1 == 'shark' || v1 == 'tiger' || v1 == 'urchin' || " + "v1 == 'viper' || v1 == 'wombat' || v1 == 'xiphos' || "
                        + "v1 == 'yak' || v1 == 'zebra' || v1 == 'apple' || " + "v1 == 'banana' || v1 == 'cherry' || v1 == 'date' || "
                        + "v1 == 'elderberry' || v1 == 'fig' || v1 == 'grape' || " + "v1 == 'honeydew' || v1 == 'kiwi' || v1 == 'lemon' || "
                        + "v1 == 'mango' || v1 == 'nectarine' || v1 == 'orange' || " + "v1 == 'papaya' || v1 == 'quince' || v1 == 'raspberry' || "
                        + "v1 == 'strawberry' || v1 == 'tangerine' || v1 == 'ugli' || " + "v1 == 'vanilla' || v1 == 'watermelon' || v1 == 'xigua' || "
                        + "v1 == 'yuzu' || v1 == 'zucchini' || v1 == 'almond' || " + "v1 == 'brazilnut' || v1 == 'cashew' || v1 == 'doughnut' || "
                        + "v1 == 'espresso' || v1 == 'frappuccino' || v1 == 'granola' || " + "v1 == 'halva' || v1 == 'icecream' || v1 == 'jellybean' || "
                        + "v1 == 'kale' || v1 == 'lettuce' || v1 == 'mushroom' || " + "v1 == 'nougat' || v1 == 'olive' || v1 == 'peanut' || "
                        + "v1 == 'quiche' || v1 == 'radish' || v1 == 'spinach' || " + "v1 == 'tofu' || v1 == 'udon' || v1 == 'vinegar' || "
                        + "v1 == 'waffle' || v1 == 'xouba' || v1 == 'yogurt' || " + "v1 == 'ziti' || v1 == 'acorn' || v1 == 'birch' || "
                        + "v1 == 'cedar' || v1 == 'douglas' || v1 == 'elm' || " + "v1 == 'fir' || v1 == 'ginkgo' || v1 == 'hawthorn' || "
                        + "v1 == 'ironwood' || v1 == 'juniper' || v1 == 'kudzu' || " + "v1 == 'larch' || v1 == 'maple' || v1 == 'nutmeg' || "
                        + "v1 == 'oak' || v1 == 'pine' || v1 == 'quaking' || " + "v1 == 'redwood' || v1 == 'spruce' || v1 == 'teak' || "
                        + "v1 == 'upas' || v1 == 'vinewood' || v1 == 'willow' || " + "v1 == 'xylem' || v1 == 'yellowwood' || v1 == 'zephyr' || "
                        + "v1 == 'amber' || v1 == 'bronze' || v1 == 'copper' || " + "v1 == 'diamond' || v1 == 'emerald' || v1 == 'feldspar' || "
                        + "v1 == 'gold' || v1 == 'hematite' || v1 == 'iron' || " + "v1 == 'jade' || v1 == 'kryptonite' || v1 == 'lapis' || "
                        + "v1 == 'malachite' || v1 == 'nickel' || v1 == 'obsidian' || " + "v1 == 'pyrite' || v1 == 'quartz' || v1 == 'ruby' || "
                        + "v1 == 'sapphire' || v1 == 'topaz' || v1 == 'uranium' || " + "v1 == 'vanadium' || v1 == 'wolfram' || v1 == 'xenotime' || "
                        + "v1 == 'yttrium' || v1 == 'zinc' || v1 == 'argon' || " + "v1 == 'boron' || v1 == 'chlorine' || v1 == 'dysprosium' || "
                        + "v1 == 'erbium' || v1 == 'fluorine' || v1 == 'gallium' || " + "v1 == 'helium' || v1 == 'iodine' || v1 == 'krypton' || "
                        + "v1 == 'lithium' || v1 == 'molybdenum' || v1 == 'neon' || " + "v1 == 'oxygen' || v1 == 'plutonium' || v1 == 'radon' || "
                        + "v1 == 'silicon' || v1 == 'thorium' || v1 == 'uranus' || " + "v1 == 'vanilla' || v1 == 'wolfram' || v1 == 'xenon' || "
                        + "v1 == 'ytterbium' || v1 == 'zirconium' || v1 == 'arch' || " + "v1 == 'bridge' || v1 == 'castle' || v1 == 'dungeon' || "
                        + "v1 == 'estate' || v1 == 'fort' || v1 == 'gateway' || " + "v1 == 'harbor' || v1 == 'island' || v1 == 'jetty' || "
                        + "v1 == 'keep' || v1 == 'lighthouse' || v1 == 'monastery' || " + "v1 == 'nunnery' || v1 == 'observatory' || v1 == 'palace' || "
                        + "v1 == 'quarry' || v1 == 'reservoir' || v1 == 'synagogue' || " + "v1 == 'temple' || v1 == 'university' || v1 == 'vault' || "
                        + "v1 == 'wharf' || v1 == 'xyst' || v1 == 'yard' || " + "v1 == 'ziggurat'";

        JexlNode query = QueryPropertyMarker.create(JexlASTHelper.parseJexlQuery(QueryString), EVALUATION_ONLY);
        Assert.assertEquals(ExecutableDeterminationVisitor.STATE.NON_EXECUTABLE, ExecutableDeterminationVisitor.getState(query, config, helper, output));
        Assert.assertEquals("Summary: NON_EXECUTABLE:[AndNode( delayed/eval only predicate )]", output.get(0));
    }
}
