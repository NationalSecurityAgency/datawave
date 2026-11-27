package datawave.query.jexl.visitors;

import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.EVALUATION_ONLY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.ignoreStubs;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ASTReference;
import org.apache.commons.jexl3.parser.ASTReferenceExpression;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.JexlNodes;
import org.apache.commons.jexl3.parser.ParseException;
import org.apache.commons.jexl3.parser.ParserTreeConstants;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import datawave.query.util.MetadataHelper;

public class ExecutableDeterminationVisitorTest {
    private ShardQueryConfiguration config;
    private MetadataHelper helper;

    @BeforeEach
    public void setup() {
        config = mock(ShardQueryConfiguration.class);
        helper = mock(MetadataHelper.class);

        // TODO: this is wrong. The datatype filter is never null.
        lenient().doReturn(null).when(config).getDatatypeFilter();
    }

    @Test
    public void testNegationErrorCheck() throws ParseException, TableNotFoundException {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery("FOO == 'bar' && !(INDEXONLYFIELD == null)");

        HashSet indexedFields = new HashSet();
        indexedFields.add("INDEXONLYFIELD");
        indexedFields.add("INDEXEDFIELD");
        HashSet<String> indexOnlyFields = new HashSet<>();
        indexOnlyFields.add("INDEXONLYFIELD");

        lenient().doReturn(indexedFields).when(config).getIndexedFields();
        lenient().doReturn(indexOnlyFields).when(helper).getIndexOnlyFields(null);

        LinkedList<String> output = new LinkedList<>();

        boolean executable = ExecutableDeterminationVisitor.isExecutable(query, config, helper, output);
        assertFalse(executable);
        assertEquals("Summary: ERROR:[INDEXONLYFIELD == null]", output.get(0));

        output.clear();
        boolean fiExecutable = ExecutableDeterminationVisitor.isExecutable(query, config, helper, true, output);
        assertFalse(fiExecutable);
        assertEquals("Summary: ERROR:[INDEXONLYFIELD == null]", output.get(0));

        output.clear();
        ExecutableDeterminationVisitor.STATE state = ExecutableDeterminationVisitor.getState(query, config, helper, output);
        assertEquals(ExecutableDeterminationVisitor.STATE.ERROR, state);
        assertEquals("Summary: ERROR:[INDEXONLYFIELD == null]", output.get(0));

        output.clear();
        ExecutableDeterminationVisitor.STATE fiState = ExecutableDeterminationVisitor.getState(query, config, helper, true, output);
        assertEquals(ExecutableDeterminationVisitor.STATE.ERROR, fiState);
        assertEquals("Summary: ERROR:[INDEXONLYFIELD == null]", output.get(0));

        if (config != null && mockingDetails(config).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(config));
        }
        if (helper != null && mockingDetails(helper).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(helper));
        }
    }

    @Test
    public void testIndexOnlyEqNull() throws ParseException, TableNotFoundException {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery("INDEXONLYFIELD == null");

        HashSet indexedFields = new HashSet();
        indexedFields.add("INDEXONLYFIELD");

        HashSet<String> indexOnlyFields = new HashSet<>();
        indexOnlyFields.add("INDEXONLYFIELD");

        lenient().doReturn(indexedFields).when(config).getIndexedFields();
        lenient().doReturn(indexOnlyFields).when(helper).getIndexOnlyFields(null);

        LinkedList<String> output = new LinkedList<>();

        boolean executable = ExecutableDeterminationVisitor.isExecutable(query, config, helper, output);
        assertFalse(executable);
        assertEquals("Summary: ERROR:[INDEXONLYFIELD == null]", output.get(0));

        output.clear();
        boolean fiExecutable = ExecutableDeterminationVisitor.isExecutable(query, config, helper, true, output);
        assertFalse(fiExecutable);
        assertEquals("Summary: ERROR:[INDEXONLYFIELD == null]", output.get(0));

        output.clear();
        ExecutableDeterminationVisitor.STATE state = ExecutableDeterminationVisitor.getState(query, config, helper, output);
        assertEquals(ExecutableDeterminationVisitor.STATE.ERROR, state);
        assertEquals("Summary: ERROR:[INDEXONLYFIELD == null]", output.get(0));

        output.clear();
        ExecutableDeterminationVisitor.STATE fiState = ExecutableDeterminationVisitor.getState(query, config, helper, true, output);
        assertEquals(ExecutableDeterminationVisitor.STATE.ERROR, fiState);
        assertEquals("Summary: ERROR:[INDEXONLYFIELD == null]", output.get(0));

        if (config != null && mockingDetails(config).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(config));
        }
        if (helper != null && mockingDetails(helper).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(helper));
        }
    }

    @Test
    public void testIndexOnlyNeNull() throws ParseException, TableNotFoundException {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery("INDEXONLYFIELD != null");

        LinkedList<String> output = new LinkedList<>();

        HashSet indexedFields = new HashSet();
        indexedFields.add("INDEXONLYFIELD");

        HashSet<String> indexOnlyFields = new HashSet<>();
        indexOnlyFields.add("INDEXONLYFIELD");

        lenient().doReturn(indexedFields).when(config).getIndexedFields();
        lenient().doReturn(indexOnlyFields).when(helper).getIndexOnlyFields(null);

        boolean executable = ExecutableDeterminationVisitor.isExecutable(query, config, helper, output);
        assertFalse(executable);
        assertEquals("Summary: ERROR:[INDEXONLYFIELD != null]", output.get(0));

        output.clear();
        boolean fiExecutable = ExecutableDeterminationVisitor.isExecutable(query, config, helper, true, output);
        assertFalse(fiExecutable);
        assertEquals("Summary: ERROR:[INDEXONLYFIELD != null]", output.get(0));

        output.clear();
        ExecutableDeterminationVisitor.STATE state = ExecutableDeterminationVisitor.getState(query, config, helper, output);
        assertEquals(ExecutableDeterminationVisitor.STATE.ERROR, state);
        assertEquals("Summary: ERROR:[INDEXONLYFIELD != null]", output.get(0));

        output.clear();
        ExecutableDeterminationVisitor.STATE fiState = ExecutableDeterminationVisitor.getState(query, config, helper, true, output);
        assertEquals(ExecutableDeterminationVisitor.STATE.ERROR, fiState);
        assertEquals("Summary: ERROR:[INDEXONLYFIELD != null]", output.get(0));

        if (config != null && mockingDetails(config).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(config));
        }
        if (helper != null && mockingDetails(helper).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(helper));
        }
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

        lenient().doReturn(indexedFields).when(config).getIndexedFields();
        lenient().doReturn(indexOnlyFields).when(helper).getIndexOnlyFields(null);

        boolean executable = ExecutableDeterminationVisitor.isExecutable(query, config, helper, output);
        assertFalse(executable);
        assertEquals("Summary: NON_EXECUTABLE:[INDEXEDFIELD == null]", output.get(0));

        output.clear();
        boolean fiExecutable = ExecutableDeterminationVisitor.isExecutable(query, config, helper, true, output);
        assertFalse(fiExecutable);
        assertEquals("Summary: NON_EXECUTABLE:[INDEXEDFIELD == null]", output.get(0));

        output.clear();
        ExecutableDeterminationVisitor.STATE state = ExecutableDeterminationVisitor.getState(query, config, helper, output);
        assertEquals(ExecutableDeterminationVisitor.STATE.NON_EXECUTABLE, state);
        assertEquals("Summary: NON_EXECUTABLE:[INDEXEDFIELD == null]", output.get(0));

        output.clear();
        ExecutableDeterminationVisitor.STATE fiState = ExecutableDeterminationVisitor.getState(query, config, helper, true, output);
        assertEquals(ExecutableDeterminationVisitor.STATE.NON_EXECUTABLE, fiState);
        assertEquals("Summary: NON_EXECUTABLE:[INDEXEDFIELD == null]", output.get(0));

        if (config != null && mockingDetails(config).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(config));
        }
        if (helper != null && mockingDetails(helper).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(helper));
        }
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

        lenient().doReturn(indexedFields).when(config).getIndexedFields();
        lenient().doReturn(indexOnlyFields).when(helper).getIndexOnlyFields(null);

        boolean executable = ExecutableDeterminationVisitor.isExecutable(query, config, helper, output);
        assertFalse(executable);
        assertEquals("Summary: NON_EXECUTABLE:[INDEXEDFIELD != null]", output.get(0));

        output.clear();
        boolean fiExecutable = ExecutableDeterminationVisitor.isExecutable(query, config, helper, true, output);
        assertFalse(fiExecutable);
        assertEquals("Summary: NON_EXECUTABLE:[INDEXEDFIELD != null]", output.get(0));

        output.clear();
        ExecutableDeterminationVisitor.STATE state = ExecutableDeterminationVisitor.getState(query, config, helper, output);
        assertEquals(ExecutableDeterminationVisitor.STATE.NON_EXECUTABLE, state);
        assertEquals("Summary: NON_EXECUTABLE:[INDEXEDFIELD != null]", output.get(0));

        output.clear();
        ExecutableDeterminationVisitor.STATE fiState = ExecutableDeterminationVisitor.getState(query, config, helper, true, output);
        assertEquals(ExecutableDeterminationVisitor.STATE.NON_EXECUTABLE, fiState);
        assertEquals("Summary: NON_EXECUTABLE:[INDEXEDFIELD != null]", output.get(0));

        if (config != null && mockingDetails(config).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(config));
        }
        if (helper != null && mockingDetails(helper).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(helper));
        }
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

        lenient().doReturn(indexedFields).when(config).getIndexedFields();
        lenient().doReturn(indexOnlyFields).when(helper).getIndexOnlyFields(null);

        boolean executable = ExecutableDeterminationVisitor.isExecutable(query, config, helper, output);
        assertFalse(executable);
        assertEquals("Summary: NON_EXECUTABLE:[INDEXONLYFIELD == 'b', INDEXEDFIELD == 'c', INDEXEDFIELD == 'a']", output.get(0));

        output.clear();
        boolean fiExecutable = ExecutableDeterminationVisitor.isExecutable(query, config, helper, true, output);
        assertFalse(fiExecutable);
        assertEquals("Summary: NEGATED_EXECUTABLE:[INDEXONLYFIELD == 'b', INDEXEDFIELD == 'c', INDEXEDFIELD == 'a']", output.get(0));

        output.clear();
        ExecutableDeterminationVisitor.STATE state = ExecutableDeterminationVisitor.getState(query, config, helper, output);
        assertEquals(ExecutableDeterminationVisitor.STATE.PARTIAL, state);
        assertEquals("Summary: NON_EXECUTABLE:[INDEXONLYFIELD == 'b', INDEXEDFIELD == 'c', INDEXEDFIELD == 'a']", output.get(0));

        output.clear();
        ExecutableDeterminationVisitor.STATE fiState = ExecutableDeterminationVisitor.getState(query, config, helper, true, output);
        assertEquals(ExecutableDeterminationVisitor.STATE.NEGATED_EXECUTABLE, fiState);
        assertEquals("Summary: NEGATED_EXECUTABLE:[INDEXONLYFIELD == 'b', INDEXEDFIELD == 'c', INDEXEDFIELD == 'a']", output.get(0));

        if (config != null && mockingDetails(config).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(config));
        }
        if (helper != null && mockingDetails(helper).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(helper));
        }
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

        lenient().doReturn(indexedFields).when(config).getIndexedFields();
        lenient().doReturn(indexOnlyFields).when(helper).getIndexOnlyFields(null);

        boolean executable = ExecutableDeterminationVisitor.isExecutable(query, config, helper, output);
        assertTrue(executable);
        assertEquals("Summary: EXECUTABLE:[INDEXONLYFIELD == 'b', INDEXEDFIELD == 'c', INDEXEDFIELD == 'a']", output.get(0));

        output.clear();
        boolean fiExecutable = ExecutableDeterminationVisitor.isExecutable(query, config, helper, true, output);
        assertTrue(fiExecutable);
        assertEquals("Summary: EXECUTABLE:[INDEXONLYFIELD == 'b', INDEXEDFIELD == 'c', INDEXEDFIELD == 'a']", output.get(0));

        output.clear();
        ExecutableDeterminationVisitor.STATE state = ExecutableDeterminationVisitor.getState(query, config, helper, output);
        assertEquals(ExecutableDeterminationVisitor.STATE.EXECUTABLE, state);
        assertEquals("Summary: EXECUTABLE:[INDEXONLYFIELD == 'b', INDEXEDFIELD == 'c', INDEXEDFIELD == 'a']", output.get(0));

        output.clear();
        ExecutableDeterminationVisitor.STATE fiState = ExecutableDeterminationVisitor.getState(query, config, helper, true, output);
        assertEquals(ExecutableDeterminationVisitor.STATE.EXECUTABLE, fiState);
        assertEquals("Summary: EXECUTABLE:[INDEXONLYFIELD == 'b', INDEXEDFIELD == 'c', INDEXEDFIELD == 'a']", output.get(0));

        if (config != null && mockingDetails(config).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(config));
        }
        if (helper != null && mockingDetails(helper).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(helper));
        }
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

        lenient().doReturn(indexedFields).when(config).getIndexedFields();
        lenient().doReturn(indexOnlyFields).when(helper).getIndexOnlyFields(null);

        boolean executable = ExecutableDeterminationVisitor.isExecutable(query, config, helper, output);
        assertFalse(executable);
        assertEquals("Summary: NON_EXECUTABLE:[INDEXONLYFIELD == 'b', INDEXEDFIELD == 'a', filter:includeRegex(INDEXEDFIELD, '.*')]", output.get(0));

        output.clear();
        boolean fiExecutable = ExecutableDeterminationVisitor.isExecutable(query, config, helper, true, output);
        assertFalse(fiExecutable);
        assertEquals("Summary: NON_EXECUTABLE:[filter:includeRegex(INDEXEDFIELD, '.*')]", output.get(0));

        output.clear();
        ExecutableDeterminationVisitor.STATE state = ExecutableDeterminationVisitor.getState(query, config, helper, output);
        assertEquals(ExecutableDeterminationVisitor.STATE.PARTIAL, state);
        assertEquals("Summary: NON_EXECUTABLE:[INDEXONLYFIELD == 'b', INDEXEDFIELD == 'a', filter:includeRegex(INDEXEDFIELD, '.*')]", output.get(0));

        output.clear();
        ExecutableDeterminationVisitor.STATE fiState = ExecutableDeterminationVisitor.getState(query, config, helper, true, output);
        assertEquals(ExecutableDeterminationVisitor.STATE.PARTIAL, fiState);
        assertEquals("Summary: NON_EXECUTABLE:[filter:includeRegex(INDEXEDFIELD, '.*')]", output.get(0));

        if (config != null && mockingDetails(config).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(config));
        }
        if (helper != null && mockingDetails(helper).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(helper));
        }
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

        lenient().doReturn(indexedFields).when(config).getIndexedFields();
        lenient().doReturn(indexOnlyFields).when(helper).getIndexOnlyFields(null);

        boolean executable = ExecutableDeterminationVisitor.isExecutable(query, config, helper, output);
        assertTrue(executable);
        assertEquals("Summary: EXECUTABLE:[INDEXONLYFIELD == 'b', INDEXEDFIELD == 'a']", output.get(0));

        output.clear();
        boolean fiExecutable = ExecutableDeterminationVisitor.isExecutable(query, config, helper, true, output);
        assertTrue(fiExecutable);
        assertEquals("Summary: EXECUTABLE:[INDEXONLYFIELD == 'b', INDEXEDFIELD == 'a']", output.get(0));

        output.clear();
        ExecutableDeterminationVisitor.STATE state = ExecutableDeterminationVisitor.getState(query, config, helper, output);
        assertEquals(ExecutableDeterminationVisitor.STATE.EXECUTABLE, state);
        assertEquals("Summary: EXECUTABLE:[INDEXONLYFIELD == 'b', INDEXEDFIELD == 'a']", output.get(0));

        output.clear();
        ExecutableDeterminationVisitor.STATE fiState = ExecutableDeterminationVisitor.getState(query, config, helper, true, output);
        assertEquals(ExecutableDeterminationVisitor.STATE.EXECUTABLE, fiState);
        assertEquals("Summary: EXECUTABLE:[INDEXONLYFIELD == 'b', INDEXEDFIELD == 'a']", output.get(0));

        if (config != null && mockingDetails(config).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(config));
        }
        if (helper != null && mockingDetails(helper).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(helper));
        }
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

        lenient().doReturn(indexedFields).when(config).getIndexedFields();
        lenient().doReturn(indexOnlyFields).when(helper).getIndexOnlyFields(null);

        boolean executable = ExecutableDeterminationVisitor.isExecutable(query, config, helper, output);
        assertFalse(executable);
        assertEquals("Summary: NON_EXECUTABLE:[INDEXONLYFIELD == 'b', filter:includeRegex(INDEXEDFIELD, '.*')]", output.get(0));

        output.clear();
        boolean fiExecutable = ExecutableDeterminationVisitor.isExecutable(query, config, helper, true, output);
        assertFalse(fiExecutable);
        assertTrue(Arrays.asList(new String[] {"Summary: EXECUTABLE:[INDEXEDFIELD == 'a']; NEGATED_EXECUTABLE:[INDEXONLYFIELD == 'b']",
                "Summary: NEGATED_EXECUTABLE:[INDEXONLYFIELD == 'b']; EXECUTABLE:[INDEXEDFIELD == 'a']"}).contains(output.get(0)));

        output.clear();
        ExecutableDeterminationVisitor.STATE state = ExecutableDeterminationVisitor.getState(query, config, helper, output);
        assertEquals(ExecutableDeterminationVisitor.STATE.PARTIAL, state);
        assertEquals("Summary: NON_EXECUTABLE:[INDEXONLYFIELD == 'b', filter:includeRegex(INDEXEDFIELD, '.*')]", output.get(0));

        output.clear();
        ExecutableDeterminationVisitor.STATE fiState = ExecutableDeterminationVisitor.getState(query, config, helper, true, output);
        assertEquals(ExecutableDeterminationVisitor.STATE.NEGATED_EXECUTABLE, fiState);
        assertTrue(Arrays.asList(new String[] {"Summary: EXECUTABLE:[INDEXEDFIELD == 'a']; NEGATED_EXECUTABLE:[INDEXONLYFIELD == 'b']",
                "Summary: NEGATED_EXECUTABLE:[INDEXONLYFIELD == 'b']; EXECUTABLE:[INDEXEDFIELD == 'a']"}).contains(output.get(0)));

        if (config != null && mockingDetails(config).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(config));
        }
        if (helper != null && mockingDetails(helper).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(helper));
        }
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

        lenient().doReturn(indexedFields).when(config).getIndexedFields();
        lenient().doReturn(indexOnlyFields).when(helper).getIndexOnlyFields(null);

        boolean executable = ExecutableDeterminationVisitor.isExecutable(query, config, helper, output);
        assertFalse(executable);
        assertEquals("Summary: NON_EXECUTABLE:[filter:includeRegex(INDEXEDFIELD, '.*')]", output.get(0));

        output.clear();
        boolean fiExecutable = ExecutableDeterminationVisitor.isExecutable(query, config, helper, true, output);
        assertFalse(fiExecutable);
        assertEquals("Summary: NON_EXECUTABLE:[filter:includeRegex(INDEXEDFIELD, '.*')]", output.get(0));

        output.clear();
        ExecutableDeterminationVisitor.STATE state = ExecutableDeterminationVisitor.getState(query, config, helper, output);
        assertEquals(ExecutableDeterminationVisitor.STATE.PARTIAL, state);
        assertEquals("Summary: NON_EXECUTABLE:[filter:includeRegex(INDEXEDFIELD, '.*')]", output.get(0));

        output.clear();
        ExecutableDeterminationVisitor.STATE fiState = ExecutableDeterminationVisitor.getState(query, config, helper, true, output);
        assertEquals(ExecutableDeterminationVisitor.STATE.PARTIAL, fiState);
        assertEquals("Summary: NON_EXECUTABLE:[filter:includeRegex(INDEXEDFIELD, '.*')]", output.get(0));

        if (config != null && mockingDetails(config).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(config));
        }
        if (helper != null && mockingDetails(helper).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(helper));
        }
    }

    @Test
    public void testNegationEvaluation() {
        assertFalse(ExecutableDeterminationVisitor.isNegated(null));
        assertFalse(ExecutableDeterminationVisitor.isNegated(""));
        assertFalse(ExecutableDeterminationVisitor.isNegated("!!"));
        assertFalse(ExecutableDeterminationVisitor.isNegated("            !                    !    "));
        assertFalse(ExecutableDeterminationVisitor.isNegated("!           !   !  !    !        !    !    !"));

        assertTrue(ExecutableDeterminationVisitor.isNegated("!!!"));
        assertTrue(ExecutableDeterminationVisitor.isNegated("            !                    !    !"));
        assertTrue(ExecutableDeterminationVisitor.isNegated("!                                !    !"));
        assertTrue(ExecutableDeterminationVisitor.isNegated("!           !   !  !    !            !    !"));
        assertTrue(ExecutableDeterminationVisitor.isNegated("!           !   !  !    !            !    !             "));
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

        lenient().doReturn(indexedFields).when(config).getIndexedFields();
        lenient().doReturn(indexOnlyFields).when(helper).getIndexOnlyFields(null);

        assertEquals(ExecutableDeterminationVisitor.STATE.IGNORABLE, ExecutableDeterminationVisitor.getState(andNode, config, helper, output));
        assertEquals("Summary: ", output.get(0)); // no summary because the query is empty

        if (config != null && mockingDetails(config).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(config));
        }
        if (helper != null && mockingDetails(helper).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(helper));
        }
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

        lenient().doReturn(indexedFields).when(config).getIndexedFields();
        lenient().doReturn(indexOnlyFields).when(helper).getIndexOnlyFields(null);

        assertEquals(ExecutableDeterminationVisitor.STATE.EXECUTABLE, ExecutableDeterminationVisitor.getState(andNode, config, helper, output));
        assertEquals("Summary: EXECUTABLE:[INDEXEDFIELD == 'v2', INDEXEDFIELD == 'v1']", output.get(0));

        if (config != null && mockingDetails(config).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(config));
        }
        if (helper != null && mockingDetails(helper).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(helper));
        }
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

        lenient().doReturn(indexedFields).when(config).getIndexedFields();
        lenient().doReturn(indexOnlyFields).when(helper).getIndexOnlyFields(null);

        assertEquals(ExecutableDeterminationVisitor.STATE.IGNORABLE, ExecutableDeterminationVisitor.getState(orNode, config, helper, output));
        assertEquals("Summary: ", output.get(0)); // empty summary because the query is empty

        if (config != null && mockingDetails(config).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(config));
        }
        if (helper != null && mockingDetails(helper).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(helper));
        }
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

        lenient().doReturn(indexedFields).when(config).getIndexedFields();
        lenient().doReturn(indexOnlyFields).when(helper).getIndexOnlyFields(null);

        assertEquals(ExecutableDeterminationVisitor.STATE.EXECUTABLE, ExecutableDeterminationVisitor.getState(orNode, config, helper, output));
        assertEquals("Summary: EXECUTABLE:[INDEXEDFIELD == 'v2', INDEXEDFIELD == 'v1']", output.get(0));

        if (config != null && mockingDetails(config).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(config));
        }
        if (helper != null && mockingDetails(helper).isMock()) {
            verifyNoMoreInteractions(ignoreStubs(helper));
        }
    }

    @Test
    public void testEvaluationOnlyReferenceNode() throws ParseException, TableNotFoundException {
        doReturn(Collections.emptySet()).when(helper).getIndexOnlyFields(null);

        LinkedList<String> output = new LinkedList<>();

        JexlNode query = QueryPropertyMarker.create(JexlASTHelper.parseJexlQuery("FOO == FOO2"), EVALUATION_ONLY);
        assertEquals(ExecutableDeterminationVisitor.STATE.NON_EXECUTABLE, ExecutableDeterminationVisitor.getState(query, config, helper, output));
        assertEquals("Summary: NON_EXECUTABLE:[AndNode( delayed/eval only predicate )]", output.get(0));
    }

    /**
     * Verify that delayed and evaluation-only terms against _NOFIELD_ are ignorable, like their undelayed counterparts.
     */
    @Test
    public void testDelayedNoFieldTermsAreIgnorable() throws ParseException, TableNotFoundException {
        HashSet<String> indexedFields = new HashSet<>();
        indexedFields.add("INDEXEDFIELD");

        EasyMock.expect(config.getIndexedFields()).andReturn(indexedFields).anyTimes();
        EasyMock.expect(helper.getIndexOnlyFields(null)).andReturn(Collections.emptySet()).anyTimes();

        replayAll();

        assertNoFieldMarkerState("((_Delayed_ = true) && (_NOFIELD_ == 'a'))", ExecutableDeterminationVisitor.STATE.IGNORABLE,
                        "Summary: IGNORABLE:[AndNode( delayed/eval only predicate )]");
        assertNoFieldMarkerState("((_Eval_ = true) && (_NOFIELD_ == 'a'))", ExecutableDeterminationVisitor.STATE.IGNORABLE,
                        "Summary: IGNORABLE:[AndNode( delayed/eval only predicate )]");

        // an ignorable delayed term should not prevent a disjunction from being executable
        assertNoFieldMarkerState("INDEXEDFIELD == 'a' || ((_Delayed_ = true) && (_NOFIELD_ == 'b'))", ExecutableDeterminationVisitor.STATE.EXECUTABLE,
                        "Summary: EXECUTABLE:[INDEXEDFIELD == 'a']");

        // a delayed expression that is not solely against _NOFIELD_ is still non-executable
        assertNoFieldMarkerState("((_Delayed_ = true) && (_NOFIELD_ == 'a' && INDEXEDFIELD == 'b'))", ExecutableDeterminationVisitor.STATE.NON_EXECUTABLE,
                        "Summary: NON_EXECUTABLE:[AndNode( delayed/eval only predicate )]");

        verifyAll();
    }

    private void assertNoFieldMarkerState(String query, ExecutableDeterminationVisitor.STATE expectedState, String expectedSummary) throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
        for (boolean forFieldIndex : new boolean[] {false, true}) {
            LinkedList<String> output = new LinkedList<>();
            assertEquals(expectedState, ExecutableDeterminationVisitor.getState(script, config, helper, forFieldIndex, output), query);
            assertEquals(expectedSummary, output.get(0), query);
        }
    }
}
