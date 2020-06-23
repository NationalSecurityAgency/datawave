package datawave.query.jexl.visitors;

import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.util.MetadataHelper;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ParseException;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;

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
        
        boolean executable = ExecutableDeterminationVisitor.isExecutable(query, config, helper);
        Assert.assertFalse(executable);
        
        boolean fiExecutable = ExecutableDeterminationVisitor.isExecutable(query, config, helper, true);
        Assert.assertFalse(fiExecutable);
        
        ExecutableDeterminationVisitor.STATE state = ExecutableDeterminationVisitor.getState(query, config, helper);
        Assert.assertEquals(ExecutableDeterminationVisitor.STATE.ERROR, state);
        
        ExecutableDeterminationVisitor.STATE fiState = ExecutableDeterminationVisitor.getState(query, config, helper, true);
        Assert.assertEquals(ExecutableDeterminationVisitor.STATE.ERROR, fiState);
        
        verifyAll();
    }
    
    public void testIndexOnlyEqNull() throws ParseException, TableNotFoundException {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery("INDEXONLYFIELD == null");
        
        HashSet indexedFields = new HashSet();
        indexedFields.add("INDEXONLYFIELD");
        
        HashSet<String> indexOnlyFields = new HashSet<>();
        indexOnlyFields.add("INDEXONLYFIELD");
        
        EasyMock.expect(config.getIndexedFields()).andReturn(indexedFields).anyTimes();
        EasyMock.expect(helper.getIndexOnlyFields(null)).andReturn(indexOnlyFields).anyTimes();
        
        replayAll();
        
        boolean executable = ExecutableDeterminationVisitor.isExecutable(query, config, helper);
        Assert.assertFalse(executable);
        
        boolean fiExecutable = ExecutableDeterminationVisitor.isExecutable(query, config, helper, true);
        Assert.assertFalse(fiExecutable);
        
        ExecutableDeterminationVisitor.STATE state = ExecutableDeterminationVisitor.getState(query, config, helper);
        Assert.assertEquals(ExecutableDeterminationVisitor.STATE.ERROR, state);
        
        ExecutableDeterminationVisitor.STATE fiState = ExecutableDeterminationVisitor.getState(query, config, helper, true);
        Assert.assertEquals(ExecutableDeterminationVisitor.STATE.ERROR, fiState);
        
        verifyAll();
    }
    
    @Test
    public void testIndexOnlyNeNull() throws ParseException, TableNotFoundException {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery("INDEXONLYFIELD != null");
        
        HashSet indexedFields = new HashSet();
        indexedFields.add("INDEXONLYFIELD");
        
        HashSet<String> indexOnlyFields = new HashSet<>();
        indexOnlyFields.add("INDEXONLYFIELD");
        
        EasyMock.expect(config.getIndexedFields()).andReturn(indexedFields).anyTimes();
        EasyMock.expect(helper.getIndexOnlyFields(null)).andReturn(indexOnlyFields).anyTimes();
        
        replayAll();
        
        boolean executable = ExecutableDeterminationVisitor.isExecutable(query, config, helper);
        Assert.assertFalse(executable);
        
        boolean fiExecutable = ExecutableDeterminationVisitor.isExecutable(query, config, helper, true);
        Assert.assertFalse(fiExecutable);
        
        ExecutableDeterminationVisitor.STATE state = ExecutableDeterminationVisitor.getState(query, config, helper);
        Assert.assertEquals(ExecutableDeterminationVisitor.STATE.ERROR, state);
        
        ExecutableDeterminationVisitor.STATE fiState = ExecutableDeterminationVisitor.getState(query, config, helper, true);
        Assert.assertEquals(ExecutableDeterminationVisitor.STATE.ERROR, fiState);
        
        verifyAll();
    }
    
    @Test
    public void testIndexedEqNull() throws ParseException, TableNotFoundException {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery("INDEXEDFIELD == null");
        
        HashSet indexedFields = new HashSet();
        indexedFields.add("INDEXONLYFIELD");
        indexedFields.add("INDEXEDFIELD");
        
        HashSet<String> indexOnlyFields = new HashSet<>();
        indexOnlyFields.add("INDEXONLYFIELD");
        
        EasyMock.expect(config.getIndexedFields()).andReturn(indexedFields).anyTimes();
        EasyMock.expect(helper.getIndexOnlyFields(null)).andReturn(indexOnlyFields).anyTimes();
        
        replayAll();
        
        boolean executable = ExecutableDeterminationVisitor.isExecutable(query, config, helper);
        Assert.assertFalse(executable);
        
        boolean fiExecutable = ExecutableDeterminationVisitor.isExecutable(query, config, helper, true);
        Assert.assertFalse(fiExecutable);
        
        ExecutableDeterminationVisitor.STATE state = ExecutableDeterminationVisitor.getState(query, config, helper);
        Assert.assertEquals(ExecutableDeterminationVisitor.STATE.NON_EXECUTABLE, state);
        
        ExecutableDeterminationVisitor.STATE fiState = ExecutableDeterminationVisitor.getState(query, config, helper, true);
        Assert.assertEquals(ExecutableDeterminationVisitor.STATE.NON_EXECUTABLE, fiState);
        
        verifyAll();
    }
    
    @Test
    public void testIndexedNeNull() throws ParseException, TableNotFoundException {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery("INDEXEDFIELD != null");
        
        HashSet indexedFields = new HashSet();
        indexedFields.add("INDEXONLYFIELD");
        indexedFields.add("INDEXEDFIELD");
        
        HashSet<String> indexOnlyFields = new HashSet<>();
        indexOnlyFields.add("INDEXONLYFIELD");
        
        EasyMock.expect(config.getIndexedFields()).andReturn(indexedFields).anyTimes();
        EasyMock.expect(helper.getIndexOnlyFields(null)).andReturn(indexOnlyFields).anyTimes();
        
        replayAll();
        
        boolean executable = ExecutableDeterminationVisitor.isExecutable(query, config, helper);
        Assert.assertFalse(executable);
        
        boolean fiExecutable = ExecutableDeterminationVisitor.isExecutable(query, config, helper, true);
        Assert.assertFalse(fiExecutable);
        
        ExecutableDeterminationVisitor.STATE state = ExecutableDeterminationVisitor.getState(query, config, helper);
        Assert.assertEquals(ExecutableDeterminationVisitor.STATE.NON_EXECUTABLE, state);
        
        ExecutableDeterminationVisitor.STATE fiState = ExecutableDeterminationVisitor.getState(query, config, helper, true);
        Assert.assertEquals(ExecutableDeterminationVisitor.STATE.NON_EXECUTABLE, fiState);
        
        verifyAll();
    }
    
    /**
     * Expected outputs Global Index: EXECUTABLE || NOT_EXECUTABLE == PARTIAL Field Index: EXECUTABLE || !(EXECUTABLE && EXECUTABLE) == EXECUTABLE || EXECUTABLE
     * == EXECUTABLE
     * 
     * @throws Exception
     */
    @Test
    public void testNegatedAndExecutable() throws Exception {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery("INDEXEDFIELD == 'a' || !(INDEXONLYFIELD == 'b' && INDEXEDFIELD == 'c')");
        
        HashSet indexedFields = new HashSet();
        indexedFields.add("INDEXONLYFIELD");
        indexedFields.add("INDEXEDFIELD");
        
        HashSet<String> indexOnlyFields = new HashSet<>();
        indexOnlyFields.add("INDEXONLYFIELD");
        
        EasyMock.expect(config.getIndexedFields()).andReturn(indexedFields).anyTimes();
        EasyMock.expect(helper.getIndexOnlyFields(null)).andReturn(indexOnlyFields).anyTimes();
        
        replayAll();
        
        boolean executable = ExecutableDeterminationVisitor.isExecutable(query, config, helper);
        Assert.assertFalse(executable);
        
        boolean fiExecutable = ExecutableDeterminationVisitor.isExecutable(query, config, helper, true);
        Assert.assertFalse(fiExecutable);
        
        ExecutableDeterminationVisitor.STATE state = ExecutableDeterminationVisitor.getState(query, config, helper);
        Assert.assertEquals(ExecutableDeterminationVisitor.STATE.PARTIAL, state);
        
        ExecutableDeterminationVisitor.STATE fiState = ExecutableDeterminationVisitor.getState(query, config, helper, true);
        Assert.assertEquals(ExecutableDeterminationVisitor.STATE.NEGATED_EXECUTABLE, fiState);
        
        verifyAll();
    }
    
    /**
     * Expected outputs Global Index: EXECUTABLE || NOT_EXECUTABLE == PARTIAL Field Index: EXECUTABLE || !!(EXECUTABLE && EXECUTABLE) == EXECUTABLE ||
     * EXECUTABLE == EXECUTABLE
     * 
     * @throws Exception
     */
    @Test
    public void testDoubleNegatedAndExecutable() throws Exception {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery("INDEXEDFIELD == 'a' || !!(INDEXONLYFIELD == 'b' && INDEXEDFIELD == 'c')");
        
        HashSet indexedFields = new HashSet();
        indexedFields.add("INDEXONLYFIELD");
        indexedFields.add("INDEXEDFIELD");
        
        HashSet<String> indexOnlyFields = new HashSet<>();
        indexOnlyFields.add("INDEXONLYFIELD");
        
        EasyMock.expect(config.getIndexedFields()).andReturn(indexedFields).anyTimes();
        EasyMock.expect(helper.getIndexOnlyFields(null)).andReturn(indexOnlyFields).anyTimes();
        
        replayAll();
        
        boolean executable = ExecutableDeterminationVisitor.isExecutable(query, config, helper);
        Assert.assertTrue(executable);
        
        boolean fiExecutable = ExecutableDeterminationVisitor.isExecutable(query, config, helper, true);
        Assert.assertTrue(fiExecutable);
        
        ExecutableDeterminationVisitor.STATE state = ExecutableDeterminationVisitor.getState(query, config, helper);
        Assert.assertEquals(ExecutableDeterminationVisitor.STATE.EXECUTABLE, state);
        
        ExecutableDeterminationVisitor.STATE fiState = ExecutableDeterminationVisitor.getState(query, config, helper, true);
        Assert.assertEquals(ExecutableDeterminationVisitor.STATE.EXECUTABLE, fiState);
        
        verifyAll();
    }
    
    /**
     * Expected outputs Global Index: EXECUTABLE || NOT_EXECUTABLE == PARTIAL Field Index: EXECUTABLE || !(EXECUTABLE && NOT_EXECUTABLE) == EXECUTABLE ||
     * NOT_EXECUTABLE == PARTIAL
     * 
     * @throws Exception
     */
    @Test
    public void testNegatedAndNotExecutable() throws Exception {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery("INDEXEDFIELD == 'a' || !(INDEXONLYFIELD == 'b' && filter:includeRegex(INDEXEDFIELD,'.*'))");
        
        HashSet indexedFields = new HashSet();
        indexedFields.add("INDEXONLYFIELD");
        indexedFields.add("INDEXEDFIELD");
        
        HashSet<String> indexOnlyFields = new HashSet<>();
        indexOnlyFields.add("INDEXONLYFIELD");
        
        EasyMock.expect(config.getIndexedFields()).andReturn(indexedFields).anyTimes();
        EasyMock.expect(helper.getIndexOnlyFields(null)).andReturn(indexOnlyFields).anyTimes();
        
        replayAll();
        
        boolean executable = ExecutableDeterminationVisitor.isExecutable(query, config, helper);
        Assert.assertFalse(executable);
        
        boolean fiExecutable = ExecutableDeterminationVisitor.isExecutable(query, config, helper, true);
        Assert.assertFalse(fiExecutable);
        
        ExecutableDeterminationVisitor.STATE state = ExecutableDeterminationVisitor.getState(query, config, helper);
        Assert.assertEquals(ExecutableDeterminationVisitor.STATE.PARTIAL, state);
        
        ExecutableDeterminationVisitor.STATE fiState = ExecutableDeterminationVisitor.getState(query, config, helper, true);
        Assert.assertEquals(ExecutableDeterminationVisitor.STATE.PARTIAL, fiState);
        
        verifyAll();
    }
    
    /**
     * Expected outputs Global Index: EXECUTABLE || NOT_EXECUTABLE == PARTIAL Field Index: EXECUTABLE || !!(EXECUTABLE && NOT_EXECUTABLE) == EXECUTABLE ||
     * EXECUTABLE == EXECUTABLE
     * 
     * @throws Exception
     */
    @Test
    public void testDoubleNegatedAndNotExecutable() throws Exception {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery("INDEXEDFIELD == 'a' || !!(INDEXONLYFIELD == 'b' && filter:includeRegex(INDEXEDFIELD,'.*'))");
        
        HashSet indexedFields = new HashSet();
        indexedFields.add("INDEXONLYFIELD");
        indexedFields.add("INDEXEDFIELD");
        
        HashSet<String> indexOnlyFields = new HashSet<>();
        indexOnlyFields.add("INDEXONLYFIELD");
        
        EasyMock.expect(config.getIndexedFields()).andReturn(indexedFields).anyTimes();
        EasyMock.expect(helper.getIndexOnlyFields(null)).andReturn(indexOnlyFields).anyTimes();
        
        replayAll();
        
        boolean executable = ExecutableDeterminationVisitor.isExecutable(query, config, helper);
        Assert.assertTrue(executable);
        
        boolean fiExecutable = ExecutableDeterminationVisitor.isExecutable(query, config, helper, true);
        Assert.assertTrue(fiExecutable);
        
        ExecutableDeterminationVisitor.STATE state = ExecutableDeterminationVisitor.getState(query, config, helper);
        Assert.assertEquals(ExecutableDeterminationVisitor.STATE.EXECUTABLE, state);
        
        ExecutableDeterminationVisitor.STATE fiState = ExecutableDeterminationVisitor.getState(query, config, helper, true);
        Assert.assertEquals(ExecutableDeterminationVisitor.STATE.EXECUTABLE, fiState);
        
        verifyAll();
    }
    
    /**
     * Expected outputs Global Index: EXECUTABLE || NOT_EXECUTABLE == PARTIAL Field Index: EXECUTABLE || !(EXECUTABLE || NOT_EXECUTABLE) == EXECUTABLE ||
     * EXECUTABLE == EXECUTABLE
     * 
     * @throws Exception
     */
    @Test
    public void testNegatedOrExecutable() throws Exception {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery("INDEXEDFIELD == 'a' || !(INDEXONLYFIELD == 'b' || filter:includeRegex(INDEXEDFIELD,'.*'))");
        
        HashSet indexedFields = new HashSet();
        indexedFields.add("INDEXONLYFIELD");
        indexedFields.add("INDEXEDFIELD");
        
        HashSet<String> indexOnlyFields = new HashSet<>();
        indexOnlyFields.add("INDEXONLYFIELD");
        
        EasyMock.expect(config.getIndexedFields()).andReturn(indexedFields).anyTimes();
        EasyMock.expect(helper.getIndexOnlyFields(null)).andReturn(indexOnlyFields).anyTimes();
        
        replayAll();
        
        boolean executable = ExecutableDeterminationVisitor.isExecutable(query, config, helper);
        Assert.assertFalse(executable);
        
        boolean fiExecutable = ExecutableDeterminationVisitor.isExecutable(query, config, helper, true);
        Assert.assertFalse(fiExecutable);
        
        ExecutableDeterminationVisitor.STATE state = ExecutableDeterminationVisitor.getState(query, config, helper);
        Assert.assertEquals(ExecutableDeterminationVisitor.STATE.PARTIAL, state);
        
        ExecutableDeterminationVisitor.STATE fiState = ExecutableDeterminationVisitor.getState(query, config, helper, true);
        Assert.assertEquals(ExecutableDeterminationVisitor.STATE.NEGATED_EXECUTABLE, fiState);
        
        verifyAll();
    }
    
    /**
     * Expected outputs Global Index: EXECUTABLE || NOT_EXECUTABLE == PARTIAL Field Index: EXECUTABLE || !!(EXECUTABLE || NOT_EXECUTABLE) == EXECUTABLE ||
     * NON_EXECUTABLE == PARTIAL
     * 
     * @throws Exception
     */
    @Test
    public void testDoubleNegatedOrExecutable() throws Exception {
        ASTJexlScript query = JexlASTHelper.parseJexlQuery("INDEXEDFIELD == 'a' || !!(INDEXONLYFIELD == 'b' || filter:includeRegex(INDEXEDFIELD,'.*'))");
        
        HashSet indexedFields = new HashSet();
        indexedFields.add("INDEXONLYFIELD");
        indexedFields.add("INDEXEDFIELD");
        
        HashSet<String> indexOnlyFields = new HashSet<>();
        indexOnlyFields.add("INDEXONLYFIELD");
        
        EasyMock.expect(config.getIndexedFields()).andReturn(indexedFields).anyTimes();
        EasyMock.expect(helper.getIndexOnlyFields(null)).andReturn(indexOnlyFields).anyTimes();
        
        replayAll();
        
        boolean executable = ExecutableDeterminationVisitor.isExecutable(query, config, helper);
        Assert.assertFalse(executable);
        
        boolean fiExecutable = ExecutableDeterminationVisitor.isExecutable(query, config, helper, true);
        Assert.assertFalse(fiExecutable);
        
        ExecutableDeterminationVisitor.STATE state = ExecutableDeterminationVisitor.getState(query, config, helper);
        Assert.assertEquals(ExecutableDeterminationVisitor.STATE.PARTIAL, state);
        
        ExecutableDeterminationVisitor.STATE fiState = ExecutableDeterminationVisitor.getState(query, config, helper, true);
        Assert.assertEquals(ExecutableDeterminationVisitor.STATE.PARTIAL, fiState);
        
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
}
