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
}
