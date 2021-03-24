package datawave.query.jexl;

import org.apache.commons.jexl2.JexlContext;
import org.apache.commons.jexl2.JexlEngine;
import org.apache.commons.jexl2.JexlException;
import org.apache.commons.jexl2.Script;
import org.apache.commons.jexl2.parser.ASTStringLiteral;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.easymock.EasyMock.mock;

public class DatawaveInterpreterTest {
    
    @Test
    public void mergeAndNodeFunctionalSetsTest() {
        String query = "((GEO == '0321􏿿+bE4.4' || GEO == '0334􏿿+bE4.4' || GEO == '0320􏿿+bE4.4' || GEO == '0335􏿿+bE4.4') && ((_Delayed_ = true) && ((GEO >= '030a' && GEO <= '0335') && (WKT_BYTE_LENGTH >= '+AE0' && WKT_BYTE_LENGTH < '+bE8'))))";
        
        DatawaveJexlContext context = new DatawaveJexlContext();
        
        Script script = ArithmeticJexlEngines.getEngine(new DefaultArithmetic()).createScript(query);
        
        context.set("GEO", "0321􏿿+bE4.4");
        context.set("WKT_BYTE_LENGTH", "+bE4.4");
        
        Assert.assertTrue(DatawaveInterpreter.isMatched(script.execute(context)));
    }
    
    @Test
    public void largeOrListTest() {
        List<String> uuids = new ArrayList<>();
        for (int i = 0; i < 1000000; i++)
            uuids.add("'" + UUID.randomUUID().toString() + "'");
        
        String query = String.join(" || ", uuids);
        
        DatawaveJexlContext context = new DatawaveJexlContext();
        
        Script script = ArithmeticJexlEngines.getEngine(new DefaultArithmetic()).createScript(query);
        
        Assert.assertTrue(DatawaveInterpreter.isMatched(script.execute(context)));
    }
    
    @Test
    public void invocationFails_alwaysThrowsException() {
        JexlEngine engine = mock(JexlEngine.class);
        JexlContext context = mock(JexlContext.class);
        DatawaveInterpreter interpreter = new DatawaveInterpreter(engine, context, false, false);
        JexlException exception = new JexlException(new ASTStringLiteral(1), "Function failure");
        
        // Make mocks available.
        EasyMock.replay(engine, context);
        
        // Capture the expected exception.
        Exception thrown = null;
        try {
            interpreter.invocationFailed(exception);
        } catch (Exception e) {
            thrown = e;
        }
        
        // Verify that an exception is thrown even when strict == false.
        Assert.assertEquals(thrown, exception);
    }
}
