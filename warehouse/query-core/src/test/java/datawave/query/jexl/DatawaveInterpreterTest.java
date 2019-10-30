package datawave.query.jexl;

import org.apache.commons.jexl2.Script;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class DatawaveInterpreterTest {
    
    @Test
    public void mergeAndNodeFunctionalSetsTest() {
        String query = "((GEO == '0321􏿿+bE4.4' || GEO == '0334􏿿+bE4.4' || GEO == '0320􏿿+bE4.4' || GEO == '0335􏿿+bE4.4') && ((ASTDelayedPredicate = true) && ((GEO >= '030a' && GEO <= '0335') && (WKT_BYTE_LENGTH >= '+AE0' && WKT_BYTE_LENGTH < '+bE8'))))";
        
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
}
