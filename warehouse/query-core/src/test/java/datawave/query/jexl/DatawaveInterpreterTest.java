package datawave.query.jexl;

import org.apache.commons.jexl2.Script;
import org.junit.Test;

public class DatawaveInterpreterTest {

    @Test
    public void mergeAndNodeFunctionalSetsTest() {
        String query = "((ASTDelayedPredicate = true) && (GEO == '1f36c71c71c71c71c7' && (WKT_BYTE_LENGTH >= '+AE0' && WKT_BYTE_LENGTH < '+bE8'))) && GEO == '1f36c71c71c71c71c7??+bE1.3'";

        DatawaveJexlContext context = new DatawaveJexlContext();

        Script script = ArithmeticJexlEngines.getEngine(new DefaultArithmetic()).createScript(query);


        context.set("GEO", );

        script.execute(context);
    }
}
