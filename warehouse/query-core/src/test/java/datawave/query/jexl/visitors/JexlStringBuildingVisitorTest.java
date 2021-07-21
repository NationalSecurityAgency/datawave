package datawave.query.jexl.visitors;

import datawave.query.jexl.JexlASTHelper;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParseException;
import org.junit.Assert;
import org.junit.Test;

public class JexlStringBuildingVisitorTest {
    @Test
    public void testNegativeNumber() throws ParseException {
        String query = "BLAH == -2";
        JexlNode node = JexlASTHelper.parseJexlQuery(query);
        Assert.assertEquals("BLAH == -2", JexlStringBuildingVisitor.buildQuery(node));
    }
}
