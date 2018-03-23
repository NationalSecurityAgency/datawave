package datawave.query.jexl.visitors;

import datawave.query.jexl.JexlASTHelper;
import org.apache.commons.jexl2.parser.JexlNode;
import org.junit.Assert;
import org.junit.Test;

public class TreeFlatteningRebuildingVisitorTest {

    @Test
    public void dontFlattenASTDelayedPredicateTest() throws Exception {
        String query = "((ASTDelayedPredicate = true) && (GEO == '1f36c71c71c71c71c7' && (WKT_BYTE_LENGTH >= '+AE0' && WKT_BYTE_LENGTH < '+bE8'))) && GEO >= '1f36c71c71c71c71c7\uDBFF\uDFFF+AE0' && GEO < '1f36c71c71c71c71c8\uDBFF\uDFFF+bE8'";
        JexlNode node = TreeFlatteningRebuildingVisitor.flatten(JexlASTHelper.parseJexlQuery(query));
        Assert.assertEquals(query, JexlStringBuildingVisitor.buildQuery(node));
    }
}
