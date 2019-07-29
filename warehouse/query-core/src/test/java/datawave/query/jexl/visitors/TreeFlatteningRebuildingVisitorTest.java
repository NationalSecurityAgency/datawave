package datawave.query.jexl.visitors;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.language.parser.jexl.LuceneToJexlQueryParser;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.Parser;
import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.StringReader;

public class TreeFlatteningRebuildingVisitorTest {
    
    @Test
    public void dontFlattenASTDelayedPredicateAndTest() throws Exception {
        String query = "((ASTDelayedPredicate = true) && (GEO == '1f36c71c71c71c71c7' && (WKT_BYTE_LENGTH >= '+AE0' && WKT_BYTE_LENGTH < '+bE8'))) && GEO >= '1f36c71c71c71c71c7\uDBFF\uDFFF+AE0' && GEO < '1f36c71c71c71c71c8\uDBFF\uDFFF+bE8'";
        String expected = "GEO < '1f36c71c71c71c71c8\uDBFF\uDFFF+bE8' && ((ASTDelayedPredicate = true) && (GEO == '1f36c71c71c71c71c7' && (WKT_BYTE_LENGTH >= '+AE0' && WKT_BYTE_LENGTH < '+bE8'))) && GEO >= '1f36c71c71c71c71c7\uDBFF\uDFFF+AE0'";
        JexlNode node = TreeFlatteningRebuildingVisitor.flatten(JexlASTHelper.parseJexlQuery(query));
        Assert.assertEquals(expected, JexlStringBuildingVisitor.buildQuery(node));
    }
    
    @Test
    public void dontFlattenASTDelayedPredicateOrTest() throws Exception {
        String query = "((ASTDelayedPredicate = true) || (GEO == '1f36c71c71c71c71c7' && (WKT_BYTE_LENGTH >= '+AE0' && WKT_BYTE_LENGTH < '+bE8'))) || GEO >= '1f36c71c71c71c71c7\uDBFF\uDFFF+AE0' || GEO < '1f36c71c71c71c71c8\uDBFF\uDFFF+bE8'";
        String expected = "GEO < '1f36c71c71c71c71c8\uDBFF\uDFFF+bE8' || GEO >= '1f36c71c71c71c71c7\uDBFF\uDFFF+AE0' || (ASTDelayedPredicate = true) || (GEO == '1f36c71c71c71c71c7' && (WKT_BYTE_LENGTH >= '+AE0' && WKT_BYTE_LENGTH < '+bE8'))";
        JexlNode node = TreeFlatteningRebuildingVisitor.flatten(JexlASTHelper.parseJexlQuery(query));
        Assert.assertEquals(expected, JexlStringBuildingVisitor.buildQuery(node));
    }
    
    @Test
    public void depthNoStackTraceTest() throws Exception {
        final int numTerms = 10_000;
        final StringBuilder sb = new StringBuilder(13 * numTerms); // 13 == "abc_" + 5 + " OR "
        sb.append("abc_" + StringUtils.leftPad(Integer.toString(numTerms, 10), 5, '0'));
        for (int i = 2; i <= numTerms; i++) {
            sb.append(" OR " + i + "");
        }
        Assert.assertNotNull(TreeFlatteningRebuildingVisitor.flattenAll(new Parser(new StringReader(";")).parse(new StringReader(new LuceneToJexlQueryParser()
                        .parse(sb.toString()).toString()), null)));
    }
    
    @Test
    public void multipleNestingTest() throws Exception {
        String query = "(a || b || (c || d || e || (f || g || (h || i || (((j || k)))))))";
        String expected = "(j || k || h || i || f || g || e || c || d || a || b)";
        JexlNode node = TreeFlatteningRebuildingVisitor.flatten(JexlASTHelper.parseJexlQuery(query));
        Assert.assertEquals(expected, JexlStringBuildingVisitor.buildQuery(node));
    }
    
}
