package datawave.query.jexl.visitors;

import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ASTNumberLiteral;
import org.apache.commons.jexl3.parser.ASTUnaryMinusNode;
import org.apache.commons.jexl3.parser.ParseException;
import org.junit.Test;

import datawave.query.jexl.JexlASTHelper;
import datawave.test.JexlNodeAssert;

public class FixNegativeNumbersVisitorTest {

    @Test
    public void testUnaryMinusModeConvertedToNumberLiteral() throws ParseException {
        String query = "FOO == -1";
        ASTJexlScript queryScript = JexlASTHelper.parseJexlQuery(query);

        // Verify the script was parsed with a single unary minus node.
        JexlNodeAssert.assertThat(queryScript).child(0).child(1).isInstanceOf(ASTUnaryMinusNode.class);

        ASTJexlScript fixed = FixNegativeNumbersVisitor.fix(queryScript);

        // Verify the unary minus mode was converted to a number literal.
        JexlNodeAssert.assertThat(fixed).child(0).child(1).isInstanceOf(ASTNumberLiteral.class).hasImage("-1");

        // Verify the resulting script has a valid lineage and the same query string.
        JexlNodeAssert.assertThat(fixed).hasExactQueryString(query).hasValidLineage();

        // Verify the original script was not modified, and has a valid lineage.
        JexlNodeAssert.assertThat(queryScript).isEqualTo(query).hasValidLineage();
    }
}
