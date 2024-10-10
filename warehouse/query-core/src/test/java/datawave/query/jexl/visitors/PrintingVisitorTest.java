package datawave.query.jexl.visitors;

import static org.junit.Assert.assertEquals;

import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ParseException;
import org.junit.Before;
import org.junit.Test;

import datawave.core.query.jexl.visitors.PrintingVisitor;
import datawave.query.jexl.JexlASTHelper;

public class PrintingVisitorTest {
    private static final String QUERY_TEXT = "FOO == 'abc' AND ((_Bounded_ = true) && (BAZ >= '+aE5' AND BAZ <= '+bE1.2'))";

    // @formatter:off
    private static final String FULL_RESULT = "\n" +
            "JexlScript\n" +
            "  AndNode\n" +
            "    EQNode\n" +
            "      FOO:FOO\n" +
            "      abc:abc\n" +
            "    ReferenceExpression\n" +
            "      AndNode\n" +
            "        ReferenceExpression\n" +
            "          Assignment\n" +
            "            _Bounded_:_Bounded_\n" +
            "            TrueNode\n" +
            "        ReferenceExpression\n" +
            "          AndNode\n" +
            "            GENode\n" +
            "              BAZ:BAZ\n" +
            "              +aE5:+aE5\n" +
            "            LENode\n" +
            "              BAZ:BAZ\n" +
            "              +bE1.2:+bE1.2\n";
    // @formatter:on

    private ASTJexlScript script;

    @Before
    public void before() throws ParseException {
        this.script = JexlASTHelper.parseJexlQuery(QUERY_TEXT);
    }

    @Test
    public void testDefaultPrintsAll() {
        String result = PrintingVisitor.formattedQueryString(script);
        assertEquals(FULL_RESULT, result);
    }

    @Test
    public void defaultPrintsAllLines() {
        String result = PrintingVisitor.formattedQueryString(script);
        assertNumberOfLines(21, result);
    }

    @Test
    public void maxNineteenPrintsAll() {
        String result = PrintingVisitor.formattedQueryString(script, 0, 19);
        assertEquals(FULL_RESULT, result);
        assertNumberOfLines(21, result);
    }

    @Test
    public void maxEighteen() {
        String result = PrintingVisitor.formattedQueryString(script, 0, 18);
        assertNumberOfLines(20, result);
    }

    @Test
    public void maxFive() {
        String result = PrintingVisitor.formattedQueryString(script, 0, 5);
        // @formatter:off
        String expectedOutput = "\n" +
                "JexlScript\n" +
                "  AndNode\n" +
                "    EQNode\n" +
                "      FOO:FOO\n" +
                "      abc:abc\n";
        // @formatter:on
        assertEquals(expectedOutput, result);
        assertNumberOfLines(7, result);
    }

    @Test
    public void maxFour() {
        String result = PrintingVisitor.formattedQueryString(script, 0, 4);
        // @formatter:off
        String expectedOutput = "\n" +
                "JexlScript\n" +
                "  AndNode\n" +
                "    EQNode\n" +
                "      FOO:FOO\n";
        // @formatter:on
        assertEquals(expectedOutput, result);
        assertNumberOfLines(6, result);
    }

    @Test
    public void maxThree() {
        String result = PrintingVisitor.formattedQueryString(script, 0, 3);
        // @formatter:off
        String expectedOutput = "\n" +
                "JexlScript\n" +
                "  AndNode\n" +
                "    EQNode\n";
        // @formatter:on
        assertEquals(expectedOutput, result);
        assertNumberOfLines(5, result);
    }

    private void assertNumberOfLines(int expectedNumberOfLines, String result) {
        String[] lines = result.split("\n", -1);
        assertEquals(result, expectedNumberOfLines, lines.length);
    }
}
