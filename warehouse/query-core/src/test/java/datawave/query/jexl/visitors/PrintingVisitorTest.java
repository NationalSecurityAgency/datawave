package datawave.query.jexl.visitors;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ParseException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import datawave.query.jexl.JexlASTHelper;

public class PrintingVisitorTest {
    private final PrintStream originalSystemOut = System.out;

    private static final String QUERY_TEXT = "FOO == 'abc' AND ((_Bounded_ = true) && (BAZ >= '+aE5' AND BAZ <= '+bE1.2'))";

    // @formatter:off
    private static final String FULL_RESULT = "JexlScript\n" +
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
    private ByteArrayOutputStream streamCaptor;

    @Before
    public void before() throws ParseException {
        this.streamCaptor = new ByteArrayOutputStream();
        System.setOut(new PrintStream(streamCaptor));

        this.script = JexlASTHelper.parseJexlQuery(QUERY_TEXT);
    }

    @After
    public void after() {
        System.setOut(this.originalSystemOut);
    }

    @Test
    public void testDefaultPrintsAll() {
        String result = callPrintVisitorAccept(new PrintingVisitor());
        assertEquals(FULL_RESULT, result);
    }

    @Test
    public void defaultPrintsAllLines() {
        String result = callPrintVisitorAccept(new PrintingVisitor());
        assertNumberOfLines(20, result);
    }

    @Test
    public void maxNineteenPrintsAll() {
        String result = callPrintVisitorAccept(new PrintingVisitor(0, 19));
        assertEquals(FULL_RESULT, result);
        assertNumberOfLines(20, result);
    }

    @Test
    public void maxEighteen() {
        String result = callPrintVisitorAccept(new PrintingVisitor(0, 18));
        assertNumberOfLines(19, result);
    }

    @Test
    public void maxFive() {
        String result = callPrintVisitorAccept(new PrintingVisitor(0, 5));
        // @formatter:off
        String expectedOutput = "JexlScript\n" +
                "  AndNode\n" +
                "    EQNode\n" +
                "      FOO:FOO\n" +
                "      abc:abc\n";
        // @formatter:on
        assertEquals(expectedOutput, result);
        assertNumberOfLines(6, result);
    }

    @Test
    public void maxFour() {
        String result = callPrintVisitorAccept(new PrintingVisitor(0, 4));
        // @formatter:off
        String expectedOutput = "JexlScript\n" +
                "  AndNode\n" +
                "    EQNode\n" +
                "      FOO:FOO\n";
        // @formatter:on
        assertEquals(expectedOutput, result);
        assertNumberOfLines(5, result);
    }

    @Test
    public void maxThree() {
        String result = callPrintVisitorAccept(new PrintingVisitor(0, 3));
        // @formatter:off
        String expectedOutput = "JexlScript\n" +
                "  AndNode\n" +
                "    EQNode\n";
        // @formatter:on
        assertEquals(expectedOutput, result);
        assertNumberOfLines(4, result);
    }

    private void assertNumberOfLines(int expectedNumberOfLines, String result) {
        String[] lines = result.split("\n", -1);
        assertEquals(result, expectedNumberOfLines, lines.length);
    }

    private String callPrintVisitorAccept(PrintingVisitor visitor) {
        script.jjtAccept(visitor, "");
        return this.streamCaptor.toString();
    }
}
