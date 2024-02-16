package datawave.query.jexl.visitors;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Collections;
import java.util.List;

import org.apache.commons.jexl3.JexlFeatures;
import org.apache.commons.jexl3.parser.ASTAddNode;
import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTAnnotatedStatement;
import org.apache.commons.jexl3.parser.ASTAnnotation;
import org.apache.commons.jexl3.parser.ASTArguments;
import org.apache.commons.jexl3.parser.ASTArrayAccess;
import org.apache.commons.jexl3.parser.ASTArrayLiteral;
import org.apache.commons.jexl3.parser.ASTAssignment;
import org.apache.commons.jexl3.parser.ASTBitwiseAndNode;
import org.apache.commons.jexl3.parser.ASTBitwiseComplNode;
import org.apache.commons.jexl3.parser.ASTBitwiseOrNode;
import org.apache.commons.jexl3.parser.ASTBitwiseXorNode;
import org.apache.commons.jexl3.parser.ASTBlock;
import org.apache.commons.jexl3.parser.ASTBreak;
import org.apache.commons.jexl3.parser.ASTConstructorNode;
import org.apache.commons.jexl3.parser.ASTContinue;
import org.apache.commons.jexl3.parser.ASTDecrementGetNode;
import org.apache.commons.jexl3.parser.ASTDefineVars;
import org.apache.commons.jexl3.parser.ASTDivNode;
import org.apache.commons.jexl3.parser.ASTDoWhileStatement;
import org.apache.commons.jexl3.parser.ASTEQNode;
import org.apache.commons.jexl3.parser.ASTERNode;
import org.apache.commons.jexl3.parser.ASTEWNode;
import org.apache.commons.jexl3.parser.ASTEmptyFunction;
import org.apache.commons.jexl3.parser.ASTExtendedLiteral;
import org.apache.commons.jexl3.parser.ASTFalseNode;
import org.apache.commons.jexl3.parser.ASTForeachStatement;
import org.apache.commons.jexl3.parser.ASTFunctionNode;
import org.apache.commons.jexl3.parser.ASTGENode;
import org.apache.commons.jexl3.parser.ASTGTNode;
import org.apache.commons.jexl3.parser.ASTGetDecrementNode;
import org.apache.commons.jexl3.parser.ASTGetIncrementNode;
import org.apache.commons.jexl3.parser.ASTIdentifier;
import org.apache.commons.jexl3.parser.ASTIdentifierAccess;
import org.apache.commons.jexl3.parser.ASTIfStatement;
import org.apache.commons.jexl3.parser.ASTIncrementGetNode;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ASTJxltLiteral;
import org.apache.commons.jexl3.parser.ASTLENode;
import org.apache.commons.jexl3.parser.ASTLTNode;
import org.apache.commons.jexl3.parser.ASTMapEntry;
import org.apache.commons.jexl3.parser.ASTMapLiteral;
import org.apache.commons.jexl3.parser.ASTMethodNode;
import org.apache.commons.jexl3.parser.ASTModNode;
import org.apache.commons.jexl3.parser.ASTMulNode;
import org.apache.commons.jexl3.parser.ASTNENode;
import org.apache.commons.jexl3.parser.ASTNEWNode;
import org.apache.commons.jexl3.parser.ASTNRNode;
import org.apache.commons.jexl3.parser.ASTNSWNode;
import org.apache.commons.jexl3.parser.ASTNotNode;
import org.apache.commons.jexl3.parser.ASTNullLiteral;
import org.apache.commons.jexl3.parser.ASTNullpNode;
import org.apache.commons.jexl3.parser.ASTNumberLiteral;
import org.apache.commons.jexl3.parser.ASTOrNode;
import org.apache.commons.jexl3.parser.ASTQualifiedIdentifier;
import org.apache.commons.jexl3.parser.ASTRangeNode;
import org.apache.commons.jexl3.parser.ASTReference;
import org.apache.commons.jexl3.parser.ASTReferenceExpression;
import org.apache.commons.jexl3.parser.ASTRegexLiteral;
import org.apache.commons.jexl3.parser.ASTReturnStatement;
import org.apache.commons.jexl3.parser.ASTSWNode;
import org.apache.commons.jexl3.parser.ASTSetAddNode;
import org.apache.commons.jexl3.parser.ASTSetAndNode;
import org.apache.commons.jexl3.parser.ASTSetDivNode;
import org.apache.commons.jexl3.parser.ASTSetLiteral;
import org.apache.commons.jexl3.parser.ASTSetModNode;
import org.apache.commons.jexl3.parser.ASTSetMultNode;
import org.apache.commons.jexl3.parser.ASTSetOrNode;
import org.apache.commons.jexl3.parser.ASTSetShiftLeftNode;
import org.apache.commons.jexl3.parser.ASTSetShiftRightNode;
import org.apache.commons.jexl3.parser.ASTSetShiftRightUnsignedNode;
import org.apache.commons.jexl3.parser.ASTSetSubNode;
import org.apache.commons.jexl3.parser.ASTSetXorNode;
import org.apache.commons.jexl3.parser.ASTShiftLeftNode;
import org.apache.commons.jexl3.parser.ASTShiftRightNode;
import org.apache.commons.jexl3.parser.ASTShiftRightUnsignedNode;
import org.apache.commons.jexl3.parser.ASTSizeFunction;
import org.apache.commons.jexl3.parser.ASTStringLiteral;
import org.apache.commons.jexl3.parser.ASTSubNode;
import org.apache.commons.jexl3.parser.ASTTernaryNode;
import org.apache.commons.jexl3.parser.ASTTrueNode;
import org.apache.commons.jexl3.parser.ASTUnaryMinusNode;
import org.apache.commons.jexl3.parser.ASTUnaryPlusNode;
import org.apache.commons.jexl3.parser.ASTVar;
import org.apache.commons.jexl3.parser.ASTWhileStatement;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.JexlNodes;
import org.apache.commons.jexl3.parser.ParseException;
import org.apache.commons.jexl3.parser.Parser;
import org.apache.commons.jexl3.parser.ParserVisitor;
import org.apache.commons.jexl3.parser.SimpleNode;
import org.apache.commons.jexl3.parser.StringProvider;
import org.apache.commons.jexl3.parser.TokenMgrException;

import com.google.common.collect.Lists;

/**
 * Does a pretty print out of a depth first traversal.
 *
 */
public class PrintingVisitor extends ParserVisitor {

    private interface Output {
        void writeLine(String line);
    }

    private static class PrintStreamOutput implements Output {
        private final PrintStream ps;

        public PrintStreamOutput(PrintStream ps) {
            this.ps = ps;
        }

        /*
         * (non-Javadoc)
         *
         * @see PrintingVisitor.Output#writeLine(java.lang.String)
         */
        @Override
        public void writeLine(String line) {
            ps.println(line);
        }
    }

    private static class StringListOutput implements Output {
        private final List<String> outputLines;

        public StringListOutput() {
            outputLines = Lists.newArrayListWithCapacity(32);
        }

        public List<String> getOutputLines() {
            return Collections.unmodifiableList(outputLines);
        }

        /*
         * (non-Javadoc)
         *
         * @see PrintingVisitor.Output#writeLine(java.lang.String)
         */
        @Override
        public void writeLine(String line) {
            outputLines.add(line);
        }
    }

    private static PrintStreamOutput newPrintStreamOutput(PrintStream ps) {
        return new PrintStreamOutput(ps);
    }

    private static StringListOutput newStringListOutput() {
        return new StringListOutput();

    }

    private static final String PREFIX = "  ";

    private int maxChildNodes;

    private int maxTermsToPrint;

    private Output output;

    public PrintingVisitor() {
        this(0, 150);
    }

    public PrintingVisitor(int maxChildNodes, int maxTermsToPrint) {
        this(maxChildNodes, maxTermsToPrint, new PrintStreamOutput(System.out));
    }

    public PrintingVisitor(int maxChildNodes, int maxTermsToPrint, Output output) {
        this.maxChildNodes = maxChildNodes;
        this.maxTermsToPrint = maxTermsToPrint;
        this.output = output;
    }

    /**
     * Static method to run a depth-first traversal over the AST
     *
     * @param query
     *            JEXL query string
     * @throws ParseException
     *             for issues parsing
     */
    public static void printQuery(String query) throws ParseException {
        // Instantiate a parser and visitor
        Parser parser = new Parser(new StringProvider(";"));

        // Parse the query
        try {
            printQuery(parser.parse(null, new JexlFeatures(), query, null));
        } catch (TokenMgrException e) {
            throw new ParseException(e.getMessage());
        }
    }

    /**
     * Print a representation of this AST
     *
     * @param query
     *            a node
     */
    public static void printQuery(JexlNode query) {
        printQuery(query, 0);
    }

    /**
     * Print a representation of this AST
     *
     * @param query
     *            a query node
     * @param maxChildNodes
     *            maximum number of child nodes
     */
    public static void printQuery(JexlNode query, int maxChildNodes) {
        PrintingVisitor printer = new PrintingVisitor(maxChildNodes, 150);

        // visit() and get the root which is the root of a tree of Boolean Logic Iterator<Key>'s
        query.jjtAccept(printer, "");
    }

    /**
     * Get a {@link java.lang.String} representation for this query string
     *
     * @param query
     *            a query node
     * @return formatted string
     * @throws ParseException
     *             for parsing issues
     */
    public static String formattedQueryString(String query) throws ParseException {
        return formattedQueryString(query, 0, 150);
    }

    /**
     * Get a {@link java.lang.String} representation for this query string
     *
     * @param query
     *            a query node
     * @param maxChildNodes
     *            maximum number of child nodes
     * @return formatted string
     * @throws ParseException
     *             for parsing issues
     */
    public static String formattedQueryString(String query, int maxChildNodes, int maxTermsToPrint) throws ParseException {
        // Instantiate a parser and visitor
        Parser parser = new Parser(new StringProvider(";"));

        // Parse the query
        try {
            return formattedQueryString(parser.parse(null, new JexlFeatures(), query, null), maxChildNodes, maxTermsToPrint);
        } catch (TokenMgrException e) {
            throw new ParseException(e.getMessage());
        }
    }

    /**
     * Get a {@link java.lang.String} representation for this AST
     *
     * @param query
     *            a query node
     * @return formatted string
     */
    public static String formattedQueryString(JexlNode query) {
        return formattedQueryString(query, 0, 150);
    }

    /**
     * Get a {@link java.lang.String} representation for this AST
     *
     * @param query
     *            a query node
     * @param maxChildNodes
     *            maximum number of child nodes
     * @return a formatted string
     */
    public static String formattedQueryString(JexlNode query, int maxChildNodes, int maxTermsToPrint) {
        if (query == null)
            return null;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream output = new PrintStream(baos);
        output.println("");

        PrintingVisitor printer = new PrintingVisitor(maxChildNodes, maxTermsToPrint, newPrintStreamOutput(output));

        query.jjtAccept(printer, "");

        return baos.toString();
    }

    /**
     * Get a {@link java.util.List} of {@link java.lang.String} of each line that should be printed for this AST
     *
     * @param query
     *            a query node
     * @return list of the formatted strings
     */
    public static List<String> formattedQueryStringList(JexlNode query) {
        return formattedQueryStringList(query, 0, 150);
    }

    /**
     * Get a {@link java.util.List} of {@link java.lang.String} of each line that should be printed for this AST
     *
     * @param query
     *            a query node
     * @param maxChildNodes
     *            maximum number of child nodes
     * @return list of the formatted strings
     */
    public static List<String> formattedQueryStringList(JexlNode query, int maxChildNodes, int maxTermsToPrint) {
        StringListOutput output = newStringListOutput();

        PrintingVisitor printer = new PrintingVisitor(maxChildNodes, maxTermsToPrint, output);

        query.jjtAccept(printer, "");

        return output.getOutputLines();
    }

    private void childrenAccept(SimpleNode node, ParserVisitor visitor, Object data) {
        if (maxChildNodes > 0 && node.jjtGetNumChildren() > maxChildNodes) {
            output.writeLine(data + "(Showing " + maxChildNodes + " of " + node.jjtGetNumChildren() + " child nodes)");
            for (int i = 0; i < maxChildNodes; i++)
                node.jjtGetChild(i).jjtAccept(visitor, data);
        } else {
            node.childrenAccept(this, data);
        }
    }

    public Object visit(ASTJexlScript node, Object data) {
        maxTermsToPrint -= 1;
        if (maxTermsToPrint >= 0) {
            output.writeLine(data + node.toString());
            childrenAccept(node, this, data + PREFIX);
        }
        return null;
    }

    public Object visit(ASTBlock node, Object data) {
        maxTermsToPrint -= 1;
        if (maxTermsToPrint >= 0) {
            output.writeLine(data + node.toString());
            childrenAccept(node, this, data + PREFIX);
        }
        return null;
    }

    public Object visit(ASTIfStatement node, Object data) {
        maxTermsToPrint -= 1;
        if (maxTermsToPrint >= 0) {
            output.writeLine(data + node.toString());
            childrenAccept(node, this, data + PREFIX);
        }
        return null;
    }

    public Object visit(ASTWhileStatement node, Object data) {
        maxTermsToPrint -= 1;
        if (maxTermsToPrint >= 0) {
            output.writeLine(data + node.toString());
            childrenAccept(node, this, data + PREFIX);
        }
        return null;
    }

    public Object visit(ASTForeachStatement node, Object data) {
        maxTermsToPrint -= 1;
        if (maxTermsToPrint >= 0) {
            output.writeLine(data + node.toString());
            childrenAccept(node, this, data + PREFIX);
        }
        return null;
    }

    public Object visit(ASTAssignment node, Object data) {
        maxTermsToPrint -= 1;
        if (maxTermsToPrint >= 0) {
            output.writeLine(data + node.toString());
            childrenAccept(node, this, data + PREFIX);
        }
        return null;
    }

    public Object visit(ASTTernaryNode node, Object data) {
        maxTermsToPrint -= 1;
        if (maxTermsToPrint >= 0) {
            output.writeLine(data + node.toString());
            childrenAccept(node, this, data + PREFIX);
        }
        return null;
    }

    public Object visit(ASTOrNode node, Object data) {
        maxTermsToPrint -= 1;
        if (maxTermsToPrint >= 0) {
            output.writeLine(data + node.toString());
            childrenAccept(node, this, data + PREFIX);
        }
        return null;
    }

    public Object visit(ASTAndNode node, Object data) {
        maxTermsToPrint -= 1;
        if (maxTermsToPrint >= 0) {
            output.writeLine(data + node.toString());
            childrenAccept(node, this, data + PREFIX);
        }
        return null;
    }

    public Object visit(ASTBitwiseOrNode node, Object data) {
        maxTermsToPrint -= 1;
        if (maxTermsToPrint >= 0) {
            output.writeLine(data + node.toString());
            childrenAccept(node, this, data + PREFIX);
        }
        return null;
    }

    public Object visit(ASTBitwiseXorNode node, Object data) {
        maxTermsToPrint -= 1;
        if (maxTermsToPrint >= 0) {
            output.writeLine(data + node.toString());
            childrenAccept(node, this, data + PREFIX);
        }
        return null;
    }

    public Object visit(ASTBitwiseAndNode node, Object data) {
        maxTermsToPrint -= 1;
        if (maxTermsToPrint >= 0) {
            output.writeLine(data + node.toString());
            childrenAccept(node, this, data + PREFIX);
        }
        return null;
    }

    public Object visit(ASTEQNode node, Object data) {
        maxTermsToPrint -= 1;
        if (maxTermsToPrint >= 0) {
            output.writeLine(data + node.toString());
            childrenAccept(node, this, data + PREFIX);
        }
        return null;
    }

    public Object visit(ASTNENode node, Object data) {
        maxTermsToPrint -= 1;
        if (maxTermsToPrint >= 0) {
            output.writeLine(data + node.toString());
            childrenAccept(node, this, data + PREFIX);
        }
        return null;
    }

    public Object visit(ASTLTNode node, Object data) {
        maxTermsToPrint -= 1;
        if (maxTermsToPrint >= 0) {
            output.writeLine(data + node.toString());
            childrenAccept(node, this, data + PREFIX);
        }
        return null;
    }

    public Object visit(ASTGTNode node, Object data) {
        maxTermsToPrint -= 1;
        if (maxTermsToPrint >= 0) {
            output.writeLine(data + node.toString());
            childrenAccept(node, this, data + PREFIX);
        }
        return null;
    }

    public Object visit(ASTLENode node, Object data) {
        maxTermsToPrint -= 1;
        if (maxTermsToPrint >= 0) {
            output.writeLine(data + node.toString());
            childrenAccept(node, this, data + PREFIX);
        }
        return null;
    }

    public Object visit(ASTGENode node, Object data) {
        maxTermsToPrint -= 1;
        if (maxTermsToPrint >= 0) {
            output.writeLine(data + node.toString());
            childrenAccept(node, this, data + PREFIX);
        }
        return null;
    }

    public Object visit(ASTERNode node, Object data) {
        maxTermsToPrint -= 1;
        if (maxTermsToPrint >= 0) {
            output.writeLine(data + node.toString());
            childrenAccept(node, this, data + PREFIX);
        }
        return null;
    }

    public Object visit(ASTNRNode node, Object data) {
        maxTermsToPrint -= 1;
        if (maxTermsToPrint >= 0) {
            output.writeLine(data + node.toString());
            childrenAccept(node, this, data + PREFIX);
        }
        return null;
    }

    public Object visit(ASTMulNode node, Object data) {
        maxTermsToPrint -= 1;
        if (maxTermsToPrint >= 0) {
            output.writeLine(data + node.toString());
            childrenAccept(node, this, data + PREFIX);
        }
        return null;
    }

    public Object visit(ASTDivNode node, Object data) {
        maxTermsToPrint -= 1;
        if (maxTermsToPrint >= 0) {
            output.writeLine(data + node.toString());
            childrenAccept(node, this, data + PREFIX);
        }
        return null;
    }

    public Object visit(ASTModNode node, Object data) {
        maxTermsToPrint -= 1;
        if (maxTermsToPrint >= 0) {
            output.writeLine(data + node.toString());
            childrenAccept(node, this, data + PREFIX);
        }
        return null;
    }

    public Object visit(ASTUnaryMinusNode node, Object data) {
        maxTermsToPrint -= 1;
        if (maxTermsToPrint >= 0) {
            output.writeLine(data + node.toString());
            childrenAccept(node, this, data + PREFIX);
        }
        return null;
    }

    public Object visit(ASTBitwiseComplNode node, Object data) {
        maxTermsToPrint -= 1;
        if (maxTermsToPrint >= 0) {
            output.writeLine(data + node.toString());
            childrenAccept(node, this, data + PREFIX);
        }
        return null;
    }

    public Object visit(ASTNotNode node, Object data) {
        maxTermsToPrint -= 1;
        if (maxTermsToPrint >= 0) {
            output.writeLine(data + node.toString());
            childrenAccept(node, this, data + PREFIX);
        }
        return null;
    }

    public Object visit(ASTIdentifier node, Object data) {
        maxTermsToPrint -= 1;
        if (maxTermsToPrint >= 0) {
            output.writeLine(data + node.toString() + ":" + JexlNodes.getIdentifierOrLiteral(node));
            childrenAccept(node, this, data + PREFIX);
            return null;
        } else {
            return null;
        }
    }

    public Object visit(ASTNullLiteral node, Object data) {
        maxTermsToPrint -= 1;
        if (maxTermsToPrint >= 0) {
            output.writeLine(data + node.toString());
            childrenAccept(node, this, data + PREFIX);
        }
        return null;
    }

    public Object visit(ASTTrueNode node, Object data) {
        maxTermsToPrint -= 1;
        if (maxTermsToPrint >= 0) {
            output.writeLine(data + node.toString());
            childrenAccept(node, this, data + PREFIX);
        }
        return null;
    }

    public Object visit(ASTFalseNode node, Object data) {
        maxTermsToPrint -= 1;
        if (maxTermsToPrint >= 0) {
            output.writeLine(data + node.toString());
            childrenAccept(node, this, data + PREFIX);
        }
        return null;
    }

    public Object visit(ASTStringLiteral node, Object data) {
        maxTermsToPrint -= 1;
        output.writeLine(data + node.toString() + ":" + JexlNodes.getIdentifierOrLiteral(node));
        childrenAccept(node, this, data + PREFIX);
        return null;
    }

    public Object visit(ASTArrayLiteral node, Object data) {
        maxTermsToPrint -= 1;
        if (maxTermsToPrint >= 0) {
            output.writeLine(data + node.toString());
            childrenAccept(node, this, data + PREFIX);
        }
        return null;
    }

    public Object visit(ASTMapLiteral node, Object data) {
        maxTermsToPrint -= 1;
        if (maxTermsToPrint >= 0) {
            output.writeLine(data + node.toString());
            childrenAccept(node, this, data + PREFIX);
        }
        return null;
    }

    public Object visit(ASTMapEntry node, Object data) {
        maxTermsToPrint -= 1;
        if (maxTermsToPrint >= 0) {
            output.writeLine(data + node.toString());
            childrenAccept(node, this, data + PREFIX);
        }
        return null;
    }

    public Object visit(ASTEmptyFunction node, Object data) {
        maxTermsToPrint -= 1;
        if (maxTermsToPrint >= 0) {
            output.writeLine(data + node.toString());
            childrenAccept(node, this, data + PREFIX);
        }
        return null;
    }

    public Object visit(ASTSizeFunction node, Object data) {
        maxTermsToPrint -= 1;
        if (maxTermsToPrint >= 0) {
            output.writeLine(data + node.toString());
            childrenAccept(node, this, data + PREFIX);
        }
        return null;
    }

    public Object visit(ASTFunctionNode node, Object data) {
        maxTermsToPrint -= 1;
        if (maxTermsToPrint >= 0) {
            output.writeLine(data + node.toString());
            childrenAccept(node, this, data + PREFIX);
        }
        return null;
    }

    public Object visit(ASTMethodNode node, Object data) {
        maxTermsToPrint -= 1;
        if (maxTermsToPrint >= 0) {
            output.writeLine(data + node.toString());
            childrenAccept(node, this, data + PREFIX);
        }
        return null;
    }

    public Object visit(ASTConstructorNode node, Object data) {
        maxTermsToPrint -= 1;
        if (maxTermsToPrint >= 0) {
            output.writeLine(data + node.toString());
            childrenAccept(node, this, data + PREFIX);
        }
        return null;
    }

    public Object visit(ASTArrayAccess node, Object data) {
        maxTermsToPrint -= 1;
        if (maxTermsToPrint >= 0) {
            output.writeLine(data + node.toString());
            childrenAccept(node, this, data + PREFIX);
        }
        return null;
    }

    public Object visit(ASTReference node, Object data) {
        maxTermsToPrint -= 1;
        if (maxTermsToPrint >= 0) {
            output.writeLine(data + node.toString());
            childrenAccept(node, this, data + PREFIX);
        }
        return null;
    }

    public Object visit(ASTReturnStatement node, Object data) {
        maxTermsToPrint -= 1;
        if (maxTermsToPrint >= 0) {
            output.writeLine(data + node.toString());
            childrenAccept(node, this, data + PREFIX);
        }
        return null;
    }

    public Object visit(ASTVar node, Object data) {
        maxTermsToPrint -= 1;
        if (maxTermsToPrint >= 0) {
            output.writeLine(data + node.toString());
            childrenAccept(node, this, data + PREFIX);
        }
        return null;
    }

    public Object visit(ASTNumberLiteral node, Object data) {
        maxTermsToPrint -= 1;
        if (maxTermsToPrint >= 0) {
            output.writeLine(data + node.toString() + ":" + node.getLiteral());
            childrenAccept(node, this, data + PREFIX);
        }
        return null;
    }

    public Object visit(ASTReferenceExpression node, Object data) {
        maxTermsToPrint -= 1;
        if (maxTermsToPrint >= 0) {
            output.writeLine(data + node.toString());
            childrenAccept(node, this, data + PREFIX);
        }
        return null;
    }

    @Override
    protected Object visit(ASTDoWhileStatement node, Object data) {
        maxTermsToPrint -= 1;
        if (maxTermsToPrint >= 0) {
            output.writeLine(data + node.toString());
            childrenAccept(node, this, data + PREFIX);
        }
        return null;
    }

    @Override
    protected Object visit(ASTContinue node, Object data) {
        maxTermsToPrint -= 1;
        if (maxTermsToPrint >= 0) {
            output.writeLine(data + node.toString());
            childrenAccept(node, this, data + PREFIX);
        }
        return null;
    }

    @Override
    protected Object visit(ASTBreak node, Object data) {
        maxTermsToPrint -= 1;
        if (maxTermsToPrint >= 0) {
            output.writeLine(data + node.toString());
            childrenAccept(node, this, data + PREFIX);
        }
        return null;
    }

    @Override
    protected Object visit(ASTDefineVars node, Object data) {
        maxTermsToPrint -= 1;
        if (maxTermsToPrint >= 0) {
            output.writeLine(data + node.toString());
            childrenAccept(node, this, data + PREFIX);
        }
        return null;
    }

    @Override
    protected Object visit(ASTNullpNode node, Object data) {
        maxTermsToPrint -= 1;
        if (maxTermsToPrint >= 0) {
            output.writeLine(data + node.toString());
            childrenAccept(node, this, data + PREFIX);
        }
        return null;
    }

    @Override
    protected Object visit(ASTShiftLeftNode node, Object data) {
        maxTermsToPrint -= 1;
        if (maxTermsToPrint >= 0) {
            output.writeLine(data + node.toString());
            childrenAccept(node, this, data + PREFIX);
        }
        return null;
    }

    @Override
    protected Object visit(ASTShiftRightNode node, Object data) {
        maxTermsToPrint -= 1;
        if (maxTermsToPrint >= 0) {
            output.writeLine(data + node.toString());
            childrenAccept(node, this, data + PREFIX);
        }
        return null;
    }

    @Override
    protected Object visit(ASTShiftRightUnsignedNode node, Object data) {
        maxTermsToPrint -= 1;
        if (maxTermsToPrint >= 0) {
            output.writeLine(data + node.toString());
            childrenAccept(node, this, data + PREFIX);
        }
        return null;
    }

    @Override
    protected Object visit(ASTSWNode node, Object data) {
        maxTermsToPrint -= 1;
        if (maxTermsToPrint >= 0) {
            output.writeLine(data + node.toString());
            childrenAccept(node, this, data + PREFIX);
        }
        return null;
    }

    @Override
    protected Object visit(ASTNSWNode node, Object data) {
        maxTermsToPrint -= 1;
        if (maxTermsToPrint >= 0) {
            output.writeLine(data + node.toString());
            childrenAccept(node, this, data + PREFIX);
        }
        return null;
    }

    @Override
    protected Object visit(ASTEWNode node, Object data) {
        maxTermsToPrint -= 1;
        if (maxTermsToPrint >= 0) {
            System.out.println("maxTerms: " + maxTermsToPrint);
            output.writeLine(data + node.toString());
            childrenAccept(node, this, data + PREFIX);
        }
        return null;
    }

    @Override
    protected Object visit(ASTNEWNode node, Object data) {
        maxTermsToPrint -= 1;
        if (maxTermsToPrint >= 0) {
            System.out.println("maxTerms: " + maxTermsToPrint);
            output.writeLine(data + node.toString());
            childrenAccept(node, this, data + PREFIX);
        }
        return null;
    }

    @Override
    protected Object visit(ASTAddNode node, Object data) {
        maxTermsToPrint -= 1;
        if (maxTermsToPrint >= 0) {
            System.out.println("maxTerms: " + maxTermsToPrint);
            output.writeLine(data + node.toString());
            childrenAccept(node, this, data + PREFIX);
        }
        return null;
    }

    @Override
    protected Object visit(ASTSubNode node, Object data) {
        maxTermsToPrint -= 1;
        if (maxTermsToPrint >= 0) {
            System.out.println("maxTerms: " + maxTermsToPrint);
            output.writeLine(data + node.toString());
            childrenAccept(node, this, data + PREFIX);
        }
        return null;
    }

    @Override
    protected Object visit(ASTUnaryPlusNode node, Object data) {
        maxTermsToPrint -= 1;
        if (maxTermsToPrint >= 0) {
            System.out.println("maxTerms: " + maxTermsToPrint);
            output.writeLine(data + node.toString());
            childrenAccept(node, this, data + PREFIX);
        }
        return null;
    }

    @Override
    protected Object visit(ASTRegexLiteral node, Object data) {
        maxTermsToPrint -= 1;
        if (maxTermsToPrint >= 0) {
            System.out.println("maxTerms: " + maxTermsToPrint);
            output.writeLine(data + node.toString());
            childrenAccept(node, this, data + PREFIX);
        }
        return null;
    }

    @Override
    protected Object visit(ASTSetLiteral node, Object data) {
        maxTermsToPrint -= 1;
        if (maxTermsToPrint >= 0) {
            System.out.println("maxTerms: " + maxTermsToPrint);
            output.writeLine(data + node.toString());
            childrenAccept(node, this, data + PREFIX);
        }
        return null;
    }

    @Override
    protected Object visit(ASTExtendedLiteral node, Object data) {
        maxTermsToPrint -= 1;
        if (maxTermsToPrint >= 0) {
            System.out.println("maxTerms: " + maxTermsToPrint);
            output.writeLine(data + node.toString());
            childrenAccept(node, this, data + PREFIX);
        }
        return null;
    }

    @Override
    protected Object visit(ASTRangeNode node, Object data) {
        maxTermsToPrint -= 1;
        if (maxTermsToPrint >= 0) {
            System.out.println("maxTerms: " + maxTermsToPrint);
            output.writeLine(data + node.toString());
            childrenAccept(node, this, data + PREFIX);
        }
        return null;
    }

    @Override
    protected Object visit(ASTIdentifierAccess node, Object data) {
        maxTermsToPrint -= 1;
        if (maxTermsToPrint >= 0) {
            System.out.println("maxTerms: " + maxTermsToPrint);
            output.writeLine(data + node.toString());
            childrenAccept(node, this, data + PREFIX);
        }
        return null;
    }

    @Override
    protected Object visit(ASTArguments node, Object data) {
        maxTermsToPrint -= 1;
        if (maxTermsToPrint >= 0) {
            System.out.println("maxTerms: " + maxTermsToPrint);
            output.writeLine(data + node.toString());
            childrenAccept(node, this, data + PREFIX);
        }
        return null;
    }

    @Override
    protected Object visit(ASTSetAddNode node, Object data) {
        maxTermsToPrint -= 1;
        if (maxTermsToPrint >= 0) {
            System.out.println("maxTerms: " + maxTermsToPrint);
            output.writeLine(data + node.toString());
            childrenAccept(node, this, data + PREFIX);
        }
        return null;
    }

    @Override
    protected Object visit(ASTSetSubNode node, Object data) {
        maxTermsToPrint -= 1;
        if (maxTermsToPrint >= 0) {
            System.out.println("maxTerms: " + maxTermsToPrint);
            output.writeLine(data + node.toString());
            childrenAccept(node, this, data + PREFIX);
        }
        return null;
    }

    @Override
    protected Object visit(ASTSetMultNode node, Object data) {
        maxTermsToPrint -= 1;
        if (maxTermsToPrint >= 0) {
            System.out.println("maxTerms: " + maxTermsToPrint);
            output.writeLine(data + node.toString());
            childrenAccept(node, this, data + PREFIX);
        }
        return null;
    }

    @Override
    protected Object visit(ASTSetDivNode node, Object data) {
        maxTermsToPrint -= 1;
        if (maxTermsToPrint >= 0) {
            System.out.println("maxTerms: " + maxTermsToPrint);
            output.writeLine(data + node.toString());
            childrenAccept(node, this, data + PREFIX);
        }
        return null;
    }

    @Override
    protected Object visit(ASTSetModNode node, Object data) {
        maxTermsToPrint -= 1;
        if (maxTermsToPrint >= 0) {
            System.out.println("maxTerms: " + maxTermsToPrint);
            output.writeLine(data + node.toString());
            childrenAccept(node, this, data + PREFIX);
        }
        return null;
    }

    @Override
    protected Object visit(ASTSetAndNode node, Object data) {
        maxTermsToPrint -= 1;
        if (maxTermsToPrint >= 0) {
            System.out.println("maxTerms: " + maxTermsToPrint);
            output.writeLine(data + node.toString());
            childrenAccept(node, this, data + PREFIX);
        }
        return null;
    }

    @Override
    protected Object visit(ASTSetOrNode node, Object data) {
        maxTermsToPrint -= 1;
        if (maxTermsToPrint >= 0) {
            System.out.println("maxTerms: " + maxTermsToPrint);
            output.writeLine(data + node.toString());
            childrenAccept(node, this, data + PREFIX);
        }
        return null;
    }

    @Override
    protected Object visit(ASTSetXorNode node, Object data) {
        maxTermsToPrint -= 1;
        if (maxTermsToPrint >= 0) {
            System.out.println("maxTerms: " + maxTermsToPrint);
            output.writeLine(data + node.toString());
            childrenAccept(node, this, data + PREFIX);
        }
        return null;
    }

    @Override
    protected Object visit(ASTSetShiftLeftNode node, Object data) {
        maxTermsToPrint -= 1;
        if (maxTermsToPrint >= 0) {
            System.out.println("maxTerms: " + maxTermsToPrint);
            output.writeLine(data + node.toString());
            childrenAccept(node, this, data + PREFIX);
        }
        return null;
    }

    @Override
    protected Object visit(ASTSetShiftRightNode node, Object data) {
        maxTermsToPrint -= 1;
        if (maxTermsToPrint >= 0) {
            System.out.println("maxTerms: " + maxTermsToPrint);
            output.writeLine(data + node.toString());
            childrenAccept(node, this, data + PREFIX);
        }
        return null;
    }

    @Override
    protected Object visit(ASTSetShiftRightUnsignedNode node, Object data) {
        maxTermsToPrint -= 1;
        if (maxTermsToPrint >= 0) {
            System.out.println("maxTerms: " + maxTermsToPrint);
            output.writeLine(data + node.toString());
            childrenAccept(node, this, data + PREFIX);
        }
        return null;
    }

    @Override
    protected Object visit(ASTGetDecrementNode node, Object data) {
        maxTermsToPrint -= 1;
        if (maxTermsToPrint >= 0) {
            System.out.println("maxTerms: " + maxTermsToPrint);
            output.writeLine(data + node.toString());
            childrenAccept(node, this, data + PREFIX);
        }
        return null;
    }

    @Override
    protected Object visit(ASTGetIncrementNode node, Object data) {
        maxTermsToPrint -= 1;
        if (maxTermsToPrint >= 0) {
            System.out.println("maxTerms: " + maxTermsToPrint);
            output.writeLine(data + node.toString());
            childrenAccept(node, this, data + PREFIX);
        }
        return null;
    }

    @Override
    protected Object visit(ASTDecrementGetNode node, Object data) {
        maxTermsToPrint -= 1;
        if (maxTermsToPrint >= 0) {
            System.out.println("maxTerms: " + maxTermsToPrint);
            output.writeLine(data + node.toString());
            childrenAccept(node, this, data + PREFIX);
        }
        return null;
    }

    @Override
    protected Object visit(ASTIncrementGetNode node, Object data) {
        maxTermsToPrint -= 1;
        if (maxTermsToPrint >= 0) {
            System.out.println("maxTerms: " + maxTermsToPrint);
            output.writeLine(data + node.toString());
            childrenAccept(node, this, data + PREFIX);
        }
        return null;
    }

    @Override
    protected Object visit(ASTJxltLiteral node, Object data) {
        maxTermsToPrint -= 1;
        if (maxTermsToPrint >= 0) {
            System.out.println("maxTerms: " + maxTermsToPrint);
            output.writeLine(data + node.toString());
            childrenAccept(node, this, data + PREFIX);
        }
        return null;
    }

    @Override
    protected Object visit(ASTAnnotation node, Object data) {
        maxTermsToPrint -= 1;
        if (maxTermsToPrint >= 0) {
            System.out.println("maxTerms: " + maxTermsToPrint);
            output.writeLine(data + node.toString());
            childrenAccept(node, this, data + PREFIX);
        }
        return null;
    }

    @Override
    protected Object visit(ASTAnnotatedStatement node, Object data) {
        maxTermsToPrint -= 1;
        if (maxTermsToPrint >= 0) {
            System.out.println("maxTerms: " + maxTermsToPrint);
            output.writeLine(data + node.toString());
            childrenAccept(node, this, data + PREFIX);
        }
        return null;
    }

    @Override
    protected Object visit(ASTQualifiedIdentifier node, Object data) {
        maxTermsToPrint -= 1;
        if (maxTermsToPrint >= 0) {
            System.out.println("maxTerms: " + maxTermsToPrint);
            output.writeLine(data + node.toString());
            childrenAccept(node, this, data + PREFIX);
        }
        return null;
    }
}
