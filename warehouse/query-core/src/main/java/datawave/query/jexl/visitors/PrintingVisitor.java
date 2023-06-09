package datawave.query.jexl.visitors;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.StringReader;
import java.util.Collections;
import java.util.List;

import org.apache.commons.jexl2.parser.ASTAdditiveNode;
import org.apache.commons.jexl2.parser.ASTAdditiveOperator;
import org.apache.commons.jexl2.parser.ASTAmbiguous;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTArrayAccess;
import org.apache.commons.jexl2.parser.ASTArrayLiteral;
import org.apache.commons.jexl2.parser.ASTAssignment;
import org.apache.commons.jexl2.parser.ASTBitwiseAndNode;
import org.apache.commons.jexl2.parser.ASTBitwiseComplNode;
import org.apache.commons.jexl2.parser.ASTBitwiseOrNode;
import org.apache.commons.jexl2.parser.ASTBitwiseXorNode;
import org.apache.commons.jexl2.parser.ASTBlock;
import org.apache.commons.jexl2.parser.ASTConstructorNode;
import org.apache.commons.jexl2.parser.ASTDivNode;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTEmptyFunction;
import org.apache.commons.jexl2.parser.ASTFalseNode;
import org.apache.commons.jexl2.parser.ASTForeachStatement;
import org.apache.commons.jexl2.parser.ASTFunctionNode;
import org.apache.commons.jexl2.parser.ASTGENode;
import org.apache.commons.jexl2.parser.ASTGTNode;
import org.apache.commons.jexl2.parser.ASTIdentifier;
import org.apache.commons.jexl2.parser.ASTIfStatement;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ASTLENode;
import org.apache.commons.jexl2.parser.ASTLTNode;
import org.apache.commons.jexl2.parser.ASTMapEntry;
import org.apache.commons.jexl2.parser.ASTMapLiteral;
import org.apache.commons.jexl2.parser.ASTMethodNode;
import org.apache.commons.jexl2.parser.ASTModNode;
import org.apache.commons.jexl2.parser.ASTMulNode;
import org.apache.commons.jexl2.parser.ASTNENode;
import org.apache.commons.jexl2.parser.ASTNRNode;
import org.apache.commons.jexl2.parser.ASTNotNode;
import org.apache.commons.jexl2.parser.ASTNullLiteral;
import org.apache.commons.jexl2.parser.ASTNumberLiteral;
import org.apache.commons.jexl2.parser.ASTOrNode;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.ASTReferenceExpression;
import org.apache.commons.jexl2.parser.ASTReturnStatement;
import org.apache.commons.jexl2.parser.ASTSizeFunction;
import org.apache.commons.jexl2.parser.ASTSizeMethod;
import org.apache.commons.jexl2.parser.ASTStringLiteral;
import org.apache.commons.jexl2.parser.ASTTernaryNode;
import org.apache.commons.jexl2.parser.ASTTrueNode;
import org.apache.commons.jexl2.parser.ASTUnaryMinusNode;
import org.apache.commons.jexl2.parser.ASTVar;
import org.apache.commons.jexl2.parser.ASTWhileStatement;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParseException;
import org.apache.commons.jexl2.parser.Parser;
import org.apache.commons.jexl2.parser.ParserVisitor;
import org.apache.commons.jexl2.parser.SimpleNode;

import com.google.common.collect.Lists;
import org.apache.commons.jexl2.parser.TokenMgrError;

/**
 * Does a pretty print out of a depth first traversal.
 *
 */
public class PrintingVisitor implements org.apache.commons.jexl2.parser.ParserVisitor {

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
    private Output output;

    public PrintingVisitor() {
        this(0);
    }

    public PrintingVisitor(int maxChildNodes) {
        this(maxChildNodes, new PrintStreamOutput(System.out));
    }

    public PrintingVisitor(int maxChildNodes, Output output) {
        this.maxChildNodes = maxChildNodes;
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
        Parser parser = new Parser(new StringReader(";"));

        // Parse the query
        try {
            printQuery(parser.parse(new StringReader(query), null));
        } catch (TokenMgrError e) {
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
        PrintingVisitor printer = new PrintingVisitor(maxChildNodes);

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
        return formattedQueryString(query, 0);
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
    public static String formattedQueryString(String query, int maxChildNodes) throws ParseException {
        // Instantiate a parser and visitor
        Parser parser = new Parser(new StringReader(";"));

        // Parse the query
        try {
            return formattedQueryString(parser.parse(new StringReader(query), null), maxChildNodes);
        } catch (TokenMgrError e) {
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
        return formattedQueryString(query, 0);
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
    public static String formattedQueryString(JexlNode query, int maxChildNodes) {
        if (query == null)
            return null;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream output = new PrintStream(baos);
        output.println("");

        PrintingVisitor printer = new PrintingVisitor(maxChildNodes, newPrintStreamOutput(output));

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
        return formattedQueryStringList(query, 0);
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
    public static List<String> formattedQueryStringList(JexlNode query, int maxChildNodes) {
        StringListOutput output = newStringListOutput();

        PrintingVisitor printer = new PrintingVisitor(maxChildNodes, output);

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

    public Object visit(SimpleNode node, Object data) {
        output.writeLine(data + node.toString());
        childrenAccept(node, this, data + PREFIX);
        return null;
    }

    public Object visit(ASTJexlScript node, Object data) {
        output.writeLine(data + node.toString());
        childrenAccept(node, this, data + PREFIX);
        return null;
    }

    public Object visit(ASTBlock node, Object data) {
        output.writeLine(data + node.toString());
        childrenAccept(node, this, data + PREFIX);
        return null;
    }

    public Object visit(ASTAmbiguous node, Object data) {
        output.writeLine(data + node.toString());
        childrenAccept(node, this, data + PREFIX);
        return null;
    }

    public Object visit(ASTIfStatement node, Object data) {
        output.writeLine(data + node.toString());
        childrenAccept(node, this, data + PREFIX);
        return null;
    }

    public Object visit(ASTWhileStatement node, Object data) {
        output.writeLine(data + node.toString());
        childrenAccept(node, this, data + PREFIX);
        return null;
    }

    public Object visit(ASTForeachStatement node, Object data) {
        output.writeLine(data + node.toString());
        childrenAccept(node, this, data + PREFIX);
        return null;
    }

    public Object visit(ASTAssignment node, Object data) {
        output.writeLine(data + node.toString());
        childrenAccept(node, this, data + PREFIX);
        return null;
    }

    public Object visit(ASTTernaryNode node, Object data) {
        output.writeLine(data + node.toString());
        childrenAccept(node, this, data + PREFIX);
        return null;
    }

    public Object visit(ASTOrNode node, Object data) {
        output.writeLine(data + node.toString());
        childrenAccept(node, this, data + PREFIX);
        return null;
    }

    public Object visit(ASTAndNode node, Object data) {
        output.writeLine(data + node.toString());
        childrenAccept(node, this, data + PREFIX);
        return null;
    }

    public Object visit(ASTBitwiseOrNode node, Object data) {
        output.writeLine(data + node.toString());
        childrenAccept(node, this, data + PREFIX);
        return null;
    }

    public Object visit(ASTBitwiseXorNode node, Object data) {
        output.writeLine(data + node.toString());
        childrenAccept(node, this, data + PREFIX);
        return null;
    }

    public Object visit(ASTBitwiseAndNode node, Object data) {
        output.writeLine(data + node.toString());
        childrenAccept(node, this, data + PREFIX);
        return null;
    }

    public Object visit(ASTEQNode node, Object data) {
        output.writeLine(data + node.toString());
        childrenAccept(node, this, data + PREFIX);
        return null;
    }

    public Object visit(ASTNENode node, Object data) {
        output.writeLine(data + node.toString());
        childrenAccept(node, this, data + PREFIX);
        return null;
    }

    public Object visit(ASTLTNode node, Object data) {
        output.writeLine(data + node.toString());
        childrenAccept(node, this, data + PREFIX);
        return null;
    }

    public Object visit(ASTGTNode node, Object data) {
        output.writeLine(data + node.toString());
        childrenAccept(node, this, data + PREFIX);
        return null;
    }

    public Object visit(ASTLENode node, Object data) {
        output.writeLine(data + node.toString());
        childrenAccept(node, this, data + PREFIX);
        return null;
    }

    public Object visit(ASTGENode node, Object data) {
        output.writeLine(data + node.toString());
        childrenAccept(node, this, data + PREFIX);
        return null;
    }

    public Object visit(ASTERNode node, Object data) {
        output.writeLine(data + node.toString());
        childrenAccept(node, this, data + PREFIX);
        return null;
    }

    public Object visit(ASTNRNode node, Object data) {
        output.writeLine(data + node.toString());
        childrenAccept(node, this, data + PREFIX);
        return null;
    }

    public Object visit(ASTAdditiveNode node, Object data) {
        output.writeLine(data + node.toString());
        childrenAccept(node, this, data + PREFIX);
        return null;
    }

    public Object visit(ASTAdditiveOperator node, Object data) {
        output.writeLine(data + node.toString());
        childrenAccept(node, this, data + PREFIX);
        return null;
    }

    public Object visit(ASTMulNode node, Object data) {
        output.writeLine(data + node.toString());
        childrenAccept(node, this, data + PREFIX);
        return null;
    }

    public Object visit(ASTDivNode node, Object data) {
        output.writeLine(data + node.toString());
        childrenAccept(node, this, data + PREFIX);
        return null;
    }

    public Object visit(ASTModNode node, Object data) {
        output.writeLine(data + node.toString());
        childrenAccept(node, this, data + PREFIX);
        return null;
    }

    public Object visit(ASTUnaryMinusNode node, Object data) {
        output.writeLine(data + node.toString());
        childrenAccept(node, this, data + PREFIX);
        return null;
    }

    public Object visit(ASTBitwiseComplNode node, Object data) {
        output.writeLine(data + node.toString());
        childrenAccept(node, this, data + PREFIX);
        return null;
    }

    public Object visit(ASTNotNode node, Object data) {
        output.writeLine(data + node.toString());
        childrenAccept(node, this, data + PREFIX);
        return null;
    }

    public Object visit(ASTIdentifier node, Object data) {
        output.writeLine(data + node.toString() + ":" + node.image);
        childrenAccept(node, this, data + PREFIX);
        return null;
    }

    public Object visit(ASTNullLiteral node, Object data) {
        output.writeLine(data + node.toString());
        childrenAccept(node, this, data + PREFIX);
        return null;
    }

    public Object visit(ASTTrueNode node, Object data) {
        output.writeLine(data + node.toString());
        childrenAccept(node, this, data + PREFIX);
        return null;
    }

    public Object visit(ASTFalseNode node, Object data) {
        output.writeLine(data + node.toString());
        childrenAccept(node, this, data + PREFIX);
        return null;
    }

    public Object visit(ASTStringLiteral node, Object data) {
        output.writeLine(data + node.toString() + ":" + node.image);
        childrenAccept(node, this, data + PREFIX);
        return null;
    }

    public Object visit(ASTArrayLiteral node, Object data) {
        output.writeLine(data + node.toString());
        childrenAccept(node, this, data + PREFIX);
        return null;
    }

    public Object visit(ASTMapLiteral node, Object data) {
        output.writeLine(data + node.toString());
        childrenAccept(node, this, data + PREFIX);
        return null;
    }

    public Object visit(ASTMapEntry node, Object data) {
        output.writeLine(data + node.toString());
        childrenAccept(node, this, data + PREFIX);
        return null;
    }

    public Object visit(ASTEmptyFunction node, Object data) {
        output.writeLine(data + node.toString());
        childrenAccept(node, this, data + PREFIX);
        return null;
    }

    public Object visit(ASTSizeFunction node, Object data) {
        output.writeLine(data + node.toString());
        childrenAccept(node, this, data + PREFIX);
        return null;
    }

    public Object visit(ASTFunctionNode node, Object data) {
        output.writeLine(data + node.toString());
        childrenAccept(node, this, data + PREFIX);
        return null;
    }

    public Object visit(ASTMethodNode node, Object data) {
        output.writeLine(data + node.toString());
        childrenAccept(node, this, data + PREFIX);
        return null;
    }

    public Object visit(ASTSizeMethod node, Object data) {
        output.writeLine(data + node.toString());
        childrenAccept(node, this, data + PREFIX);
        return null;
    }

    public Object visit(ASTConstructorNode node, Object data) {
        output.writeLine(data + node.toString());
        childrenAccept(node, this, data + PREFIX);
        return null;
    }

    public Object visit(ASTArrayAccess node, Object data) {
        output.writeLine(data + node.toString());
        childrenAccept(node, this, data + PREFIX);
        return null;
    }

    public Object visit(ASTReference node, Object data) {
        output.writeLine(data + node.toString());
        childrenAccept(node, this, data + PREFIX);
        return null;
    }

    public Object visit(ASTReturnStatement node, Object data) {
        output.writeLine(data + node.toString());
        childrenAccept(node, this, data + PREFIX);
        return null;
    }

    public Object visit(ASTVar node, Object data) {
        output.writeLine(data + node.toString());
        childrenAccept(node, this, data + PREFIX);
        return null;
    }

    public Object visit(ASTNumberLiteral node, Object data) {
        output.writeLine(data + node.toString() + ":" + node.getLiteral());
        childrenAccept(node, this, data + PREFIX);
        return null;
    }

    public Object visit(ASTReferenceExpression node, Object data) {
        output.writeLine(data + node.toString());
        childrenAccept(node, this, data + PREFIX);
        return null;
    }

}
