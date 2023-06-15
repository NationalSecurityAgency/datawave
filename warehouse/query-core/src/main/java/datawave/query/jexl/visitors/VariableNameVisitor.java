package datawave.query.jexl.visitors;

import com.google.common.collect.Sets;
import datawave.query.jexl.nodes.ExceededOrThresholdMarkerJexlNode;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import org.apache.commons.jexl2.parser.ASTIdentifier;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParseException;
import org.apache.commons.jexl2.parser.Parser;
import org.apache.commons.jexl2.parser.TokenMgrError;

import java.io.StringReader;
import java.util.Set;

/**
 * Extracts all of the identifier names from a query. This exists only because the getVariables() method in JexlEngine is broken in the released versions of
 * commons-jexl
 *
 */
public class VariableNameVisitor extends BaseVisitor {

    private Set<String> variableNames = Sets.newHashSet();

    /**
     * Static method to run a depth-first traversal over the AST
     *
     * @param query
     *            JEXL query string
     * @return the parsed query set
     * @throws ParseException
     *             for issues with parsing
     */
    public static Set<String> parseQuery(String query) throws ParseException {
        // Instantiate a parser and visitor
        Parser parser = new Parser(new StringReader(";"));

        // Parse the query
        try {
            return parseQuery(parser.parse(new StringReader(query), null));
        } catch (TokenMgrError e) {
            throw new ParseException(e.getMessage());
        }
    }

    /**
     * Print a representation of this AST
     *
     * @param query
     *            a jexl node
     * @return the parsed query set
     */
    public static Set<String> parseQuery(JexlNode query) {
        VariableNameVisitor printer = new VariableNameVisitor();

        // visit() and get the root which is the root of a tree of Boolean Logic Iterator<Key>'s
        query.jjtAccept(printer, "");
        return printer.variableNames;
    }

    @Override
    public Object visit(ASTIdentifier node, Object data) {
        this.variableNames.add(node.image);
        return super.visit(node, data);
    }

    @Override
    public Object visit(ASTReference node, Object data) {
        if (QueryPropertyMarker.findInstance(node).isType(ExceededOrThresholdMarkerJexlNode.class)) {
            this.variableNames.add(ExceededOrThresholdMarkerJexlNode.getField(node));
            return data;
        } else {
            return super.visit(node, data);
        }
    }
}
