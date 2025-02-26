package datawave.query.jexl.visitors;

import static datawave.query.jexl.JexlASTHelper.jexlFeatures;
import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.EXCEEDED_OR;

import java.util.Set;

import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTIdentifier;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.ParseException;
import org.apache.commons.jexl3.parser.Parser;
import org.apache.commons.jexl3.parser.StringProvider;
import org.apache.commons.jexl3.parser.TokenMgrException;

import com.google.common.collect.Sets;

import datawave.query.jexl.nodes.ExceededOr;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import datawave.webservice.query.exception.BadRequestQueryException;
import datawave.webservice.query.exception.DatawaveErrorCode;

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
        Parser parser = new Parser(new StringProvider(";"));

        // Parse the query
        try {
            return parseQuery(parser.parse(null, jexlFeatures(), query, null));
        } catch (TokenMgrException e) {
            BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.UNPARSEABLE_JEXL_QUERY, e.getMessage());
            throw new IllegalArgumentException(qe);
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
        this.variableNames.add(node.getName());
        return super.visit(node, data);
    }

    @Override
    public Object visit(ASTAndNode node, Object data) {
        QueryPropertyMarker.Instance instance = QueryPropertyMarker.findInstance(node);
        if (instance.isType(EXCEEDED_OR)) {
            ExceededOr exceededOr = new ExceededOr(instance.getSource());
            this.variableNames.add(exceededOr.getField());
            return data;
        } else {
            return super.visit(node, data);
        }
    }
}
