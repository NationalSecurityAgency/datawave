package nsa.datawave.query.rewrite.jexl.visitors;

import java.io.IOException;
import java.io.StringReader;
import java.util.Set;

import org.apache.commons.jexl2.parser.ASTIdentifier;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParseException;
import org.apache.commons.jexl2.parser.Parser;

import com.google.common.collect.Sets;

/**
 * Extracts all of the identifier names from a query. This exists only because the getVariables() method in JexlEngine is broken in the released versions of
 * commons-jexl
 * 
 */
public class VariableNameVisitor extends BaseVisitor {
    
    private Set<String> variableNames = Sets.newHashSet();
    
    public VariableNameVisitor() {}
    
    /**
     * Static method to run a depth-first traversal over the AST
     * 
     * @param query
     *            JEXL query string
     * @throws IOException
     */
    public static Set<String> parseQuery(String query) throws ParseException {
        // Instantiate a parser and visitor
        Parser parser = new Parser(new StringReader(";"));
        
        // Parse the query
        return parseQuery(parser.parse(new StringReader(query), null));
    }
    
    /**
     * Print a representation of this AST
     * 
     * @param query
     */
    public static Set<String> parseQuery(JexlNode query) {
        VariableNameVisitor printer = new VariableNameVisitor();
        
        // visit() and get the root which is the root of a tree of Boolean Logic Iterator<Key>'s
        query.jjtAccept(printer, "");
        return printer.variableNames;
    }
    
    public Object visit(ASTIdentifier node, Object data) {
        this.variableNames.add(node.image);
        return super.visit(node, data);
    }
}
