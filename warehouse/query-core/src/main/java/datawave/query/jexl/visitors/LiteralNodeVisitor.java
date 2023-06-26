package datawave.query.jexl.visitors;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import datawave.query.jexl.JexlASTHelper;
import org.apache.commons.jexl3.parser.ASTEQNode;
import org.apache.commons.jexl3.parser.ASTERNode;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ASTNENode;
import org.apache.commons.jexl3.parser.ASTNRNode;

import java.util.Set;

/**
 * Collect a {@link Set} of all literals/terms in the AST
 */
@SuppressWarnings("unchecked")
public class LiteralNodeVisitor extends ShortCircuitBaseVisitor {

    public static Multimap<String,String> getLiterals(ASTJexlScript script) {
        LiteralNodeVisitor visitor = new LiteralNodeVisitor();

        return (Multimap<String,String>) script.jjtAccept(visitor, HashMultimap.create());
    }

    @Override
    public Object visit(ASTEQNode node, Object data) {
        Multimap<String,String> literals = (Multimap<String,String>) data;

        String identifier = JexlASTHelper.getIdentifier(node);
        Object literal = JexlASTHelper.getLiteralValue(node);

        literals.put(identifier, (literal == null ? null : literal.toString()));

        return literals;
    }

    // Descend through these nodes
    @Override
    public Object visit(ASTJexlScript node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }

    @Override
    public Object visit(ASTERNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }

    @Override
    public Object visit(ASTNRNode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }

    @Override
    public Object visit(ASTNENode node, Object data) {
        node.childrenAccept(this, data);
        return data;
    }

}
