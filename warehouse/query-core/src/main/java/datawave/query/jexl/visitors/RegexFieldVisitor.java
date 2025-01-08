package datawave.query.jexl.visitors;

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.jexl3.parser.ASTERNode;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ASTNRNode;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.ParseException;

import datawave.query.jexl.JexlASTHelper;

public class RegexFieldVisitor extends BaseVisitor {
    /**
     * Get a set of all fields using regex operators
     *
     * @param query
     *            the query string
     * @return a non-null set of query fields using regexes
     */
    public static Set<String> parseRegexFields(String query) {
        try {
            ASTJexlScript script = JexlASTHelper.parseAndFlattenJexlQuery(query);
            return parseRegexFields(script);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Get a set of all fields using regex operators
     *
     * @param script
     *            the parsed query
     * @return a non-null set of query fields using regexes
     */
    public static Set<String> parseRegexFields(ASTJexlScript script) {
        RegexFieldVisitor visitor = new RegexFieldVisitor();
        return (Set<String>) script.jjtAccept(visitor, new HashSet<>());
    }

    private RegexFieldVisitor() {
        // no-op
    }

    @Override
    public Object visit(ASTERNode node, Object data) {
        return addField(node, (Set<String>) data);
    }

    @Override
    public Object visit(ASTNRNode node, Object data) {
        return addField(node, (Set<String>) data);
    }

    private Set<String> addField(JexlNode node, Set<String> data) {
        String fieldName = JexlASTHelper.getIdentifier(node);
        if (fieldName != null) {
            data.add(fieldName);
        }
        return data;
    }
}
