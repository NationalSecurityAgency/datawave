package datawave.query.planner.replacement.rules;

import datawave.query.jexl.JexlASTHelper;
import org.apache.commons.jexl3.parser.ASTIdentifier;
import org.apache.commons.jexl3.parser.JexlNode;

public class DirectFieldReplacementRule implements FieldReplacementRule {
    private String field = null;
    private String replacement = null;

    public DirectFieldReplacementRule() {
    }

    public DirectFieldReplacementRule(String field, String replacement) {
        this.field = field;
        this.replacement = replacement;
    }

    public boolean matches(JexlNode node) {
        return node instanceof ASTIdentifier && ((ASTIdentifier) node).getName().equals(field);
    }

    public void apply(JexlNode node) {
        JexlASTHelper.setField(node, replacement);
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public String getReplacement() {
        return replacement;
    }

    public void setReplacement(String replacement) {
        this.replacement = replacement;
    }
}
