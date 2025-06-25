package datawave.query.planner.replacement.rules;

import org.apache.commons.jexl3.parser.JexlNode;

public interface FieldReplacementRule {
    boolean matches(JexlNode node);

    void apply(JexlNode node);
}
