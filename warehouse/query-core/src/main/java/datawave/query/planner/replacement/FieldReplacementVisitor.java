package datawave.query.planner.replacement;

import datawave.core.common.logging.ThreadConfigurableLogger;
import datawave.query.jexl.visitors.RebuildingVisitor;
import datawave.query.planner.replacement.rules.FieldReplacementRule;
import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTIdentifier;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.log4j.Logger;

import java.util.List;

public class FieldReplacementVisitor extends RebuildingVisitor {
    private static final Logger log = ThreadConfigurableLogger.getLogger(FieldReplacementVisitor.class);
    private final List<FieldReplacementRule> rules;

    public FieldReplacementVisitor(List<FieldReplacementRule> rules) {
        this.rules = rules;
    }

    public static ASTJexlScript apply(ASTJexlScript script, List<FieldReplacementRule> rules) {
        FieldReplacementVisitor visitor = new FieldReplacementVisitor(rules);

        return visitor.apply(script);
    }

    @Override
    public Object visit(ASTAndNode node, Object data) {
        return applyRules(super.visit(node, data));
    }

    @Override
    public Object visit(ASTIdentifier node, Object data) {
        return applyRules(super.visit(node, data));
    }

    public JexlNode applyRules(Object node) {
        JexlNode jexlNode = (JexlNode) node;
        for (FieldReplacementRule rule : rules) {
            if (rule.matches(jexlNode)) {
                jexlNode = rule.apply(jexlNode);
            }
        }
        return jexlNode;
    }
}
