package datawave.query.planner.rules;

import java.lang.reflect.InvocationTargetException;

import org.apache.commons.jexl3.parser.ASTFalseNode;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.ParserTreeConstants;

import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.util.MetadataHelper;

public class FieldTransformRule implements NodeTransformRule {
    FieldRule rule;
    JexlNode falseNode = new ASTFalseNode(ParserTreeConstants.JJTFALSENODE);

    @Override
    public JexlNode apply(JexlNode node, ShardQueryConfiguration config, MetadataHelper helper) {
        if (rule.shouldPrune(node, helper)) {
            return falseNode;
        }
        if (rule.shouldModify(node, helper)) {
            node = rule.modify(node, helper);
        }
        return node;
    }

    public void setupRules(ShardQueryConfiguration config) {
        try {
            Class<? extends FieldRule> ruleClass = Class.forName(config.getFieldRuleClassName()).asSubclass(FieldRule.class);
            rule = ruleClass.getDeclaredConstructor(GenericQueryConfiguration.class).newInstance(config);
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException | ClassNotFoundException e) {
            throw new RuntimeException("Unable to load pruning rules for " + config.getFieldRuleClassName() + " " + e.getMessage());
        }

    }
}
