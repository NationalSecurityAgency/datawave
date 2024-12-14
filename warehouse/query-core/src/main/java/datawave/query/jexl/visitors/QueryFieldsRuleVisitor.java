package datawave.query.jexl.visitors;

import java.util.HashSet;

import org.apache.commons.jexl3.parser.ASTJexlScript;

import datawave.query.model.QueryModel;
import datawave.query.planner.QueryPlanningStage;
import datawave.query.util.MetadataHelper;

public class QueryFieldsRuleVisitor extends BaseVisitor {
    private final MetadataHelper helper;
    private final QueryModel model;
    private final QueryPlanningStage stage;

    public QueryFieldsRuleVisitor(MetadataHelper helper, QueryModel model, QueryPlanningStage stage) {
        this.helper = helper;
        this.model = model;
        this.stage = stage;
    }

    public static ASTJexlScript applyRules(ASTJexlScript script, MetadataHelper helper, QueryModel model, QueryPlanningStage stage) {
        QueryFieldsRuleVisitor ruleVisitor = new QueryFieldsRuleVisitor(helper, model, stage);
        return (ASTJexlScript) script.jjtAccept(ruleVisitor, new HashSet<>());
    }

}
