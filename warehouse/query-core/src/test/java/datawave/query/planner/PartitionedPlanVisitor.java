package datawave.query.planner;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ParseException;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.nodes.QueryPropertyMarker;
import datawave.query.jexl.visitors.BaseVisitor;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;

public class PartitionedPlanVisitor extends BaseVisitor {

    public static Collection<String> getPlans(String queryString) throws ParseException {
        ASTJexlScript script = JexlASTHelper.parseJexlQuery(queryString);
        HashSet<String> plans = new HashSet<>();
        new PartitionedPlanVisitor().visit(script, plans);
        if (plans.isEmpty()) {
            plans.add(queryString);
        }
        return plans;
    }

    @Override
    public Object visit(ASTAndNode node, Object data) {
        QueryPropertyMarker.Instance type = QueryPropertyMarker.findInstance(node);
        if (type.isType(QueryPropertyMarker.MarkerType.PLAN)) {
            ((Set) data).add(JexlStringBuildingVisitor.buildQuery(type.getSource()));
            return data;
        } else {
            return super.visit(node, data);
        }
    }
}
