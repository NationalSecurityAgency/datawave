package datawave.query.jexl.nodes;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.jexl3.parser.JexlNode;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.JexlNodeFactory;

public class DroppedExpression {

    private static final String REASON_LABEL = "_Reason_";
    private static final String QUERY_LABEL = "_Query_";

    private final String query;
    private final String reason;
    private final JexlNode jexlNode;

    public DroppedExpression(JexlNode jexlNode) {
        this.query = decodeQuery(jexlNode);
        this.reason = decodeReason(jexlNode);
        this.jexlNode = jexlNode;
    }

    public DroppedExpression(String query) {
        this(query, null);
    }

    public DroppedExpression(String query, String reason) {
        this.query = query;
        this.reason = reason;
        this.jexlNode = createJexlNode();
    }

    public String getQuery() {
        return query;
    }

    public String getReason() {
        return reason;
    }

    public JexlNode getJexlNode() {
        return jexlNode;
    }

    private String decodeQuery(JexlNode source) {
        Map<String,Object> assignments = JexlASTHelper.getAssignments(source);
        Object queryObj = assignments.get(QUERY_LABEL);
        if (queryObj != null) {
            return String.valueOf(queryObj);
        } else {
            return null;
        }
    }

    private String decodeReason(JexlNode source) {
        Map<String,Object> assignments = JexlASTHelper.getAssignments(source);
        Object queryObj = assignments.get(REASON_LABEL);
        if (queryObj != null) {
            return String.valueOf(queryObj);
        } else {
            return null;
        }
    }

    protected JexlNode createJexlNode() {
        List<JexlNode> nodes = new ArrayList<>();
        // Create an assignment for the query and reason
        if (query != null) {
            nodes.add(JexlNodeFactory.createExpression(JexlNodeFactory.createAssignment(QUERY_LABEL, query)));
        }
        if (reason != null) {
            nodes.add(JexlNodeFactory.createExpression(JexlNodeFactory.createAssignment(REASON_LABEL, reason)));
        }

        // now create the source
        return (nodes.size() > 1) ? JexlNodeFactory.createAndNode(nodes) : nodes.get(0);
    }
}
