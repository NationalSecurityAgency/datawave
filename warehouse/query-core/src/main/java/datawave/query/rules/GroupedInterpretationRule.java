package datawave.query.rules;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.log4j.Logger;
import org.apache.lucene.queryparser.flexible.core.nodes.GroupQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;

import datawave.query.lucene.visitors.GroupedInterpretationVisitor;
import datawave.query.lucene.visitors.LuceneQueryStringBuildingVisitor;
import datawave.query.lucene.visitors.QueryNodeType;

/**
 * An implementation of {@link QueryRule} that interprets a LUCENE query for any grouped phrases with fields, e.g. {@code FOO:(aaa bbb ccc)},
 * {@code (FOO:aaa bbb ccc)} and will return a LUCENE string warning message to let the user know how the query will be interpreted e.g.
 * "{@code FOO:(aaa bbb ccc)} will be interpreted as {@code (FOO:aaa AND FOO:bbb AND FOO:ccc)}"
 */
public class GroupedInterpretationRule extends ShardQueryRule {

    private static final Logger log = Logger.getLogger(GroupedInterpretationRule.class);

    public GroupedInterpretationRule() {}

    public GroupedInterpretationRule(String name) {
        super(name);
    }

    private String query;

    @Override
    protected Syntax getSupportedSyntax() {
        return Syntax.LUCENE;
    }

    @Override
    public QueryRuleResult validate(QueryValidationConfiguration configuration) throws Exception {
        ShardQueryValidationConfiguration config = (ShardQueryValidationConfiguration) configuration;
        if (log.isDebugEnabled()) {
            log.debug("Validating config against instance '" + getName() + "' of " + getClass() + ": " + config);
        }

        QueryRuleResult result = new QueryRuleResult(getName());
        try {
            query = config.getQueryString();
            QueryNode luceneQuery = (QueryNode) config.getParsedQuery();
            List<QueryNode> interpretNodes = GroupedInterpretationVisitor.check(luceneQuery, GroupedInterpretationVisitor.JUNCTION.AND);
            interpretNodes.stream().map(this::formatMessage).forEach(result::addMessage);
        } catch (Exception e) {
            log.error("Error occurred when validating against instance '" + getName() + "' of " + getClass(), e);
            result.setException(e);
        }
        return result;
    }

    @Override
    public QueryRule copy() {
        return new GroupedInterpretationRule(name);
    }

    // Return a message about the given nodes.
    private String formatMessage(QueryNode node) {
        // @formatter:off
        return new StringBuilder()
                .append("Operator precedence may be missing, ")
                .append(queryInfo((GroupQueryNode)node, query, new ArrayList(), new ArrayList(), new ArrayList()))
                .append(" will be interpreted as: ")
                .append(LuceneQueryStringBuildingVisitor.build(node))
                .toString();
        // @formatter:on
    }

    public String queryInfo (GroupQueryNode node, String query, ArrayList fieldList, ArrayList valueList, ArrayList prevField) {
        prevField.add("");

        ArrayList[] fieldValueList = new ArrayList[3];
        fieldValueList[0] = fieldList;
        fieldValueList[1] = valueList;
        fieldValueList[2] = prevField;

        return originalQueryInfo(node, query, fieldValueList);
    }

    private String originalQueryInfo(GroupQueryNode node, String query, ArrayList[] fieldValueList) {
        // first checks to see if query is nested
        QueryNode nestedChild = node.getChild();
        if (QueryNodeType.get(nestedChild.getClass()) == QueryNodeType.GROUP) {
            return originalQueryInfo((GroupQueryNode) nestedChild, query, fieldValueList);
        }
        for (QueryNode child : node.getChildren()) {
            if (!(child.getChildren() == null)) {
                for (QueryNode grandchild : child.getChildren()) {
                    if (QueryNodeType.get(grandchild.getClass()) == QueryNodeType.GROUP) {
                        // checks if child is nested
                        originalQueryInfo((GroupQueryNode) grandchild, query, fieldValueList);
                    } else if (!grandchild.toString().isEmpty()) {
                        fieldValueLists(grandchild, query, fieldValueList);
                    }
                }
            } else if (!child.toString().isEmpty()) {
                fieldValueLists(child, query, fieldValueList);
            }
        }

        Object fieldList = fieldValueList[0];
        Object valueList = fieldValueList[1];

        return "field(s): " + fieldList + " with value(s): " + valueList;
    }

    private ArrayList[] fieldValueLists(QueryNode node, String query, ArrayList[] fieldValueList) {
        // index 0 = fields, index 1 = values, index 3 = previous fields
        ArrayList fieldList = fieldValueList[0];
        ArrayList valueList = fieldValueList[1];
        ArrayList prevField = fieldValueList[2];

        if (!node.toString().isEmpty()) {
            String start = (node.toString()).substring(((node.toString()).indexOf("start=\'") + 7), (node.toString()).indexOf("\' end"));
            String end = (node.toString()).substring(((node.toString()).indexOf("end=\'") + 5), (node.toString()).indexOf("\' field"));
            String field = (node.toString()).substring(((node.toString()).indexOf("field=\'") + 7), (node.toString()).indexOf("\' text"));

            valueList.add(query.substring(Integer.parseInt(start), Integer.parseInt(end)));
            if (!field.isEmpty() && !Objects.equals(field, prevField.get(0))) {
                fieldList.add(field);
                prevField.set(0, field);
                fieldValueList[2] = prevField;
            }
        }

        return fieldValueList;
    }
}
