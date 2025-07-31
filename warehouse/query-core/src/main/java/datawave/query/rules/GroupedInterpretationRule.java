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
 * An implementation of {@link QueryRule} that checks a LUCENE query for any grouped phrases with the same fields, e.g. {@code FOO:(aaa bbb ccc)},
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
                .append(printOriginalQueryInfo((GroupQueryNode)node, query))
                .append(" will be interpreted as: ")
                .append(LuceneQueryStringBuildingVisitor.build(node))
                .toString();
        // @formatter:on
    }

    private String printOriginalQueryInfo(GroupQueryNode node, String query) {
        QueryNode child = node.getChild();
        QueryNodeType type = QueryNodeType.get(child.getClass());
        if (type == QueryNodeType.GROUP) {
            // child is a nested group
            return printOriginalQueryInfo((GroupQueryNode) child, query);
        } else {
            return getOriginalQueryInfo(child, query);
        }
    }

    private String getOriginalQueryInfo(QueryNode node, String query) {
        // check node index then get query between indexes, return list
        List<String> valueList = new ArrayList<>();
        List<String> fieldList = new ArrayList<>();
        String prevField = "";

        List<QueryNode> children = node.getChildren();
        for (QueryNode child : children) {
            if (!child.toString().isEmpty()) {
                String start = (child.toString()).substring(((child.toString()).indexOf("start=\'") + 7), (child.toString()).indexOf("\' end"));
                String end = (child.toString()).substring(((child.toString()).indexOf("end=\'") + 5), (child.toString()).indexOf("\' field"));
                String field = (child.toString()).substring(((child.toString()).indexOf("field=\'") + 7), (child.toString()).indexOf("\' text"));

                valueList.add(query.substring(Integer.parseInt(start), Integer.parseInt(end)));
                if (!field.isEmpty() && !Objects.equals(field, prevField)) {
                    fieldList.add(field);
                    prevField = field;
                }
            }
        }
        String message = "Field(s): " + fieldList.toString() + " with value(s): " + valueList.toString();
        return message;
    }

}
