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
                .append(originalQueryInfo((GroupQueryNode)node, query, new ArrayList(), new ArrayList(), ""))
                .append(" will be interpreted as: ")
                .append(LuceneQueryStringBuildingVisitor.build(node))
                .toString();
        // @formatter:on
    }

    private String originalQueryInfo(GroupQueryNode node, String query, List valueList, List fieldList, String prevField) {
        for (QueryNode child : node.getChildren()) {
            if (!(child.getChildren() == null)) {
                for (QueryNode grandchild : child.getChildren()) {
                    if (QueryNodeType.get(grandchild.getClass()) == QueryNodeType.GROUP) {
                        originalQueryInfo((GroupQueryNode) grandchild, query, valueList, fieldList, prevField);
                    } else if (!grandchild.toString().isEmpty()) {
                        String start = (grandchild.toString()).substring(((grandchild.toString()).indexOf("start=\'") + 7),
                                        (grandchild.toString()).indexOf("\' end"));
                        String end = (grandchild.toString()).substring(((grandchild.toString()).indexOf("end=\'") + 5),
                                        (grandchild.toString()).indexOf("\' field"));
                        String field = (grandchild.toString()).substring(((grandchild.toString()).indexOf("field=\'") + 7),
                                        (grandchild.toString()).indexOf("\' text"));

                        valueList.add(query.substring(Integer.parseInt(start), Integer.parseInt(end)));
                        if (!field.isEmpty() && !Objects.equals(field, prevField)) {
                            fieldList.add(field);
                            prevField = field;
                        }
                    }
                }
            } else if (!child.toString().isEmpty()) {
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
        return "field(s): " + fieldList + " with value(s): " + valueList;
    }
}
