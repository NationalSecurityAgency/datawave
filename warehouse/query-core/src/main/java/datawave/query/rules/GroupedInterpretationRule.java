package datawave.query.rules;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;
import org.apache.lucene.queryparser.flexible.core.nodes.FieldQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.GroupQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;

import datawave.query.lucene.visitors.GroupedInterpretationVisitor;
import datawave.query.lucene.visitors.LuceneQueryStringBuildingVisitor;
import datawave.query.lucene.visitors.QueryNodeType;

/**
 * An implementation of {@link QueryRule} that interprets a LUCENE query for any grouped phrases with fields, e.g. {@code FOO:(aaa bbb ccc)},
 * {@code (FOO:aaa bbb ccc)} and will return a LUCENE string warning message to let the user know how the query will be interpreted e.g. "Operator precedence
 * may be missing, field(s) {@code [FOO]} with value(s) {@code [aaa, bbb, ccc]} will be interpreted as {@code (FOO:aaa AND FOO:bbb AND FOO:ccc)}"
 */
public class GroupedInterpretationRule extends ShardQueryRule {

    private static final Logger log = Logger.getLogger(GroupedInterpretationRule.class);

    public GroupedInterpretationRule() {}

    public GroupedInterpretationRule(String name) {
        super(name);
    }

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
            QueryNode luceneQuery = (QueryNode) config.getParsedQuery();
            List<QueryNode> interpretNodes = GroupedInterpretationVisitor.check(luceneQuery);
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
                .append(formatGroup((GroupQueryNode) node))
                .append(" will be interpreted as: ")
                .append(LuceneQueryStringBuildingVisitor.build(node))
                .toString();
        // @formatter:on
    }

    // Return a formatted string containing the fields and values in the group.
    private String formatGroup(GroupQueryNode node) {
        Pair<Set<String>,List<String>> fieldValues = Pair.of(new HashSet<>(), new ArrayList<>());
        collectFieldsAndValues(node, fieldValues);
        return "field(s): " + fieldValues.getLeft() + " with value(s): " + fieldValues.getRight();
    }

    // Collect the fields and values in the given node, traveling down through the children.
    private void collectFieldsAndValues(QueryNode node, Pair<Set<String>,List<String>> fieldValues) {
        for (QueryNode child : node.getChildren()) {
            QueryNodeType childType = QueryNodeType.get(child.getClass());
            switch (childType) {
                case GROUP:
                case OR:
                case AND:
                    collectFieldsAndValues(child, fieldValues);
                default:
                    // Skip any non-field query nodes.
                    if (child instanceof FieldQueryNode) {
                        FieldQueryNode fqn = (FieldQueryNode) child;
                        String field = fqn.getField().toString();
                        if (!field.isEmpty()) {
                            fieldValues.getLeft().add(field);
                        }
                        fieldValues.getRight().add(fqn.getTextAsString());
                    }
            }
        }
    }
}
