package datawave.query.lucene.visitors;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import org.apache.lucene.queryparser.flexible.core.nodes.AndQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.FieldQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.GroupQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.OrQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;

/**
 * A {@link BaseVisitor} implementation that will search a query for any grouped phrases with similar fields, it would identify cases like
 * {@code FOO:(aaa bbb ccc)}, {@code (FOO:aaa bbb ccc)}, and {@code (FOO:aaa AND FOO:bbb AND FOO:ccc)}
 */

public class GroupedInterpretationVisitor extends BaseVisitor {

    public enum JUNCTION {
        AND(QueryNodeType.AND, AndQueryNode::new), OR(QueryNodeType.OR, OrQueryNode::new);

        private final QueryNodeType type;
        private final Function<List<QueryNode>,QueryNode> constructor;

        JUNCTION(QueryNodeType type, Function<List<QueryNode>,QueryNode> constructor) {
            this.type = type;
            this.constructor = constructor;
        }

        public QueryNodeType getType() {
            return type;
        }

    }

    /**
     * Returns a list of copies of nodes representing fielded terms with unfielded terms directly following them that are conjoined by the specified junction.
     *
     * @param node
     *            the node
     * @param junction
     *            the junction type AND/OR
     * @return the list of ambiguous nodes
     */
    public static List<QueryNode> check(QueryNode node, JUNCTION junction) {
        GroupedInterpretationVisitor visitor = new GroupedInterpretationVisitor(junction);
        // noinspection unchecked
        return (List<QueryNode>) visitor.visit(node, new ArrayList<QueryNode>());
    }

    private final JUNCTION junction;

    private GroupedInterpretationVisitor(JUNCTION junction) {
        this.junction = junction;
    }

    @Override
    public Object visit(AndQueryNode node, Object data) {
        return this.junction == JUNCTION.AND ? checkJunction(node, data) : super.visit(node, data);
    }

    @Override
    public Object visit(OrQueryNode node, Object data) {
        return this.junction == JUNCTION.OR ? checkJunction(node, data) : super.visit(node, data);
    }

    @Override
    public Object visit(GroupQueryNode node, Object data) {
        // If the group node consists entirely of a single fielded term with ambiguously ORed unfielded phrases, add a copy of the group node to the data.
        if (groupConsistsOfUnfieldedTerms(node, false)) {
            // noinspection unchecked
            ((List<QueryNode>) data).add(copy(node));
            return data;
        } else {
            // Otherwise, examine the children.
            return super.visit(node, data);
        }
    }

    /**
     * Checks the given junction (AND/OR) node for any unfielded terms directly following a fielded term.
     *
     * @param node
     *            the node
     * @param data
     *            the data
     * @return the updated data
     */
    @SuppressWarnings("unchecked")

    private Object checkJunction(QueryNode node, Object data) {
        for (QueryNode child : node.getChildren()) {
            QueryNodeType type = QueryNodeType.get(child.getClass());
            switch (type) {
                case GROUP:
                    if (groupConsistsOfUnfieldedTerms((GroupQueryNode) child, false)) {
                        ((List<QueryNode>) data).add(copy(child));
                    } else {
                        super.visit(child, data);
                    }
                    break;
                default:
                    super.visit(child, data);
                    break;
            }
        }

        return data;
    }

    private boolean groupConsistsOfUnfieldedTerms(GroupQueryNode node, boolean fieldedTermFound) {
        // A GROUP node will have just one child.
        QueryNode child = node.getChild();
        QueryNodeType type = QueryNodeType.get(child.getClass());
        if (type == QueryNodeType.GROUP) {
            // child is a nested group. examine it.
            return groupConsistsOfUnfieldedTerms((GroupQueryNode) child, fieldedTermFound);
        } else if (type == junction.getType()) {
            // examine the children.
            return junctionConsistsOfUnfieldedTerms(child, fieldedTermFound);
        } else {
            // The child is not one of the target types we want
            return false;
        }
    }

    private boolean junctionConsistsOfUnfieldedTerms(QueryNode node, boolean fieldedTermFound) {
        List<QueryNode> children = node.getChildren();
        String prevField = "";

        for (QueryNode child : children) {
            QueryNodeType type = QueryNodeType.get(child.getClass());
            if (type == QueryNodeType.FIELD) {
                if (!((FieldQueryNode) child).getFieldAsString().isEmpty()) {
                    // If the field name is not empty, and we have not found a fielded term yet, mark that we've found one.
                    // If it does, we know the group is something like: FOO:(abc def ghi) or (FOO:abc AND FOO:def AND FOO:ghi)
                    // If a fielded term was found previously, then we have may something like (FOO:abc AND BAR:abc).
                    if (!fieldedTermFound) {
                        fieldedTermFound = true;
                        // make note of the field
                        prevField = ((FieldQueryNode) child).getFieldAsString();
                    } else return Objects.equals(((FieldQueryNode) child).getFieldAsString(), prevField);
                } else {
                    // The current child is an unfielded term. If no fielded term has been found yet, then we may have something like (abc AND FOO:abc).
                    return fieldedTermFound;
                }
            }
        }
        return fieldedTermFound;
    }
}
