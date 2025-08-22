package datawave.query.lucene.visitors;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.lucene.queryparser.flexible.core.nodes.AndQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.FieldQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.GroupQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;

/**
 * A {@link BaseVisitor} implementation that will search a query for any grouped phrases with similar fields, it would identify cases like
 * {@code FOO:(aaa bbb ccc)}, {@code (FOO:aaa bbb ccc)}, and {@code (FOO:aaa AND FOO:bbb AND FOO:ccc)}
 */

public class GroupedInterpretationVisitor extends BaseVisitor {

    /**
     * Returns a list of copies of nodes representing fielded terms with unfielded terms directly following them that are conjoined by the specified junction.
     *
     * @param node
     *            the node
     * @return the list of ambiguous nodes
     */
    public static List<QueryNode> check(QueryNode node) {
        GroupedInterpretationVisitor visitor = new GroupedInterpretationVisitor();
        // noinspection unchecked
        return (List<QueryNode>) visitor.visit(node, new ArrayList<QueryNode>());
    }

    @Override
    public Object visit(AndQueryNode node, Object data) {
        return checkJunction(node, data);
    }

    @Override
    public Object visit(GroupQueryNode node, Object data) {
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
     * Checks the AND junction node for any unfielded terms directly following a fielded term.
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
            if (Objects.requireNonNull(type) == QueryNodeType.GROUP) {
                if (groupConsistsOfUnfieldedTerms((GroupQueryNode) child, false)) {
                    ((List<QueryNode>) data).add(copy(child));
                } else {
                    super.visit(child, data);
                }
            } else {
                super.visit(child, data);
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
        } else if (type == QueryNodeType.AND) {
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
                FieldQueryNode fieldNode = (FieldQueryNode) child;
                if (!(fieldNode).getFieldAsString().isEmpty()) {
                    // If the field name is not empty, and we have not found a fielded term yet, mark that we've found one.
                    // If it does, we know the group is something like: FOO:(abc def ghi) or (FOO:abc AND FOO:def AND FOO:ghi)
                    // If a fielded term was found previously, then we have may something like (FOO:abc AND BAR:abc).
                    if (!fieldedTermFound) {
                        fieldedTermFound = true;
                        // make note of the field
                        prevField = fieldNode.getFieldAsString();
                    } else
                        return Objects.equals(fieldNode.getFieldAsString(), prevField);
                } else {
                    // The current child is an unfielded term. If no fielded term has been found yet, then we may have something like (abc AND FOO:abc).
                    return fieldedTermFound;
                }
            }
        }
        return fieldedTermFound;
    }
}
