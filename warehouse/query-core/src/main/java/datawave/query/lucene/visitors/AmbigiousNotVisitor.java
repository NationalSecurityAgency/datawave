package datawave.query.lucene.visitors;

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.queryparser.flexible.core.nodes.ModifierQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.NotBooleanQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;

/**
 * A visitor that checks a query for any usage of NOT with OR'd/AND'd terms before it that are not wrapped, e.g. {@code FIELD1:abc OR FIELD2:def NOT FIELD3:123}
 * should be {@code (FIELD1:abc OR FIELD2:def) NOT FIELD3:123}.
 */
public class AmbigiousNotVisitor extends BaseVisitor {

    /**
     * Returns a list of copies of any {@link NotBooleanQueryNode} instances in the given query that contain multiple unwrapped preceding terms.
     *
     * @param node
     *            the node
     * @return the list of NOT node copies
     */
    public static List<NotBooleanQueryNode> check(QueryNode node) {
        AmbigiousNotVisitor visitor = new AmbigiousNotVisitor();
        return (List<NotBooleanQueryNode>) visitor.visit(node, new ArrayList<NotBooleanQueryNode>());
    }

    @Override
    public Object visit(NotBooleanQueryNode node, Object data) {
        for (QueryNode child : node.getChildren()) {
            QueryNodeType type = QueryNodeType.get(child.getClass());
            switch (type) {
                case OR:
                case AND: {
                    // If we see an OR or AND instead of GROUP, then we have multiple unwrapped terms preceding the NOT. Return a copy of this node.
                    ((List<NotBooleanQueryNode>) data).add((NotBooleanQueryNode) copy(node));
                }
                case MODIFIER: {
                    break;
                }
                default:
                    continue;
            }
        }
        return data;
    }
}
