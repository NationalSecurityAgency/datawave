package datawave.query.lucene.visitors;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QuotedFieldQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.SlopQueryNode;

/**
 * A visitor that checks a query for any slop phrases whose value is not at least the number of terms present.
 */
public class InvalidSlopProximityVisitor extends BaseVisitor {

    /**
     * Returns copies of each {@link SlopQueryNode} that has a value that is less than the number of terms.
     *
     * @param node
     *            the node
     * @return the copies
     */
    public static List<InvalidSlop> check(QueryNode node) {
        InvalidSlopProximityVisitor visitor = new InvalidSlopProximityVisitor();
        List<InvalidSlop> list = new ArrayList<>();
        visitor.visit(node, list);
        return list;
    }

    @Override
    public Object visit(SlopQueryNode node, Object data) {
        QuotedFieldQueryNode phrase = (QuotedFieldQueryNode) node.getChild();
        String text = phrase.getTextAsString();
        int totalTerms = getTotalTerms(text);
        if (totalTerms > node.getValue()) {
            // noinspection unchecked
            ((List<InvalidSlop>) data).add(new InvalidSlop((SlopQueryNode) copy(node), totalTerms));
        }

        return data;
    }

    private int getTotalTerms(String string) {
        return string.trim().split("\\s+").length;
    }

    public static class InvalidSlop {
        private final SlopQueryNode node;
        private final int minimum;

        public InvalidSlop(SlopQueryNode node, int minimum) {
            this.node = node;
            this.minimum = minimum;
        }

        public SlopQueryNode getNode() {
            return node;
        }

        public int getMinimum() {
            return minimum;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (!(obj instanceof InvalidSlop))
                return false;
            InvalidSlop other = (InvalidSlop) obj;
            if (this.hashCode() != other.hashCode())
                return false;
            return Objects.equals(this.node, other.node) && Objects.equals(this.getMinimum(), other.getMinimum());
        }

        @Override
        public int hashCode() {
            return Objects.hash(this.node, this.getMinimum());
        }

        @Override
        public String toString() {
            return new StringJoiner(", ", InvalidSlop.class.getSimpleName() + "[", "]").add("node=" + node).add("minimum=" + minimum).toString();
        }
    }
}
