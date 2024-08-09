package datawave.core.query.language.tree;

import org.apache.log4j.Logger;

/**
 * performs a logical and on the keys of the subtree's results
 */
public class HardAndNode extends QueryNode {
    private static final Logger log = Logger.getLogger(HardAndNode.class.getName());

    public HardAndNode(QueryNode... children) {
        super(children);
    }

    @Override
    public String toString() {
        return "AND";
    }

    @Override
    protected boolean isParentDifferent() {
        // and is the same as soft and and hard and
        return parent == null || !"AND".equalsIgnoreCase(parent.toString());
    }

    @Override
    public QueryNode clone() {
        QueryNode[] newChildren = new QueryNode[children.size()];
        for (int x = 0; x < children.size(); ++x) {
            newChildren[x] = children.get(x).clone();
        }

        return new HardAndNode(newChildren);
    }
}
