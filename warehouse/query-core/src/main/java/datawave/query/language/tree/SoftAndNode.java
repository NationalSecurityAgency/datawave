package datawave.query.language.tree;

import org.apache.log4j.Logger;

public class SoftAndNode extends HardAndNode {

    private static final Logger log = Logger.getLogger(SoftAndNode.class.getName());

    public SoftAndNode(QueryNode... children) {
        super(children);
    }

    @Override
    public String toString() {
        return "and";
    }

    @Override
    protected boolean isParentDifferent() {
        // and is the same as soft and and hard and
        if (parent == null)
            return true;
        if ("and".equalsIgnoreCase(parent.toString()))
            return false;
        return true;
    }

    @Override
    public QueryNode clone() {
        QueryNode[] newChildren = new QueryNode[children.size()];
        for (int x = 0; x < children.size(); ++x) {
            newChildren[x] = children.get(x).clone();
        }

        return new SoftAndNode(newChildren);
    }
}
