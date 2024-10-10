package datawave.core.query.language.tree;

import org.apache.log4j.Logger;

public class OrNode extends QueryNode {
    private static final Logger log = Logger.getLogger(OrNode.class.getName());

    public OrNode(QueryNode... children) {
        super(children);
    }

    @Override
    public String toString() {
        return "OR";
    }

    @Override
    protected boolean isParentDifferent() {
        // OR is different from everything but OR
        if (parent == null) {
            return true;
        }
        if ("OR".equals(parent.toString())) {
            return false;
        }
        return true;
    }

    @Override
    public QueryNode clone() {
        QueryNode[] newChildren = new QueryNode[children.size()];
        for (int x = 0; x < children.size(); ++x) {
            newChildren[x] = children.get(x).clone();
        }

        return new OrNode(newChildren);
    }

}
