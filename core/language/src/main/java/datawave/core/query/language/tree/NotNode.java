package datawave.core.query.language.tree;

public class NotNode extends QueryNode {

    public NotNode(QueryNode... children) {
        super(children);
    }

    @Override
    public String toString() {
        return "NOT";
    }

    @Override
    protected boolean isParentDifferent() {
        return false;
    }

    @Override
    public QueryNode clone() {
        QueryNode[] newChildren = new QueryNode[children.size()];
        for (int x = 0; x < children.size(); ++x) {
            newChildren[x] = children.get(x).clone();
        }

        return new NotNode(newChildren);
    }

}
