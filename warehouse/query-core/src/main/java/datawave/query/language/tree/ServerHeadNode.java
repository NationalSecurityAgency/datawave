package datawave.query.language.tree;

import org.apache.log4j.Logger;

public class ServerHeadNode extends QueryNode {

    private static final Logger log = Logger.getLogger(ServerHeadNode.class.getName());

    public ServerHeadNode(QueryNode... children) {
        super(children);
    }

    @Override
    public String toString() {
        return getOriginalQuery();
    }

    @Override
    public String getContents() {
        StringBuilder s = new StringBuilder("[");
        for (QueryNode node : getChildren()) {
            s.append(node.getContents());
        }
        s.append("]");
        return s.toString();
    }

    @Override
    protected boolean isParentDifferent() {
        // TODO: What is this for??
        return true;
    }

    @Override
    public QueryNode clone() {
        QueryNode[] newChildren = new QueryNode[children.size()];
        for (int x = 0; x < children.size(); ++x) {
            newChildren[x] = children.get(x).clone();
        }

        return new ServerHeadNode(newChildren);
    }

}
