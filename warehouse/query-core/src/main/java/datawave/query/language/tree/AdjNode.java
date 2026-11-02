package datawave.query.language.tree;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AdjNode extends HardAndNode {

    static Logger log = LoggerFactory.getLogger(AdjNode.class.getName());

    protected int searchDistance;

    public AdjNode(int distance, QueryNode... children) {
        super(children);
        searchDistance = distance;
    }

    @Override
    public QueryNode clone() {
        QueryNode[] newChildren = new QueryNode[children.size()];
        for (int x = 0; x < children.size(); ++x) {
            newChildren[x] = children.get(x).clone();
        }

        return new AdjNode(searchDistance, newChildren);
    }

    @Override
    public String toString() {
        return "ADJ" + searchDistance;
    }

}
