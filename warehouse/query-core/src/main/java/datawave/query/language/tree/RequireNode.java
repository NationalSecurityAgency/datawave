package datawave.query.language.tree;

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RequireNode extends QueryNode {
    private static final Logger log = LoggerFactory.getLogger(RequireNode.class.getName());

    Set<String> requiredFields = null;

    public RequireNode(Set<String> requiredFields, QueryNode... children) {
        super(children);
        this.requiredFields = requiredFields;
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("REQUIRE (");
        for (String s : requiredFields) {
            stringBuilder.append(s).append(" ");
        }
        return stringBuilder.toString().trim() + ")";
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

        return new RequireNode(requiredFields, newChildren);
    }
}
