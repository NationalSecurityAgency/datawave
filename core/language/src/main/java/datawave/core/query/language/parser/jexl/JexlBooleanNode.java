package datawave.core.query.language.parser.jexl;

import java.util.ArrayList;
import java.util.List;

public class JexlBooleanNode extends JexlNode {
    public enum Type {
        AND, OR, NOT
    }

    private Type type = null;

    private JexlBooleanNode() {
        super(new ArrayList<>());
    }

    public JexlBooleanNode(Type type) {
        super(new ArrayList<>());
        this.type = type;
    }

    public JexlBooleanNode(Type type, List<JexlNode> children) {
        super(children);
        this.type = type;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        String mainOp = null;

        switch (type) {
            case AND:
                mainOp = " && ";
                break;
            case OR:
                mainOp = " || ";
                break;
            case NOT:
                // NOT AND's all selectors and then NOTs the last
                mainOp = " && ";
                break;
        }

        List<JexlNode> c = getChildren();
        for (int x = 0; x < c.size(); x++) {
            JexlNode n = c.get(x);

            if (x > 0 && type == Type.NOT) {
                sb.append("!(");
                sb.append(n);
                sb.append(")");
            } else {
                sb.append(n);
            }

            if (x < c.size() - 1) {
                sb.append(mainOp);
            }

        }

        return sb.toString();
    }

    public Type getType() {
        return type;
    }
}
