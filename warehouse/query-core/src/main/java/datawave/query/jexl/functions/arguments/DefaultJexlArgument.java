package datawave.query.jexl.functions.arguments;

import org.apache.commons.jexl2.parser.JexlNode;

public abstract class DefaultJexlArgument implements JexlArgument {

    JexlArgumentType type;
    JexlNode node;
    String name;

    public DefaultJexlArgument(JexlArgumentType type, String name, JexlNode node) {
        this.type = type;
        this.name = name;
        this.node = node;
    }

    @Override
    public String getJexlArgumentName() {
        return name;
    }

    @Override
    public JexlNode getJexlArgumentNode() {
        return node;
    }

    @Override
    public JexlArgumentType getArgumentType() {
        return type;
    }

}
