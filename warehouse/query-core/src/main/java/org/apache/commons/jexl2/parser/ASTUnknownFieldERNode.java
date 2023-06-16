package org.apache.commons.jexl2.parser;

/**
 *
 */
public class ASTUnknownFieldERNode extends ASTERNode {
    public ASTUnknownFieldERNode(int id) {
        super(id);
    }

    public ASTUnknownFieldERNode(Parser p, int id) {
        super(p, id);
    }

    /** Accept the visitor. **/
    @Override
    public Object jjtAccept(ParserVisitor visitor, Object data) {
        return visitor.visit(this, data);
    }

    public static ASTUnknownFieldERNode create() {
        return new ASTUnknownFieldERNode(ParserTreeConstants.JJTERNODE);
    }
}
