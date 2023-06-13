package org.apache.commons.jexl3.parser;

/**
 * An ASTERNode which cannot be run against the index. Typically, this signifies that we *could* have run the query but chose not to for any number of reasons
 *
 * 1. Regular expression we don't support (".*", ".*foo.*") 2. Non-indexed 3. Expansion of the regex exceeded configured limits
 */
public class ASTUnsatisfiableLENode extends ASTLENode {
    public ASTUnsatisfiableLENode(int id) {
        super(id);
    }
    
    public ASTUnsatisfiableLENode(Parser p, int id) {
        super(p, id);
    }
    
    /** Accept the visitor. **/
    @Override
    public Object jjtAccept(ParserVisitor visitor, Object data) {
        return visitor.visit(this, data);
    }
    
    public static ASTUnsatisfiableLENode create() {
        return new ASTUnsatisfiableLENode(ParserTreeConstants.JJTLENODE);
    }
}
