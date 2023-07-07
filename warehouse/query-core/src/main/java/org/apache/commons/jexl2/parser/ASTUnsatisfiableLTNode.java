package org.apache.commons.jexl2.parser;

/**
 * An ASTERNode which cannot be run against the index. Typically, this signifies that we *could* have run the query but chose not to for any number of reasons
 *
 * 1. Regular expression we don't support (".*", ".*foo.*") 2. Non-indexed 3. Expansion of the regex exceeded configured limits
 */
public class ASTUnsatisfiableLTNode extends ASTLTNode {
    public ASTUnsatisfiableLTNode(int id) {
        super(id);
    }

    public ASTUnsatisfiableLTNode(Parser p, int id) {
        super(p, id);
    }

    /** Accept the visitor. **/
    @Override
    public Object jjtAccept(ParserVisitor visitor, Object data) {
        return visitor.visit(this, data);
    }

    public static ASTUnsatisfiableLTNode create() {
        return new ASTUnsatisfiableLTNode(ParserTreeConstants.JJTLTNODE);
    }
}
