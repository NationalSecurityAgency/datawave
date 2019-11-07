package datawave.query.jexl.visitors;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;
import java.util.TreeSet;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;

import org.apache.commons.jexl2.parser.ASTAdditiveNode;
import org.apache.commons.jexl2.parser.ASTAdditiveOperator;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTAssignment;
import org.apache.commons.jexl2.parser.ASTDivNode;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTFalseNode;
import org.apache.commons.jexl2.parser.ASTFunctionNode;
import org.apache.commons.jexl2.parser.ASTGENode;
import org.apache.commons.jexl2.parser.ASTGTNode;
import org.apache.commons.jexl2.parser.ASTIdentifier;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ASTLENode;
import org.apache.commons.jexl2.parser.ASTLTNode;
import org.apache.commons.jexl2.parser.ASTMethodNode;
import org.apache.commons.jexl2.parser.ASTModNode;
import org.apache.commons.jexl2.parser.ASTMulNode;
import org.apache.commons.jexl2.parser.ASTNENode;
import org.apache.commons.jexl2.parser.ASTNRNode;
import org.apache.commons.jexl2.parser.ASTNotNode;
import org.apache.commons.jexl2.parser.ASTNullLiteral;
import org.apache.commons.jexl2.parser.ASTNumberLiteral;
import org.apache.commons.jexl2.parser.ASTOrNode;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.ASTReferenceExpression;
import org.apache.commons.jexl2.parser.ASTSizeMethod;
import org.apache.commons.jexl2.parser.ASTStringLiteral;
import org.apache.commons.jexl2.parser.ASTTrueNode;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParseException;
import org.apache.log4j.Logger;

import com.google.common.collect.Sets;

/**
 * A Jexl visitor which builds an equivalent Jexl query.
 * 
 */
public class JexlStringBuildingVisitor extends BaseVisitor {
    protected static final Logger log = Logger.getLogger(JexlStringBuildingVisitor.class);
    protected static final char BACKSLASH = '\\';
    protected static final char STRING_QUOTE = '\'';
    
    // allowed methods for composition. Nothing that mutates the collection is allowed, thus we have:
    private Set<String> allowedMethods = Sets.newHashSet("contains", "retainAll", "containsAll", "isEmpty", "size", "equals", "hashCode", "getValueForGroup",
                    "getGroupsForValue", "getValuesForGroups", "toString", "values", "min", "max", "lessThan", "greaterThan", "compareWith");
    
    protected boolean sortDedupeChildren;
    
    public JexlStringBuildingVisitor() {
        this(false);
    }
    
    public JexlStringBuildingVisitor(boolean sortDedupeChildren) {
        this.sortDedupeChildren = sortDedupeChildren;
    }
    
    /**
     * Build a String that is the equivalent JEXL query.
     * 
     * @param script
     *            An ASTJexlScript
     * @param sortDedupeChildren
     *            Whether or not to sort the child nodes, and dedupe them. Note: Only siblings (children with the same parent node) will be deduped. Flatten
     *            beforehand for maximum 'dedupeage'.
     * @return
     */
    public static String buildQuery(JexlNode script, boolean sortDedupeChildren) {
        
        JexlStringBuildingVisitor visitor = new JexlStringBuildingVisitor(sortDedupeChildren);
        
        String s = null;
        try {
            StringBuilder sb = (StringBuilder) script.jjtAccept(visitor, new StringBuilder());
            
            s = sb.toString();
            
            try {
                JexlASTHelper.parseJexlQuery(s);
            } catch (ParseException e) {
                log.error("Could not parse JEXL AST after performing transformations to run the query", e);
                
                for (String line : PrintingVisitor.formattedQueryStringList(script)) {
                    log.error(line);
                }
                log.error("");
                
                QueryException qe = new QueryException(DatawaveErrorCode.QUERY_EXECUTION_ERROR, e);
                throw new DatawaveFatalQueryException(qe);
            }
        } catch (StackOverflowError e) {
            
            throw e;
        }
        return s;
    }
    
    /**
     * Build a String that is the equivalent JEXL query.
     *
     * @param script
     *            An ASTJexlScript
     * @return
     */
    public static String buildQuery(JexlNode script) {
        return buildQuery(script, false);
    }
    
    /**
     * Build a String that is the equivalent JEXL query.
     * 
     * @param script
     *            An ASTJexlScript
     * @param sortDedupeChildren
     *            Whether or not to sort the child nodes, and dedupe them. Note: Only siblings (children with the same parent node) will be deduped. Flatten
     *            beforehand for maximum 'dedupeage'.
     * @return
     */
    public static String buildQueryWithoutParse(JexlNode script, boolean sortDedupeChildren) {
        JexlStringBuildingVisitor visitor = new JexlStringBuildingVisitor(sortDedupeChildren);
        
        String s = null;
        try {
            StringBuilder sb = (StringBuilder) script.jjtAccept(visitor, new StringBuilder());
            
            s = sb.toString();
        } catch (StackOverflowError e) {
            
            throw e;
        }
        return s;
    }
    
    /**
     * Build a String that is the equivalent JEXL query.
     *
     * @param script
     *            An ASTJexlScript
     * @return
     */
    public static String buildQueryWithoutParse(JexlNode script) {
        return buildQueryWithoutParse(script, false);
    }
    
    public Object visit(ASTOrNode node, Object data) {
        
        StringBuilder sb = (StringBuilder) data;
        
        int numChildren = node.jjtGetNumChildren();
        
        JexlNode parent = node.jjtGetParent();
        boolean wrapIt = false;
        if (!(parent instanceof ASTReferenceExpression || parent instanceof ASTJexlScript || parent instanceof ASTOrNode || numChildren == 0)) {
            wrapIt = true;
            sb.append("(");
        }
        
        Collection<String> childStrings = (sortDedupeChildren) ? new TreeSet<>() : new ArrayList<>(numChildren);
        StringBuilder childSB = new StringBuilder();
        for (int i = 0; i < numChildren; i++) {
            node.jjtGetChild(i).jjtAccept(this, childSB);
            childStrings.add(childSB.toString());
            childSB.setLength(0);
        }
        sb.append(String.join(" || ", childStrings));
        
        if (wrapIt)
            sb.append(")");
        
        return data;
    }
    
    public Object visit(ASTAndNode node, Object data) {
        StringBuilder sb = (StringBuilder) data;
        
        int numChildren = node.jjtGetNumChildren();
        
        JexlNode parent = node.jjtGetParent();
        boolean wrapIt = false;
        if (!(parent instanceof ASTReferenceExpression || parent instanceof ASTJexlScript || parent instanceof ASTAndNode || numChildren == 0)) {
            wrapIt = true;
            sb.append("(");
        }
        
        Collection<String> childStrings = (sortDedupeChildren) ? new TreeSet<>() : new ArrayList<>(numChildren);
        StringBuilder childSB = new StringBuilder();
        for (int i = 0; i < numChildren; i++) {
            node.jjtGetChild(i).jjtAccept(this, childSB);
            childStrings.add(childSB.toString());
            childSB.setLength(0);
        }
        sb.append(String.join(" && ", childStrings));
        
        if (wrapIt)
            sb.append(")");
        
        return data;
    }
    
    public Object visit(ASTEQNode node, Object data) {
        StringBuilder sb = (StringBuilder) data;
        
        int numChildren = node.jjtGetNumChildren();
        
        if (2 != numChildren) {
            throw new IllegalArgumentException("An ASTEQNode has more than two children");
        }
        
        node.jjtGetChild(0).jjtAccept(this, sb);
        
        sb.append(" == ");
        
        node.jjtGetChild(1).jjtAccept(this, sb);
        
        return sb;
    }
    
    public Object visit(ASTNENode node, Object data) {
        StringBuilder sb = (StringBuilder) data;
        
        int numChildren = node.jjtGetNumChildren();
        
        if (2 != numChildren) {
            throw new IllegalArgumentException("An ASTNENode has more than two children");
        }
        
        node.jjtGetChild(0).jjtAccept(this, sb);
        
        sb.append(" != ");
        
        node.jjtGetChild(1).jjtAccept(this, sb);
        
        return sb;
    }
    
    public Object visit(ASTLTNode node, Object data) {
        StringBuilder sb = (StringBuilder) data;
        
        int numChildren = node.jjtGetNumChildren();
        
        if (2 != numChildren) {
            throw new IllegalArgumentException("An ASTLTNode has more than two children");
        }
        
        node.jjtGetChild(0).jjtAccept(this, sb);
        
        sb.append(" < ");
        
        node.jjtGetChild(1).jjtAccept(this, sb);
        
        return sb;
    }
    
    public Object visit(ASTGTNode node, Object data) {
        StringBuilder sb = (StringBuilder) data;
        
        int numChildren = node.jjtGetNumChildren();
        
        if (2 != numChildren) {
            throw new IllegalArgumentException("An ASTGTNode has more than two children");
        }
        
        node.jjtGetChild(0).jjtAccept(this, sb);
        
        sb.append(" > ");
        
        node.jjtGetChild(1).jjtAccept(this, sb);
        
        return sb;
    }
    
    public Object visit(ASTLENode node, Object data) {
        StringBuilder sb = (StringBuilder) data;
        
        int numChildren = node.jjtGetNumChildren();
        
        if (2 != numChildren) {
            throw new IllegalArgumentException("An ASTLENode has more than two children");
        }
        
        node.jjtGetChild(0).jjtAccept(this, sb);
        
        sb.append(" <= ");
        
        node.jjtGetChild(1).jjtAccept(this, sb);
        
        return sb;
    }
    
    public Object visit(ASTGENode node, Object data) {
        StringBuilder sb = (StringBuilder) data;
        
        int numChildren = node.jjtGetNumChildren();
        
        if (2 != numChildren) {
            throw new IllegalArgumentException("An ASTGENode has more than two children");
        }
        
        node.jjtGetChild(0).jjtAccept(this, sb);
        
        sb.append(" >= ");
        
        node.jjtGetChild(1).jjtAccept(this, sb);
        
        return sb;
    }
    
    public Object visit(ASTERNode node, Object data) {
        StringBuilder sb = (StringBuilder) data;
        
        int numChildren = node.jjtGetNumChildren();
        
        if (2 != numChildren) {
            throw new IllegalArgumentException("An ASTERNode has more than two children");
        }
        
        node.jjtGetChild(0).jjtAccept(this, sb);
        
        sb.append(" =~ ");
        
        node.jjtGetChild(1).jjtAccept(this, sb);
        
        return sb;
    }
    
    public Object visit(ASTNRNode node, Object data) {
        StringBuilder sb = (StringBuilder) data;
        
        int numChildren = node.jjtGetNumChildren();
        
        if (2 != numChildren) {
            throw new IllegalArgumentException("An ASTERNode has more than two children");
        }
        
        node.jjtGetChild(0).jjtAccept(this, sb);
        
        sb.append(" !~ ");
        
        node.jjtGetChild(1).jjtAccept(this, sb);
        
        return sb;
    }
    
    public Object visit(ASTNotNode node, Object data) {
        StringBuilder sb = (StringBuilder) data;
        sb.append("!");
        node.childrenAccept(this, sb);
        
        return sb;
    }
    
    public Object visit(ASTIdentifier node, Object data) {
        StringBuilder sb = (StringBuilder) data;
        
        // We want to remove the $ if present and only replace it when necessary
        String fieldName = JexlASTHelper.rebuildIdentifier(JexlASTHelper.deconstructIdentifier(node.image));
        
        sb.append(fieldName);
        
        node.childrenAccept(this, sb);
        
        return sb;
    }
    
    public Object visit(ASTNullLiteral node, Object data) {
        StringBuilder sb = (StringBuilder) data;
        sb.append("null");
        node.childrenAccept(this, sb);
        return sb;
    }
    
    public Object visit(ASTTrueNode node, Object data) {
        StringBuilder sb = (StringBuilder) data;
        sb.append("true");
        node.childrenAccept(this, sb);
        return sb;
    }
    
    public Object visit(ASTFalseNode node, Object data) {
        StringBuilder sb = (StringBuilder) data;
        sb.append("false");
        node.childrenAccept(this, sb);
        return sb;
    }
    
    public Object visit(ASTStringLiteral node, Object data) {
        StringBuilder sb = (StringBuilder) data;
        
        String literal = node.image;
        int index = literal.indexOf(STRING_QUOTE);
        if (-1 != index) {
            // Slightly larger buffer
            int begin = 0;
            StringBuilder builder = new StringBuilder(literal.length() + 10);
            
            // Find every single quote and escape it
            while (-1 != index) {
                builder.append(literal.substring(begin, index));
                builder.append(BACKSLASH).append(STRING_QUOTE);
                begin = index + 1;
                literal.substring(index + 1, literal.length());
                index = literal.indexOf(STRING_QUOTE, begin);
            }
            
            // Tack on the end of the literal
            builder.append(literal.substring(begin));
            
            // Set the new version on the literal
            literal = builder.toString();
        }
        
        // Make sure we don't accidentally escape the closing quotation mark
        // e.g. FOO == 'C:\Foo\'
        if (literal.charAt(literal.length() - 1) == BACKSLASH) {
            StringBuilder builder = new StringBuilder(literal);
            
            // Nuke that last backslash
            builder.setLength(literal.length() - 1);
            
            // We need to ensure that a literal backslash makes it down to the tservers
            builder.append(BACKSLASH).append(BACKSLASH);
            
            literal = builder.toString();
        }
        
        sb.append(STRING_QUOTE).append(literal).append(STRING_QUOTE);
        node.childrenAccept(this, sb);
        return sb;
    }
    
    public Object visit(ASTFunctionNode node, Object data) {
        StringBuilder sb = (StringBuilder) data;
        
        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            if (1 == i) {
                sb.append(":");
            } else if (2 == i) {
                sb.append("(");
            } else if (2 < i) {
                sb.append(", ");
            }
            
            node.jjtGetChild(i).jjtAccept(this, sb);
        }
        
        sb.append(")");
        
        return sb;
    }
    
    @Override
    public Object visit(ASTMethodNode node, Object data) {
        StringBuilder sb = (StringBuilder) data;
        StringBuilder methodStringBuilder = new StringBuilder();
        StringBuilder argumentStringBuilder = new StringBuilder();
        int kidCount = node.jjtGetNumChildren();
        for (int i = 0; i < kidCount; i++) {
            if (i == 0) {
                JexlNode methodNode = node.jjtGetChild(i);
                methodStringBuilder.append(".");
                if (allowedMethods.contains(methodNode.image) == false) {
                    QueryException qe = new QueryException(DatawaveErrorCode.METHOD_COMPOSITION_ERROR, MessageFormat.format("{0}", methodNode.image));
                    throw new DatawaveFatalQueryException(qe);
                }
                methodStringBuilder.append(methodNode.image);
                methodStringBuilder.append("("); // parens are open. don't forget to close
            } else {
                // adding any method arguments
                JexlNode argumentNode = node.jjtGetChild(i);
                if (argumentNode instanceof ASTReference) {
                    // a method may have an argument that is another method. In this case, descend the visit tree for it
                    if (JexlASTHelper.HasMethodVisitor.hasMethod(argumentNode)) {
                        this.visit((ASTReference) argumentNode, argumentStringBuilder);
                    } else {
                        for (int j = 0; j < argumentNode.jjtGetNumChildren(); j++) {
                            JexlNode argKid = argumentNode.jjtGetChild(j);
                            if (argKid instanceof ASTFunctionNode) {
                                this.visit((ASTFunctionNode) argKid, argumentStringBuilder);
                            } else {
                                if (argumentStringBuilder.length() > 0) {
                                    argumentStringBuilder.append(",");
                                }
                                if (argKid instanceof ASTStringLiteral) {
                                    argumentStringBuilder.append("'");
                                }
                                argumentStringBuilder.append(argKid.image);
                                if (argKid instanceof ASTStringLiteral) {
                                    argumentStringBuilder.append("'");
                                }
                            }
                        }
                    }
                } else if (argumentNode instanceof ASTNumberLiteral) {
                    if (argumentStringBuilder.length() > 0) {
                        argumentStringBuilder.append(",");
                    }
                    argumentStringBuilder.append(argumentNode.image);
                }
            }
        }
        methodStringBuilder.append(argumentStringBuilder);
        methodStringBuilder.append(")"); // close parens in method
        sb.append(methodStringBuilder);
        return sb;
    }
    
    public Object visit(ASTNumberLiteral node, Object data) {
        StringBuilder sb = (StringBuilder) data;
        sb.append(node.image);
        node.childrenAccept(this, sb);
        return sb;
    }
    
    public Object visit(ASTReferenceExpression node, Object data) {
        StringBuilder sb = (StringBuilder) data;
        sb.append("(");
        int lastsize = sb.length();
        node.childrenAccept(this, sb);
        if (sb.length() == lastsize) {
            sb.setLength(sb.length() - 1);
        } else {
            sb.append(")");
        }
        return sb;
    }
    
    @Override
    public Object visit(ASTJexlScript node, Object data) {
        StringBuilder sb = (StringBuilder) data;
        for (int i = 0; i < node.jjtGetNumChildren(); ++i) {
            node.jjtGetChild(i).jjtAccept(this, sb);
            sb.append("; ");
        }
        // cutting of the last ';' is still legal jexl and lets me not have to modify
        // all the unit tests that don't expect a trailing ';'
        if (sb.length() > 0) {
            sb.setLength(sb.length() - "; ".length());
        }
        return sb;
    }
    
    @Override
    public Object visit(ASTAdditiveOperator node, Object data) {
        StringBuilder sb = (StringBuilder) data;
        sb.append(node.image);
        node.childrenAccept(this, sb);
        return sb;
    }
    
    @Override
    public Object visit(ASTAdditiveNode node, Object data) {
        StringBuilder sb = (StringBuilder) data;
        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            node.jjtGetChild(i).jjtAccept(this, sb);
        }
        
        return sb;
    }
    
    @Override
    public Object visit(ASTSizeMethod node, Object data) {
        StringBuilder sb = (StringBuilder) data;
        sb.append(".size() ");
        node.childrenAccept(this, sb);
        return sb;
    }
    
    @Override
    public Object visit(ASTMulNode node, Object data) {
        StringBuilder sb = (StringBuilder) data;
        int numChildren = node.jjtGetNumChildren();
        for (int i = 0; i < numChildren; i++) {
            node.jjtGetChild(i).jjtAccept(this, sb);
            if (i < numChildren - 1) {
                sb.append(" * ");
            }
        }
        
        return sb;
    }
    
    @Override
    public Object visit(ASTDivNode node, Object data) {
        StringBuilder sb = (StringBuilder) data;
        int numChildren = node.jjtGetNumChildren();
        for (int i = 0; i < numChildren; i++) {
            node.jjtGetChild(i).jjtAccept(this, sb);
            if (i < numChildren - 1) {
                sb.append(" / ");
            }
        }
        
        return sb;
    }
    
    @Override
    public Object visit(ASTModNode node, Object data) {
        StringBuilder sb = (StringBuilder) data;
        int numChildren = node.jjtGetNumChildren();
        for (int i = 0; i < numChildren; i++) {
            node.jjtGetChild(i).jjtAccept(this, sb);
            if (i < numChildren - 1) {
                sb.append(" % ");
            }
        }
        
        return sb;
    }
    
    @Override
    public Object visit(ASTAssignment node, Object data) {
        boolean requiresParens = !(node.jjtGetParent() instanceof ASTReferenceExpression);
        StringBuilder sb = (StringBuilder) data;
        if (requiresParens)
            sb.append('(');
        for (int i = 0; i < node.jjtGetNumChildren(); ++i) {
            node.jjtGetChild(i).jjtAccept(this, sb);
            sb.append(" = ");
        }
        sb.setLength(sb.length() - " = ".length());
        if (requiresParens) {
            sb.append(')');
        }
        return sb;
    }
}
