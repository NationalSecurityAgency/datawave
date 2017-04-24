package nsa.datawave.query.parser;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import javax.swing.tree.DefaultMutableTreeNode;

import nsa.datawave.query.parser.RangeCalculator.FatalRangeExpansionException;
import nsa.datawave.query.parser.RangeCalculator.RangeExpansionException;

import org.apache.accumulo.core.data.Range;
import org.apache.commons.jexl2.parser.ASTStringLiteral;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParserTreeConstants;
import org.apache.log4j.Logger;

/**
 * The DatawaveTreeNode is used to build a tree which represents the parsed query. By extending DefaultMutableTreeNode, we inherit tree traversal methods. This
 * is a generic node in the sense that it can represent different types of logical nodes.
 */
@Deprecated
public class DatawaveTreeNode extends DefaultMutableTreeNode {
    private static final long serialVersionUID = 1L;
    
    protected static final Logger log = Logger.getLogger(DatawaveTreeNode.class);
    private boolean negated = false;
    private String fieldName;
    private String fieldValue;
    private Class<?> fieldValueLiteralType = null;
    private String operator;
    private int type;
    private boolean removal = false;
    private boolean childrenAllNegated = false;
    private boolean optimized = true;
    private boolean functionNode = false;
    private String functionNamespace;
    private String functionName;
    private List<JexlNode> functionArgs;
    private Class<?> functionClass;
    private long cardinality;
    private Set<Range> ranges;
    private boolean infiniteRange = false;
    private boolean containsRangeNodes = false;
    private boolean rangeNode = false;
    private String rangeLowerOp;
    private String rangeUpperOp;
    private String upperBound;
    private String lowerBound;
    private boolean completeRange;
    private boolean reverseIndex = false;
    private String script = "";
    public static final Pattern jexlInvalidChars = Pattern.compile(".*[^a-zA-Z0-9_$].*"); // anything not letter, number, underscore, or dollarsign (we use $ in
                                                                                          // multivalue fields)
    public static final Pattern jexlInvalidStartingChars = Pattern.compile("^[0-9].*");
    private RangeExpansionException rcee = null;
    private boolean specialHandling = false;
    private boolean regex = false;
    private boolean unboundedRange = false;
    private boolean indexOnlyField = false;
    
    public DatawaveTreeNode() {}
    
    /**
     * Basic constructor for DatawaveTreeNode. You must specify what type it is, i.e. AND/OR/EQNode, etc. if you do not know what type it is you can pass in the
     * JEXL JJTAMBIGUOUS value.
     *
     * @param type
     *            - An int which maps to the JEXL ParserTreeConstants node type.
     */
    public DatawaveTreeNode(int type) {
        this(type, null, null, false);
    }
    
    /**
     * Constructor for DatawaveTreeNode. You must specify what type it is, i.e. AND/OR/EQNode, etc. if you do not know what type it is you can pass in the JEXL
     * JJTAMBIGUOUS value.
     *
     * @param type
     *            - An int which maps to the JEXL ParserTreeConstants node type.
     * @param fName
     *            The Field Name
     * @param fValue
     *            The Field Value
     */
    public DatawaveTreeNode(int type, String fName, String fValue) {
        this(type, fName, fValue, false);
    }
    
    /**
     * Constructor for DatawaveTreeNode. You must specify what type it is, i.e. AND/OR/EQNode, etc. if you do not know what type it is you can pass in the JEXL
     * JJTAMBIGUOUS value.
     *
     * @param type
     *            - An int which maps to the JEXL ParserTreeConstants node type.
     * @param fName
     *            The Field Name
     * @param fValue
     *            The Field Value
     * @param negate
     *            Boolean stating if this node is negated or not.
     */
    public DatawaveTreeNode(int type, String fName, String fValue, boolean negate) {
        this(type, fName, fValue, negate, null);
    }
    
    /**
     * Constructor for DatawaveTreeNode. You must specify what type it is, i.e. AND/OR/EQNode, etc. if you do not know what type it is you can pass in the JEXL
     * JJTAMBIGUOUS value.
     *
     * @param type
     *            - An int which maps to the JEXL ParserTreeConstants node type.
     * @param fName
     *            The Field Name
     * @param fValue
     *            The Field Value
     * @param negate
     *            Boolean stating if this node is negated
     * @param literalTypeClass
     *            The JEXL Literal type (String/Integer/Float/Null, etc.) of the Field Value.
     */
    public DatawaveTreeNode(int type, String fName, String fValue, boolean negate, Class<?> literalTypeClass) {
        super();
        this.type = type;
        this.fieldName = fName;
        this.fieldValue = fValue;
        this.negated = negate;
        this.operator = JexlOperatorConstants.getOperator(type);
        if (log.isDebugEnabled()) {
            log.debug("FN: " + this.fieldName + "  FV: " + this.fieldValue + " Op: " + this.operator);
        }
        this.fieldValueLiteralType = literalTypeClass;
    }
    
    /**
     * Get the node's FieldName parameter.
     *
     * @return String, Field Name, i.e. COLOR == 'red', returns COLOR
     */
    public String getFieldName() {
        return fieldName;
    }
    
    /**
     * Set this node's FieldName parameter
     *
     * @param fieldName
     *            The node's Field Name
     */
    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }
    
    /**
     * Get the node's field value parameter
     *
     * @return String, Field Value, i.e. COLOR == 'red', returns red
     */
    public String getFieldValue() {
        return fieldValue;
    }
    
    /**
     * Set the node's field value parameter
     *
     * @param fieldValue
     *            The node's field value
     */
    public void setFieldValue(String fieldValue) {
        this.fieldValue = fieldValue;
    }
    
    /**
     *
     * @return boolean stating if this node is negated or not.
     */
    public boolean isNegated() {
        return negated;
    }
    
    /**
     * Set whether this node is negated
     *
     * @param negated
     *            Boolean flag to set this node's negation.
     */
    public void setNegated(boolean negated) {
        this.negated = negated;
    }
    
    /**
     * Get this node's operator.
     *
     * @return String of the JEXL operator for this node, i.e. {@code == , != , <=, etc}.
     */
    public String getOperator() {
        return operator;
    }
    
    /**
     * Set this nodes's JEXL operator string.
     *
     * @param operator
     */
    public void setOperator(String operator) {
        this.operator = operator;
    }
    
    /**
     * Get this node's type based on JEXL ParserTreeConstants mapping.
     *
     * @return Retrieve this node's type, based on JEXL ParserTreeConstants mapping.
     */
    public int getType() {
        return type;
    }
    
    /**
     * Set this node's type based on JEXL ParserTreeConstants mapping.
     *
     * @param type
     *            int corresponding to the JEXL ParserTreeConstants mapping of this node's type
     */
    public void setType(int type) {
        this.type = type;
    }
    
    public long getCardinality() {
        return cardinality;
    }
    
    public void setCardinality(long cardinality) {
        this.cardinality = cardinality;
    }
    
    public boolean isRemoval() {
        return removal;
    }
    
    public void setRemoval(boolean removal) {
        this.removal = removal;
    }
    
    /**
     *
     * @return Boolean telling you if all of this node's children are negated. Useful for analyzing AND/OR nodes without having to check all of their children.
     */
    public boolean isChildrenAllNegated() {
        return childrenAllNegated;
    }
    
    /**
     *
     * @param childrenAllNegated
     *            set true if all children of this node are negated.
     */
    public void setChildrenAllNegated(boolean childrenAllNegated) {
        this.childrenAllNegated = childrenAllNegated;
    }
    
    public boolean isOptimized() {
        return optimized;
    }
    
    public void setOptimized(boolean isOptimized) {
        this.optimized = isOptimized;
    }
    
    public Class<?> getFieldValueLiteralType() {
        return fieldValueLiteralType;
    }
    
    public void setFieldValueLiteralType(Class<?> fieldValueLiteralType) {
        this.fieldValueLiteralType = fieldValueLiteralType;
    }
    
    public Set<Range> getRanges() {
        return ranges;
    }
    
    public void setRanges(Set<Range> ranges) {
        this.ranges = ranges;
    }
    
    public boolean isUnboundedRange() {
        return unboundedRange;
    }
    
    public void setUnboundedRange(boolean unboundedRange) {
        this.unboundedRange = unboundedRange;
    }
    
    public boolean isIndexOnlyField() {
        return indexOnlyField;
    }
    
    public void setIndexOnlyField(boolean indexOnlyField) {
        this.indexOnlyField = indexOnlyField;
    }
    
    /**
     *
     * @return String of the Node and its children.
     */
    public String getContents() {
        StringBuilder s = new StringBuilder("[");
        s.append(toString());
        
        if (children != null) {
            Enumeration<?> e = this.children();
            while (e.hasMoreElements()) {
                DatawaveTreeNode n = (DatawaveTreeNode) e.nextElement();
                s.append(",");
                s.append(n.getContents());
            }
        }
        s.append("]");
        return s.toString();
    }
    
    /**
     *
     * @return String of node internals, no children, use getContents instead.
     */
    public String printNode() {
        StringBuilder s = new StringBuilder("[");
        s.append("Full Location & Term = ");
        if (this.fieldName != null) {
            s.append(this.fieldName);
        } else {
            s.append("BlankDataLocation");
        }
        s.append("  ");
        if (this.fieldValue != null) {
            s.append(this.fieldValue);
        } else {
            s.append("BlankTerm");
        }
        s.append("]");
        return s.toString();
    }
    
    @Override
    public String toString() {
        if (this.isRangeNode()) {
            return this.printRangeNode();
        }
        switch (type) {
            case ParserTreeConstants.JJTEQNODE:
            case ParserTreeConstants.JJTNENODE:
            case ParserTreeConstants.JJTERNODE:
            case ParserTreeConstants.JJTNRNODE:
            case ParserTreeConstants.JJTLENODE:
            case ParserTreeConstants.JJTLTNODE:
            case ParserTreeConstants.JJTGENODE:
            case ParserTreeConstants.JJTGTNODE:
                return fieldName + ":" + fieldValue + ":negated=" + isNegated();
            case ParserTreeConstants.JJTAMBIGUOUS:
                return fieldName + ":" + fieldValue + ":negated=" + isNegated() + " AMBIGUOUS";
            case ParserTreeConstants.JJTJEXLSCRIPT:
                return "HEAD";
            case ParserTreeConstants.JJTANDNODE:
                return "AND childrenAllNegated=" + isChildrenAllNegated();
            case ParserTreeConstants.JJTNOTNODE:
                return "NOT";
            case ParserTreeConstants.JJTORNODE:
                return "OR childrenAllNegated=" + isChildrenAllNegated();
            case ParserTreeConstants.JJTFUNCTIONNODE:
                return "FUNCTION: " + this.functionNamespace + ":" + this.functionName + "  negated=" + isNegated();
            default:
                log.warn("Problem in DatawaveTreeNode.toString()");
                return null;
        }
    }
    
    public List<JexlNode> getFunctionArgs() {
        return functionArgs;
    }
    
    public List<String> getStringFunctionArgs() {
        ArrayList<String> strArgs = new ArrayList<>();
        
        StringBuilder sb = new StringBuilder();
        String quotes;
        
        for (JexlNode jex : this.getFunctionArgs()) {
            if (jex.getClass().equals(ASTStringLiteral.class)) {
                quotes = "'";
            } else {
                quotes = "";
            }
            sb.append(quotes).append(jex.jjtGetValue()).append(quotes);
            
            strArgs.add(sb.toString());
            
            sb.setLength(0);
        }
        
        return strArgs;
    }
    
    public void setFunctionArgs(List<JexlNode> functionArgs) {
        this.functionArgs = functionArgs;
    }
    
    public String getFunctionName() {
        return functionName;
    }
    
    public void setFunctionName(String functionName) {
        this.functionName = functionName;
    }
    
    public String getFunctionNamespace() {
        return functionNamespace;
    }
    
    public void setFunctionNamespace(String functionNamespace) {
        this.functionNamespace = functionNamespace;
    }
    
    public boolean isFunctionNode() {
        return functionNode;
    }
    
    public void setFunctionNode(boolean functionNode) {
        this.functionNode = functionNode;
    }
    
    public void setFunctionClass(Class<?> clas) {
        this.functionClass = clas;
    }
    
    public Class<?> getFunctionClass() {
        return this.functionClass;
    }
    
    public boolean isInfiniteRange() {
        return infiniteRange;
    }
    
    public void setInfiniteRange(boolean infiniteRange) {
        this.infiniteRange = infiniteRange;
    }
    
    public String getOriginalQueryString() {
        StringBuilder sb = new StringBuilder();
        if (isFunctionNode()) {
            sb.append(functionNamespace).append(":").append(functionName).append("(");
            String quotes;
            for (JexlNode jex : this.getFunctionArgs()) {
                if (jex.getClass().equals(ASTStringLiteral.class)) {
                    quotes = "'";
                } else {
                    quotes = "";
                }
                sb.append(quotes).append(jex.jjtGetValue()).append(quotes);
                sb.append(",");
            }
            sb.deleteCharAt(sb.length() - 1);
            sb.append(")");
            return sb.toString();
        } else {
            String quotes = " ";
            if (this.getFieldValueLiteralType() != null && this.getFieldValueLiteralType().equals(ASTStringLiteral.class)) {
                quotes = "'";
            }
            sb.append(fieldName).append(" ").append(operator).append(" ").append(quotes).append(fieldValue).append(quotes);
            return sb.toString();
        }
    }
    
    public String getPredicate() {
        StringBuilder sb = new StringBuilder();
        if (isFunctionNode()) {
            sb.append(functionNamespace).append(":").append(functionName).append("(");
            String quotes;
            for (JexlNode jex : this.getFunctionArgs()) {
                if (jex.getClass().equals(ASTStringLiteral.class)) {
                    quotes = "'";
                } else {
                    quotes = "";
                }
                sb.append(quotes).append(jex.jjtGetValue()).append(quotes);
                sb.append(",");
            }
            sb.deleteCharAt(sb.length() - 1);
            sb.append(")");
            return sb.toString();
        } else {
            String quotes = "";
            if (this.getFieldValueLiteralType().equals(ASTStringLiteral.class)) {
                quotes = "'";
            }
            sb.append(quotes).append(fieldValue).append(quotes);
        }
        return sb.toString();
    }
    
    public boolean containsRangeNodes() {
        return containsRangeNodes;
    }
    
    public void setContainsRangeNodes(boolean containsRangeNodes) {
        this.containsRangeNodes = containsRangeNodes;
    }
    
    public boolean isRangeNode() {
        return this.rangeNode;
    }
    
    public void setRangeNode(boolean val) {
        this.rangeNode = val;
    }
    
    public String getLowerBound() {
        return lowerBound;
    }
    
    public void setLowerBound(String lowerBound) {
        this.lowerBound = lowerBound;
    }
    
    public String getRangeLowerOp() {
        return rangeLowerOp;
    }
    
    public boolean getRangeLowerInclusive() {
        return rangeLowerOp.indexOf('=') >= 0;
    }
    
    public void setRangeLowerOp(String rangeLowerOp) {
        this.rangeLowerOp = rangeLowerOp;
    }
    
    public String getRangeUpperOp() {
        return rangeUpperOp;
    }
    
    public boolean getRangeUpperInclusive() {
        return rangeUpperOp.indexOf('=') >= 0;
    }
    
    public void setRangeUpperOp(String rangeUpperOp) {
        this.rangeUpperOp = rangeUpperOp;
    }
    
    public String getUpperBound() {
        return upperBound;
    }
    
    public void setUpperBound(String upperBound) {
        this.upperBound = upperBound;
    }
    
    public boolean isCompleteRange() {
        return completeRange;
    }
    
    public void setCompleteRange(boolean completeRange) {
        this.completeRange = completeRange;
    }
    
    public boolean isReverseIndex() {
        return reverseIndex;
    }
    
    public void setReverseIndex(boolean reverseIndex) {
        this.reverseIndex = reverseIndex;
    }
    
    public String printRangeNode() {
        return "RangeNode: " + this.getFieldName() + " " + this.getRangeLowerOp() + " " + this.getLowerBound() + " && " + this.getFieldName() + " "
                        + this.getRangeUpperOp() + " " + this.getUpperBound() + " type: " + this.getType();
    }
    
    public String getScript() {
        return script;
    }
    
    public void setScript(String script) {
        this.script = script;
    }
    
    /**
     * If the field name contains characters jexl can't parse, quote it else don't. Also mark the node as needing special handling.
     *
     * @return
     */
    public String getFieldNameSmartQuote() {
        StringBuilder b = new StringBuilder();
        String quote = "";
        if (jexlInvalidChars.matcher(this.fieldName).matches() || jexlInvalidStartingChars.matcher(this.fieldName).matches()) {
            this.specialHandling = true;
            quote = "'";
        }
        b.append(quote).append(this.getFieldName()).append(quote);
        log.debug("getFieldNameSmartQuote: " + b.toString());
        return b.toString();
    }
    
    public void setRangeExpansionException(RangeExpansionException rcee) {
        this.rcee = rcee;
    }
    
    public boolean hasRangeExpansionException() {
        return null != rcee;
    }
    
    public boolean hasFatalRangeExpansionException() {
        return rcee != null && (rcee instanceof FatalRangeExpansionException);
    }
    
    public RangeExpansionException getRangeExpansionException() {
        return this.rcee;
    }
    
    /**
     * Added to fix issues with fieldNames starting with Numeric characters
     *
     * @return
     */
    public boolean isSpecialHandling() {
        return this.specialHandling;
    }
    
    /**
     * Added to fix issues with fieldNames starting with Numeric characters
     *
     * @param b
     */
    public void setSpecialHandling(boolean b) {
        this.specialHandling = b;
    }
    
    public void regex(boolean b) {
        regex = b;
    }
    
    public boolean regex() {
        return regex;
    }
}
