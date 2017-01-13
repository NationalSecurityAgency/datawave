package nsa.datawave.query.parser;

import java.io.StringReader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import nsa.datawave.query.jexl.DatawaveArithmetic;
import nsa.datawave.query.jexl.DatawaveJexlEngine;
import nsa.datawave.webservice.common.logging.ThreadConfigurableLogger;

import org.apache.commons.jexl2.JexlEngine;
import org.apache.commons.jexl2.parser.ASTAdditiveNode;
import org.apache.commons.jexl2.parser.ASTAdditiveOperator;
import org.apache.commons.jexl2.parser.ASTAmbiguous;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTArrayAccess;
import org.apache.commons.jexl2.parser.ASTArrayLiteral;
import org.apache.commons.jexl2.parser.ASTAssignment;
import org.apache.commons.jexl2.parser.ASTBitwiseAndNode;
import org.apache.commons.jexl2.parser.ASTBitwiseComplNode;
import org.apache.commons.jexl2.parser.ASTBitwiseOrNode;
import org.apache.commons.jexl2.parser.ASTBitwiseXorNode;
import org.apache.commons.jexl2.parser.ASTBlock;
import org.apache.commons.jexl2.parser.ASTConstructorNode;
import org.apache.commons.jexl2.parser.ASTDivNode;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTEmptyFunction;
import org.apache.commons.jexl2.parser.ASTFalseNode;
import org.apache.commons.jexl2.parser.ASTForeachStatement;
import org.apache.commons.jexl2.parser.ASTFunctionNode;
import org.apache.commons.jexl2.parser.ASTGENode;
import org.apache.commons.jexl2.parser.ASTGTNode;
import org.apache.commons.jexl2.parser.ASTIdentifier;
import org.apache.commons.jexl2.parser.ASTIfStatement;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ASTLENode;
import org.apache.commons.jexl2.parser.ASTLTNode;
import org.apache.commons.jexl2.parser.ASTMapEntry;
import org.apache.commons.jexl2.parser.ASTMapLiteral;
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
import org.apache.commons.jexl2.parser.ASTReturnStatement;
import org.apache.commons.jexl2.parser.ASTSizeFunction;
import org.apache.commons.jexl2.parser.ASTSizeMethod;
import org.apache.commons.jexl2.parser.ASTStringLiteral;
import org.apache.commons.jexl2.parser.ASTTernaryNode;
import org.apache.commons.jexl2.parser.ASTTrueNode;
import org.apache.commons.jexl2.parser.ASTUnaryMinusNode;
import org.apache.commons.jexl2.parser.ASTVar;
import org.apache.commons.jexl2.parser.ASTWhileStatement;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParseException;
import org.apache.commons.jexl2.parser.Parser;
import org.apache.commons.jexl2.parser.ParserTreeConstants;
import org.apache.commons.jexl2.parser.ParserVisitor;
import org.apache.commons.jexl2.parser.SimpleNode;
import org.apache.log4j.Logger;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

/**
 *
 * 
 */
@Deprecated
public class DatawaveQueryParser implements ParserVisitor {
    public static final Set<Class<?>> REAL_NUMBERS = java.util.Collections.unmodifiableSet(Sets.<Class<?>> newHashSet(BigDecimal.class, Double.class,
                    Float.class));
    public static final Set<Class<?>> NATURAL_NUMBERS = java.util.Collections.unmodifiableSet(Sets.<Class<?>> newHashSet(Long.class, BigInteger.class,
                    Integer.class));
    
    protected static final Logger log = ThreadConfigurableLogger.getLogger(DatawaveQueryParser.class);
    protected Set<String> unFieldedValues = new HashSet<>();
    private JexlEngine engine;
    private Multimap<String,DatawaveTreeNode> queryTermMap;
    Parser parser;
    private static final String NORMALIZER_NAMESPACE = "normalize";
    
    /***************************************************************************
     * Constructors
     */
    public DatawaveQueryParser() {
        parser = new Parser(new StringReader(";"));
        engine = new DatawaveJexlEngine(null, new DatawaveArithmetic(false), null, null); // Use this to be able to get the functions.
    }
    
    public DatawaveTreeNode parseQuery(String query) throws ParseException {
        queryTermMap = HashMultimap.create();
        query = query.replaceAll("\\s+[Aa][Nn][Dd]\\s+", " and ");
        query = query.replaceAll("\\s+[Oo][Rr]\\s+", " or ");
        query = query.replaceAll("\\s+[Nn][Oo][Tt]\\s+", " not ");
        try {
            ASTJexlScript script = parser.parse(new StringReader(query), null);
            DatawaveTreeNode rootNode = new DatawaveTreeNode(ParserTreeConstants.JJTJEXLSCRIPT);
            script.jjtAccept(this, rootNode);
            return rootNode;
        } catch (ParseException e) {
            log.error("Could not parse the given query: " + query);
            throw e;
        }
    }
    
    public Multimap<String,DatawaveTreeNode> getQueryTermMap() {
        return this.queryTermMap;
    }
    
    public Set<String> getFieldNames() {
        return this.queryTermMap.keySet();
    }
    
    @Override
    public Object visit(SimpleNode sn, Object o) {
        log.trace("SimpleNode");
        return null;
    }
    
    /*
     * JexlScript Node (HEAD node) Currently 4 cases, child is AND, OR, or single Node, or function returns DatawaveTreeNode
     */
    @Override
    public Object visit(ASTJexlScript jnode, Object o) {
        log.trace("ASTJexlScript");
        DatawaveTreeNode rootNode = (DatawaveTreeNode) o;
        DatawaveTreeNode n = (DatawaveTreeNode) jnode.jjtGetChild(0).jjtAccept(this, o);
        rootNode.add(n);
        return null;
    }
    
    /*
     * This is dirty, sometimes it returns JexlNodes and other times it returns DatawaveTreeNodes.
     */
    @Override
    public Object visit(ASTReference node, Object o) {
        if (log.isTraceEnabled()) {
            log.trace("ASTReference: " + node.image);
        }
        Class<? extends JexlNode> pClass = node.jjtGetParent().getClass();
        if ((pClass.equals(ASTAndNode.class) || pClass.equals(ASTOrNode.class) || pClass.equals(ASTNotNode.class) || pClass.equals(ASTJexlScript.class))) {
            if (log.isTraceEnabled()) {
                log.trace("ASTReference node suspected to be unfielded, child: " + node.jjtGetChild(0).image + "  class: " + node.jjtGetChild(0).getClass());
            }
            Object n = node.jjtGetChild(0).jjtAccept(this, node);
            if (n instanceof JexlNode) {
                JexlNode jex = (JexlNode) n;
                if (jex.jjtGetValue().getClass().equals(DatawaveTreeNode.class)) {
                    return jex.jjtGetValue();
                }
                if (log.isTraceEnabled()) {
                    log.trace("ASTReference childrenAccept: " + jex);
                }
                this.unFieldedValues.add(node.jjtGetChild(0).image);
                DatawaveTreeNode tnode = new DatawaveTreeNode(ParserTreeConstants.JJTAMBIGUOUS, "", jex.jjtGetValue().toString());
                tnode.setFieldValueLiteralType(jex.getClass());
                return tnode;
            } else if (n instanceof DatawaveTreeNode) {
                return n;
            }
        }
        
        return node.jjtGetChild(0).jjtAccept(this, node);
    }
    
    /*
     * returns JexlNode
     */
    @Override
    public Object visit(ASTIdentifier node, Object o) {
        if (log.isTraceEnabled()) {
            log.trace("ASTIdentifier: " + node.image + "  childcount: " + node.jjtGetNumChildren());
        }
        node.jjtSetValue(node.image);
        return node;
    }
    
    /*
     * OR nodes returns a DatawaveTreeNode
     */
    
    @Override
    public Object visit(ASTOrNode jnode, Object o) {
        if (log.isTraceEnabled()) {
            log.trace("ASTOrNode");
            log.trace("\tASTOrNode, left  side: " + jnode.jjtGetChild(0).getClass());
            log.trace("\tASTOrNode, right side: " + jnode.jjtGetChild(1).getClass());
        }
        DatawaveTreeNode node = new DatawaveTreeNode(ParserTreeConstants.JJTORNODE);
        log.trace("\tbegin processing children");
        for (int i = 0; i < jnode.jjtGetNumChildren(); i++) {
            // for each child process it and add it
            DatawaveTreeNode child = (DatawaveTreeNode) jnode.jjtGetChild(i).jjtAccept(this, jnode);
            if (child.getType() == ParserTreeConstants.JJTORNODE) {
                // steal its children
                @SuppressWarnings("unchecked")
                Enumeration<DatawaveTreeNode> children = child.children();
                if (log.isTraceEnabled()) {
                    log.trace("\t this or node has enum.size: " + child.getChildCount());
                }
                while (children.hasMoreElements()) {
                    // you need to pull it off the back, enum is not a defensive copy!
                    DatawaveTreeNode gchild = (DatawaveTreeNode) child.getLastChild();
                    if (log.isTraceEnabled()) {
                        log.trace("OR added grand child: " + gchild.getContents());
                    }
                    node.add(gchild);
                }
            } else {
                if (log.isTraceEnabled()) {
                    log.trace("OR added child: " + child.getContents());
                }
                node.add(child);
            }
        }
        log.debug("returning node: " + node.getContents());
        return node;
    }
    
    /*
     * AND nodes returns a DatawaveTreeNode
     */
    @Override
    public Object visit(ASTAndNode jnode, Object o) {
        if (log.isTraceEnabled()) {
            log.trace("ASTAndNode, children: " + jnode.jjtGetNumChildren());
            log.trace("\tASTAndNode, left  side: " + jnode.jjtGetChild(0).getClass());
            log.trace("\tASTAndNode, right side: " + jnode.jjtGetChild(1).getClass());
        }
        
        DatawaveTreeNode node = new DatawaveTreeNode(ParserTreeConstants.JJTANDNODE);
        log.trace("\tbegin processing children");
        for (int i = 0; i < jnode.jjtGetNumChildren(); i++) {
            // for each child process it and add it
            DatawaveTreeNode child = (DatawaveTreeNode) jnode.jjtGetChild(i).jjtAccept(this, jnode);
            if (child.getType() == ParserTreeConstants.JJTANDNODE) {
                // steal its children
                @SuppressWarnings("unchecked")
                Enumeration<DatawaveTreeNode> children = child.children();
                if (log.isTraceEnabled()) {
                    log.trace("\t this and node has enum.size: " + child.getChildCount());
                }
                while (children.hasMoreElements()) {
                    // you need to pull it off the back, enum is not a defensive copy!
                    DatawaveTreeNode gchild = (DatawaveTreeNode) child.getLastChild();
                    if (log.isTraceEnabled()) {
                        log.trace("AND added grand child: " + gchild.getContents());
                    }
                    node.add(gchild);
                }
            } else {
                if (log.isTraceEnabled()) {
                    log.trace("AND added child: " + child.getContents());
                }
                node.add(child);
            }
        }
        if (log.isTraceEnabled()) {
            log.trace("returning node: " + node.getContents());
        }
        return node;
    }
    
    @Override
    public Object visit(ASTNotNode jnode, Object o) {
        log.trace("ASTNotNode");
        DatawaveTreeNode node = new DatawaveTreeNode(ParserTreeConstants.JJTNOTNODE);
        for (int i = 0; i < jnode.jjtGetNumChildren(); i++) {
            Object obj = jnode.jjtGetChild(i).jjtAccept(this, jnode);
            if (log.isTraceEnabled()) {
                log.trace("ASTNotNode, object class: " + obj.getClass());
            }
            if (obj.getClass().equals(ASTStringLiteral.class)) {
                // check for function node
                ASTStringLiteral lit = (ASTStringLiteral) obj;
                if (log.isTraceEnabled()) {
                    log.trace("ASTNotNode, lit.jjtGetValue.getclass: " + lit.jjtGetValue().getClass());
                }
                if (lit.jjtGetValue().getClass().equals(DatawaveTreeNode.class)) {
                    DatawaveTreeNode child = (DatawaveTreeNode) lit.jjtGetValue();
                    if (log.isTraceEnabled()) {
                        log.trace("NOT CHILD: " + child.getContents());
                    }
                    node.add(child);
                }
            } else {
                DatawaveTreeNode child = (DatawaveTreeNode) obj;
                node.add(child);
            }
        }
        if (log.isTraceEnabled()) {
            log.trace("ASTNotNode, treeNode: " + node.getContents());
        }
        return node;
    }
    
    @Override
    public Object visit(ASTReferenceExpression node, Object o) {
        log.debug("ASTReferenceExpression: " + node.image);
        Class<? extends JexlNode> pClass = node.jjtGetParent().getClass();
        if ((pClass.equals(ASTAndNode.class) || pClass.equals(ASTOrNode.class) || pClass.equals(ASTJexlScript.class))) {
            if (log.isDebugEnabled()) {
                log.debug("ASTReferenceExpression node suspected to be unfielded, child: " + node.jjtGetChild(0).image + "  class: "
                                + node.jjtGetChild(0).getClass());
            }
            Object n = node.jjtGetChild(0).jjtAccept(this, node);
            if (n instanceof JexlNode) {
                JexlNode jex = (JexlNode) n;
                if (jex.jjtGetValue().getClass().equals(DatawaveTreeNode.class)) {
                    return jex.jjtGetValue();
                }
                
                log.debug("ASTReferenceExpression childrenAccept: " + jex);
                this.unFieldedValues.add(node.jjtGetChild(0).image);
                DatawaveTreeNode tnode = new DatawaveTreeNode(ParserTreeConstants.JJTAMBIGUOUS, "", jex.jjtGetValue().toString());
                tnode.setFieldValueLiteralType(jex.getClass());
                return tnode;
            } else if (o instanceof DatawaveTreeNode) {
                return o;
            }
        }
        
        return node.jjtGetChild(0).jjtAccept(this, node);
    }
    
    /*
     * returns JexlNode
     */
    @Override
    public Object visit(ASTReturnStatement node, Object o) {
        log.debug("ASTReturnStatement: " + node.image + "  childcount: " + node.jjtGetNumChildren());
        node.jjtSetValue(node.image);
        return node;
    }
    
    /*
     * returns JexlNode
     */
    @Override
    public Object visit(ASTVar node, Object o) {
        log.debug("ASTVar: " + node.image + "  childcount: " + node.jjtGetNumChildren());
        node.jjtSetValue(node.image);
        return node;
    }
    
    protected void processLeft(DatawaveTreeNode node, JexlNode child) {
        // We have mandated that it must be FIELDNAME == FIELDVALUE
        
        // left
        if (child.getClass().equals(ASTIdentifier.class)) {
            // field Name;
            node.setFieldName(((ASTIdentifier) child).image);
            node.setFieldValueLiteralType(ASTStringLiteral.class);
        } else if (child.getClass().equals(ASTStringLiteral.class) || child.getClass().equals(ASTNumberLiteral.class)
                        || child.getClass().equals(ASTNullLiteral.class)) {
            
            node.setFieldName(child.jjtGetValue().toString());
            
        } else {
            log.warn("Node child not an Identifier or Literal");
        }
        
    }
    
    protected void processRight(DatawaveTreeNode node, JexlNode child) {
        // right
        
        node.setFieldValue(child.jjtGetValue() == null ? null : child.jjtGetValue().toString());
        node.setFieldValueLiteralType(child.getClass());
        
        if ((node.getFieldName() != null && !node.getFieldName().isEmpty())
                        && (node.getFieldValue() != null && !node.getFieldValue().isEmpty() || (node.getFieldValue() == null
                                        && node.getFieldValueLiteralType() != null && node.getFieldValueLiteralType().equals(ASTNullLiteral.class)))) {
            
            queryTermMap.put(node.getFieldName(), node);
        }
    }
    
    /*
     * == node returns a DatawaveTreeNode
     */
    @Override
    public Object visit(ASTEQNode jnode, Object o) {
        if (log.isTraceEnabled()) {
            log.trace("ASTEQNode");
            log.trace("parent: " + jnode.jjtGetParent());
            log.trace("object: " + o);
        }
        DatawaveTreeNode node = new DatawaveTreeNode(ParserTreeConstants.JJTEQNODE);
        node.setOperator(JexlOperatorConstants.getOperator(ASTEQNode.class));
        
        // this is xxx == yyy, need to parse left and right sides
        // We have mandated that it must be FIELDNAME == FIELDVALUE
        JexlNode child = (JexlNode) jnode.jjtGetChild(0).jjtAccept(this, node);
        processLeft(node, child);
        
        // right
        child = (JexlNode) jnode.jjtGetChild(1).jjtAccept(this, node);
        processRight(node, child);
        
        if ((node.getFieldName() != null && !node.getFieldName().isEmpty())
                        && (node.getFieldValue() != null && !node.getFieldValue().isEmpty() || (node.getFieldValue() == null
                                        && node.getFieldValueLiteralType() != null && node.getFieldValueLiteralType().equals(ASTNullLiteral.class)))) {
            
            queryTermMap.put(node.getFieldName(), node);
        }
        
        log.debug(node.getContents());
        
        return node;
    }
    
    /*
     * != node returns a DatawaveTreeNode
     */
    @Override
    public Object visit(ASTNENode jnode, Object o) {
        log.trace("ASTNENode");
        
        DatawaveTreeNode node = new DatawaveTreeNode(ParserTreeConstants.JJTEQNODE);
        node.setOperator(JexlOperatorConstants.getOperator(ASTEQNode.class));
        node.setNegated(true);
        
        // this is xxx != yyy, need to parse left and right sides
        // We have mandated that it must be FIELDNAME == FIELDVALUE
        JexlNode child = (JexlNode) jnode.jjtGetChild(0).jjtAccept(this, node);
        processLeft(node, child);
        
        // right
        child = (JexlNode) jnode.jjtGetChild(1).jjtAccept(this, node);
        processRight(node, child);
        
        if ((node.getFieldName() != null && !node.getFieldName().isEmpty())
                        && (node.getFieldValue() != null && !node.getFieldValue().isEmpty() || (node.getFieldValue() == null
                                        && node.getFieldValueLiteralType() != null && node.getFieldValueLiteralType().equals(ASTNullLiteral.class)))) {
            
            queryTermMap.put(node.getFieldName(), node);
        }
        
        log.debug(node.getContents());
        return node;
    }
    
    @Override
    public Object visit(ASTLTNode jnode, Object o) {
        if (log.isTraceEnabled()) {
            log.trace("ASTLTNode");
            log.trace("parent: " + jnode.jjtGetParent());
            log.trace("object: " + o);
        }
        DatawaveTreeNode node = new DatawaveTreeNode(ParserTreeConstants.JJTLTNODE);
        node.setOperator(JexlOperatorConstants.getOperator(ASTLTNode.class));
        
        // left
        JexlNode child = (JexlNode) jnode.jjtGetChild(0).jjtAccept(this, node);
        processLeft(node, child);
        
        // right
        child = (JexlNode) jnode.jjtGetChild(1).jjtAccept(this, node);
        processRight(node, child);
        
        if ((node.getFieldName() != null && !node.getFieldName().isEmpty())
                        && (node.getFieldValue() != null && !node.getFieldValue().isEmpty() || (node.getFieldValue() == null
                                        && node.getFieldValueLiteralType() != null && node.getFieldValueLiteralType().equals(ASTNullLiteral.class)))) {
            
            queryTermMap.put(node.getFieldName(), node);
        }
        
        log.debug(node.getContents());
        return node;
    }
    
    @Override
    public Object visit(ASTGTNode jnode, Object o) {
        if (log.isTraceEnabled()) {
            log.trace("ASTGTNode");
            log.trace("parent: " + jnode.jjtGetParent());
            log.trace("object: " + o);
        }
        DatawaveTreeNode node = new DatawaveTreeNode(ParserTreeConstants.JJTGTNODE);
        node.setOperator(JexlOperatorConstants.getOperator(ASTGTNode.class));
        
        // left
        JexlNode child = (JexlNode) jnode.jjtGetChild(0).jjtAccept(this, node);
        processLeft(node, child);
        
        // right
        child = (JexlNode) jnode.jjtGetChild(1).jjtAccept(this, node);
        processRight(node, child);
        
        if ((node.getFieldName() != null && !node.getFieldName().isEmpty())
                        && (node.getFieldValue() != null && !node.getFieldValue().isEmpty() || (node.getFieldValue() == null
                                        && node.getFieldValueLiteralType() != null && node.getFieldValueLiteralType().equals(ASTNullLiteral.class)))) {
            
            queryTermMap.put(node.getFieldName(), node);
        }
        
        log.debug(node.getContents());
        return node;
    }
    
    @Override
    public Object visit(ASTLENode jnode, Object o) {
        if (log.isTraceEnabled()) {
            log.trace("ASTLENode");
            log.trace("parent: " + jnode.jjtGetParent());
            log.trace("object: " + o);
        }
        DatawaveTreeNode node = new DatawaveTreeNode(ParserTreeConstants.JJTLENODE);
        node.setOperator(JexlOperatorConstants.getOperator(ASTLENode.class));
        
        // left
        JexlNode child = (JexlNode) jnode.jjtGetChild(0).jjtAccept(this, node);
        processLeft(node, child);
        
        // right
        child = (JexlNode) jnode.jjtGetChild(1).jjtAccept(this, node);
        processRight(node, child);
        
        if ((node.getFieldName() != null && !node.getFieldName().isEmpty())
                        && (node.getFieldValue() != null && !node.getFieldValue().isEmpty() || (node.getFieldValue() == null
                                        && node.getFieldValueLiteralType() != null && node.getFieldValueLiteralType().equals(ASTNullLiteral.class)))) {
            
            queryTermMap.put(node.getFieldName(), node);
        }
        
        log.debug(node.getContents());
        return node;
    }
    
    @Override
    public Object visit(ASTGENode jnode, Object o) {
        if (log.isTraceEnabled()) {
            log.trace("ASTGENode");
            log.trace("parent: " + jnode.jjtGetParent());
            log.trace("object: " + o);
        }
        DatawaveTreeNode node = new DatawaveTreeNode(ParserTreeConstants.JJTGENODE);
        node.setOperator(JexlOperatorConstants.getOperator(ASTGENode.class));
        
        // left
        JexlNode child = (JexlNode) jnode.jjtGetChild(0).jjtAccept(this, node);
        processLeft(node, child);
        
        // right
        child = (JexlNode) jnode.jjtGetChild(1).jjtAccept(this, node);
        processRight(node, child);
        
        if ((node.getFieldName() != null && !node.getFieldName().isEmpty())
                        && (node.getFieldValue() != null && !node.getFieldValue().isEmpty() || (node.getFieldValue() == null
                                        && node.getFieldValueLiteralType() != null && node.getFieldValueLiteralType().equals(ASTNullLiteral.class)))) {
            
            queryTermMap.put(node.getFieldName(), node);
        }
        
        log.debug(node.getContents());
        return node;
    }
    
    @Override
    public Object visit(ASTERNode jnode, Object o) {
        if (log.isTraceEnabled()) {
            log.trace("ASTERNode");
            log.trace("parent: " + jnode.jjtGetParent());
            log.trace("object: " + o);
        }
        DatawaveTreeNode node = new DatawaveTreeNode(ParserTreeConstants.JJTERNODE);
        node.setOperator(JexlOperatorConstants.getOperator(ASTERNode.class));
        
        // left
        JexlNode child = (JexlNode) jnode.jjtGetChild(0).jjtAccept(this, node);
        processLeft(node, child);
        
        // right
        child = (JexlNode) jnode.jjtGetChild(1).jjtAccept(this, node);
        processRight(node, child);
        
        if ((node.getFieldName() != null && !node.getFieldName().isEmpty())
                        && (node.getFieldValue() != null && !node.getFieldValue().isEmpty() || (node.getFieldValue() == null
                                        && node.getFieldValueLiteralType() != null && node.getFieldValueLiteralType().equals(ASTNullLiteral.class)))) {
            
            queryTermMap.put(node.getFieldName(), node);
        }
        
        log.debug(node.getContents());
        return node;
    }
    
    @Override
    public Object visit(ASTNRNode jnode, Object o) {
        if (log.isTraceEnabled()) {
            log.trace("ASTNRNode");
            log.trace("parent: " + jnode.jjtGetParent());
            log.trace("object: " + o);
        }
        DatawaveTreeNode node = new DatawaveTreeNode(ParserTreeConstants.JJTERNODE);
        node.setOperator(JexlOperatorConstants.getOperator(ASTERNode.class));
        node.setNegated(true);
        
        // left
        JexlNode child = (JexlNode) jnode.jjtGetChild(0).jjtAccept(this, node);
        processLeft(node, child);
        
        // right
        child = (JexlNode) jnode.jjtGetChild(1).jjtAccept(this, node);
        processRight(node, child);
        
        if ((node.getFieldName() != null && !node.getFieldName().isEmpty())
                        && (node.getFieldValue() != null && !node.getFieldValue().isEmpty() || (node.getFieldValue() == null
                                        && node.getFieldValueLiteralType() != null && node.getFieldValueLiteralType().equals(ASTNullLiteral.class)))) {
            
            queryTermMap.put(node.getFieldName(), node);
        }
        
        log.debug(node.getContents());
        return node;
    }
    
    @Override
    public Object visit(ASTNullLiteral node, Object o) {
        log.trace("ASTNullLiteral");
        node.jjtSetValue(null);
        return node;
    }
    
    @Override
    public Object visit(ASTTrueNode node, Object o) {
        log.trace("ASTTrueNode");
        node.jjtSetValue(true);
        return node;
    }
    
    @Override
    public Object visit(ASTFalseNode node, Object o) {
        log.trace("ASTFalseNode");
        node.jjtSetValue(false);
        return node;
    }
    
    @Override
    public Object visit(ASTNumberLiteral node, Object o) {
        log.trace("ASTNumberLiteral");
        Class<? extends JexlNode> pClass = node.jjtGetParent().getClass();
        if ((pClass.equals(ASTAndNode.class) || pClass.equals(ASTOrNode.class) || pClass.equals(ASTJexlScript.class))) {
            if (log.isTraceEnabled()) {
                log.trace("ASTFloatLiteral node suspected to be unfielded: " + node.image);
            }
            this.unFieldedValues.add(node.image);
            DatawaveTreeNode ambig = new DatawaveTreeNode(ParserTreeConstants.JJTAMBIGUOUS, "", node.image);
            return ambig;
        }
        node.jjtSetValue(node.image);
        return node;
    }
    
    @Override
    public Object visit(ASTStringLiteral node, Object o) {
        if (log.isTraceEnabled()) {
            log.trace("ASTStringLiteral: " + node.image);
        }
        Class<? extends JexlNode> pClass = node.jjtGetParent().getClass();
        log.debug("pClass: " + pClass);
        if ((pClass.equals(ASTAndNode.class) || pClass.equals(ASTOrNode.class) || pClass.equals(ASTJexlScript.class) || pClass.equals(ASTNotNode.class))) {
            if (log.isTraceEnabled()) {
                log.trace("ASTStringLiteral node suspected to be unfielded: " + node.image);
            }
            this.unFieldedValues.add(node.image);
            DatawaveTreeNode ambig = new DatawaveTreeNode(ParserTreeConstants.JJTAMBIGUOUS, "", node.image);
            if (pClass.equals(ASTNotNode.class)) {
                ambig.setNegated(true);
            }
            return ambig;
        }
        node.jjtSetValue(node.image);
        return node;
    }
    
    /*
     * This is a little dirty. We return a JexlNode, in some cases its jjtGetValue() function returns a String and in others it returns a DatawaveTreeNode.
     */
    @Override
    public Object visit(ASTFunctionNode node, Object o) {
        if (log.isTraceEnabled()) {
            log.trace("ASTFunctionNode: " + node.image + "  childcount: " + node.jjtGetNumChildren());
        }
        DatawaveTreeNode bnode = new DatawaveTreeNode(ParserTreeConstants.JJTFUNCTIONNODE);
        bnode.setFunctionNode(true);
        bnode.setFunctionNamespace(node.jjtGetChild(0).image);
        bnode.setFunctionName(node.jjtGetChild(1).image);
        Map<String,Object> funcs = engine.getFunctions();
        bnode.setFunctionClass((Class<?>) funcs.get(bnode.getFunctionNamespace()));
        if (log.isTraceEnabled()) {
            log.trace("func class: " + bnode.getFunctionClass() + "  namespace: " + bnode.getFunctionNamespace() + " method: " + bnode.getFunctionName());
        }
        
        List<JexlNode> args = new ArrayList<>();
        for (int i = 2; i < node.jjtGetNumChildren(); i++) {
            if (log.isTraceEnabled()) {
                log.trace("\ti: " + i);
            }
            JexlNode child = node.jjtGetChild(i);
            JexlNode obj = (JexlNode) child.jjtAccept(this, node);
            args.add(obj);
            if (log.isTraceEnabled()) {
                log.trace("\tobj class: " + obj.getClass().toString());
            }
        }
        bnode.setFunctionArgs(args);
        ASTStringLiteral lit = new ASTStringLiteral(ParserTreeConstants.JJTSTRINGLITERAL);
        if (bnode.getFunctionNamespace().equalsIgnoreCase(NORMALIZER_NAMESPACE)) {
            if (log.isTraceEnabled()) {
                log.trace("NORMALIZER_NAMESPACE");
            }
            String normalized = (String) engine.invokeMethod(engine.newInstance(bnode.getFunctionClass()), bnode.getFunctionName(), bnode.getFunctionArgs()
                            .get(0).image);
            if (log.isTraceEnabled()) {
                log.trace("Normalize function output: " + normalized);
            }
            Class<? extends JexlNode> pClass = node.jjtGetParent().getClass();
            if (log.isTraceEnabled()) {
                log.trace("FunctionNode, parent class: " + pClass);
            }
            
            lit.jjtSetValue(normalized);
            if (log.isTraceEnabled()) {
                log.trace("returning ASTStringLiteral with value: " + normalized);
            }
            return lit;
        }
        // rip through the args again
        for (JexlNode jex : bnode.getFunctionArgs()) {
            if (jex.getClass().equals(ASTIdentifier.class)) {
                this.queryTermMap.put(jex.image, bnode);
            }
        }
        lit.jjtSetValue(bnode);
        return lit;
    }
    
    /* *************************************************************************
     * Visit methods that our parser currently does not support.
     */
    
    @Override
    public Object visit(ASTAdditiveNode astan, Object o) {
        log.trace("ASTAdditiveNode");
        return null;
    }
    
    @Override
    public Object visit(ASTAdditiveOperator astao, Object o) {
        log.trace("ASTAdditiveOperator");
        return null;
    }
    
    @Override
    public Object visit(ASTMulNode astmn, Object o) {
        log.trace("ASTMulNode");
        return null;
    }
    
    @Override
    public Object visit(ASTDivNode astdn, Object o) {
        log.trace("ASTDivNode");
        return null;
    }
    
    @Override
    public Object visit(ASTModNode astmn, Object o) {
        log.trace("ASTModNode");
        return null;
    }
    
    @Override
    public Object visit(ASTUnaryMinusNode astumn, Object o) {
        log.trace("ASTUnaryMinusNode");
        if (astumn.jjtGetNumChildren() == 1 && astumn.jjtGetChild(0) instanceof ASTNumberLiteral) {
            ASTNumberLiteral node = (ASTNumberLiteral) astumn.jjtGetChild(0);
            ASTNumberLiteral newNode = new ASTNumberLiteral(ParserTreeConstants.JJTNUMBERLITERAL);
            String value = "-" + node.image;
            newNode.image = value;
            newNode.jjtSetParent(node.jjtGetParent());
            
            if (NATURAL_NUMBERS.contains(node.getLiteralClass())) {
                newNode.setNatural(value);
            } else if (REAL_NUMBERS.contains(node.getLiteralClass())) {
                newNode.setReal(value);
            } else {
                throw new IllegalArgumentException("Could not ascertain type of ASTNumberLiteral: " + node);
            }
            
            newNode.jjtSetValue(value);
            
            return newNode;
        } else {
            return null;
        }
    }
    
    @Override
    public Object visit(ASTBitwiseComplNode astbcn, Object o) {
        log.trace("ASTBitwiseComplNode");
        return null;
    }
    
    @Override
    public Object visit(ASTMethodNode astmn, Object o) {
        log.trace("ASTMethodNode");
        return null;
    }
    
    @Override
    public Object visit(ASTSizeMethod astsm, Object o) {
        log.trace("ASTSizeMethod");
        return null;
    }
    
    @Override
    public Object visit(ASTConstructorNode astcn, Object o) {
        log.trace("ASTConstructorNode");
        return null;
    }
    
    @Override
    public Object visit(ASTArrayAccess astaa, Object o) {
        log.trace("ASTArrayAccess");
        return null;
    }
    
    @Override
    public Object visit(ASTBlock astb, Object o) {
        log.trace("ASTBlock");
        return null;
    }
    
    @Override
    public Object visit(ASTAmbiguous asta, Object o) {
        log.trace("ASTAmbiguous");
        return null;
    }
    
    @Override
    public Object visit(ASTIfStatement astis, Object o) {
        log.trace("ASTIfStatement");
        return null;
    }
    
    @Override
    public Object visit(ASTWhileStatement astws, Object o) {
        log.trace("ASTWhileStatement");
        return null;
    }
    
    @Override
    public Object visit(ASTForeachStatement astfs, Object o) {
        log.trace("ASTForeachStatement");
        return null;
    }
    
    @Override
    public Object visit(ASTAssignment asta, Object o) {
        log.trace("ASTAssignment");
        return null;
    }
    
    @Override
    public Object visit(ASTTernaryNode asttn, Object o) {
        log.trace("ASTTernaryNode");
        return null;
    }
    
    @Override
    public Object visit(ASTBitwiseOrNode astbon, Object o) {
        log.trace("ASTBitwiseOrNode");
        return null;
    }
    
    @Override
    public Object visit(ASTBitwiseXorNode astbxn, Object o) {
        log.trace("ASTBitwiseXorNode");
        return null;
    }
    
    @Override
    public Object visit(ASTBitwiseAndNode astban, Object o) {
        log.trace("ASTBitwiseAndNode");
        return null;
    }
    
    @Override
    public Object visit(ASTArrayLiteral astal, Object o) {
        log.trace("ASTArrayLiteral");
        return null;
    }
    
    @Override
    public Object visit(ASTMapLiteral astml, Object o) {
        log.trace("ASTMapLiteral");
        return null;
    }
    
    @Override
    public Object visit(ASTMapEntry astme, Object o) {
        log.trace("ASTMapEntry");
        return null;
    }
    
    @Override
    public Object visit(ASTEmptyFunction astef, Object o) {
        log.trace("ASTEmptyFunction");
        return null;
    }
    
    @Override
    public Object visit(ASTSizeFunction astsf, Object o) {
        log.trace("ASTSizeFunction");
        return null;
    }
    
}
