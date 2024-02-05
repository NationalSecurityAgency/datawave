package datawave.experimental.fi;

import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.jexl3.parser.ASTAddNode;
import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTAnnotatedStatement;
import org.apache.commons.jexl3.parser.ASTAnnotation;
import org.apache.commons.jexl3.parser.ASTArguments;
import org.apache.commons.jexl3.parser.ASTArrayAccess;
import org.apache.commons.jexl3.parser.ASTArrayLiteral;
import org.apache.commons.jexl3.parser.ASTAssignment;
import org.apache.commons.jexl3.parser.ASTBitwiseAndNode;
import org.apache.commons.jexl3.parser.ASTBitwiseComplNode;
import org.apache.commons.jexl3.parser.ASTBitwiseOrNode;
import org.apache.commons.jexl3.parser.ASTBitwiseXorNode;
import org.apache.commons.jexl3.parser.ASTBlock;
import org.apache.commons.jexl3.parser.ASTBreak;
import org.apache.commons.jexl3.parser.ASTConstructorNode;
import org.apache.commons.jexl3.parser.ASTContinue;
import org.apache.commons.jexl3.parser.ASTDecrementGetNode;
import org.apache.commons.jexl3.parser.ASTDefineVars;
import org.apache.commons.jexl3.parser.ASTDivNode;
import org.apache.commons.jexl3.parser.ASTDoWhileStatement;
import org.apache.commons.jexl3.parser.ASTEQNode;
import org.apache.commons.jexl3.parser.ASTERNode;
import org.apache.commons.jexl3.parser.ASTEWNode;
import org.apache.commons.jexl3.parser.ASTEmptyFunction;
import org.apache.commons.jexl3.parser.ASTExtendedLiteral;
import org.apache.commons.jexl3.parser.ASTFalseNode;
import org.apache.commons.jexl3.parser.ASTForeachStatement;
import org.apache.commons.jexl3.parser.ASTFunctionNode;
import org.apache.commons.jexl3.parser.ASTGENode;
import org.apache.commons.jexl3.parser.ASTGTNode;
import org.apache.commons.jexl3.parser.ASTGetDecrementNode;
import org.apache.commons.jexl3.parser.ASTGetIncrementNode;
import org.apache.commons.jexl3.parser.ASTIdentifier;
import org.apache.commons.jexl3.parser.ASTIdentifierAccess;
import org.apache.commons.jexl3.parser.ASTIfStatement;
import org.apache.commons.jexl3.parser.ASTIncrementGetNode;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ASTJxltLiteral;
import org.apache.commons.jexl3.parser.ASTLENode;
import org.apache.commons.jexl3.parser.ASTLTNode;
import org.apache.commons.jexl3.parser.ASTMapEntry;
import org.apache.commons.jexl3.parser.ASTMapLiteral;
import org.apache.commons.jexl3.parser.ASTMethodNode;
import org.apache.commons.jexl3.parser.ASTModNode;
import org.apache.commons.jexl3.parser.ASTMulNode;
import org.apache.commons.jexl3.parser.ASTNENode;
import org.apache.commons.jexl3.parser.ASTNEWNode;
import org.apache.commons.jexl3.parser.ASTNRNode;
import org.apache.commons.jexl3.parser.ASTNSWNode;
import org.apache.commons.jexl3.parser.ASTNotNode;
import org.apache.commons.jexl3.parser.ASTNullLiteral;
import org.apache.commons.jexl3.parser.ASTNullpNode;
import org.apache.commons.jexl3.parser.ASTNumberLiteral;
import org.apache.commons.jexl3.parser.ASTOrNode;
import org.apache.commons.jexl3.parser.ASTQualifiedIdentifier;
import org.apache.commons.jexl3.parser.ASTRangeNode;
import org.apache.commons.jexl3.parser.ASTReference;
import org.apache.commons.jexl3.parser.ASTReferenceExpression;
import org.apache.commons.jexl3.parser.ASTRegexLiteral;
import org.apache.commons.jexl3.parser.ASTReturnStatement;
import org.apache.commons.jexl3.parser.ASTSWNode;
import org.apache.commons.jexl3.parser.ASTSetAddNode;
import org.apache.commons.jexl3.parser.ASTSetAndNode;
import org.apache.commons.jexl3.parser.ASTSetDivNode;
import org.apache.commons.jexl3.parser.ASTSetLiteral;
import org.apache.commons.jexl3.parser.ASTSetModNode;
import org.apache.commons.jexl3.parser.ASTSetMultNode;
import org.apache.commons.jexl3.parser.ASTSetOrNode;
import org.apache.commons.jexl3.parser.ASTSetShiftLeftNode;
import org.apache.commons.jexl3.parser.ASTSetShiftRightNode;
import org.apache.commons.jexl3.parser.ASTSetShiftRightUnsignedNode;
import org.apache.commons.jexl3.parser.ASTSetSubNode;
import org.apache.commons.jexl3.parser.ASTSetXorNode;
import org.apache.commons.jexl3.parser.ASTShiftLeftNode;
import org.apache.commons.jexl3.parser.ASTShiftRightNode;
import org.apache.commons.jexl3.parser.ASTShiftRightUnsignedNode;
import org.apache.commons.jexl3.parser.ASTSizeFunction;
import org.apache.commons.jexl3.parser.ASTStringLiteral;
import org.apache.commons.jexl3.parser.ASTSubNode;
import org.apache.commons.jexl3.parser.ASTTernaryNode;
import org.apache.commons.jexl3.parser.ASTTrueNode;
import org.apache.commons.jexl3.parser.ASTUnaryMinusNode;
import org.apache.commons.jexl3.parser.ASTUnaryPlusNode;
import org.apache.commons.jexl3.parser.ASTVar;
import org.apache.commons.jexl3.parser.ASTWhileStatement;
import org.apache.commons.jexl3.parser.JexlNode;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.order.OrderByCostVisitor;

/**
 * Implementation of an AbstractUidScanner that visits terms in order by cost and performs an intersecting or joining scan.
 * <p>
 * Goal: push multiple terms per scan
 * <p>
 * Will settle for serial scans
 * <p>
 * Once an 'anchor' scan has been executed
 */
public class OrderOptimizedUidScanner extends AbstractUidScanner {

    private String row;
    private Set<String> indexedFields;

    public OrderOptimizedUidScanner(AccumuloClient client, Authorizations auths, String tableName, String scanId) {
        super(client, auths, tableName, scanId);
    }

    @SuppressWarnings("unchecked")
    @Override
    public SortedSet<String> scan(ASTJexlScript script, String row, Set<String> indexedFields) {
        this.row = row;
        this.indexedFields = indexedFields;

        ASTJexlScript orderedScript = OrderByCostVisitor.order(script);
        return (SortedSet<String>) orderedScript.jjtAccept(this, null);
    }

    @Override
    public void setLogStats(boolean logStats) {
        // no op
    }

    @Override
    public Object visit(ASTJexlScript node, Object data) {
        return node.jjtGetChild(0).jjtAccept(this, data);
    }

    @Override
    public Object visit(ASTReference node, Object data) {
        return node.jjtGetChild(0).jjtAccept(this, data);
    }

    @Override
    public Object visit(ASTReferenceExpression node, Object data) {
        return node.jjtGetChild(0).jjtAccept(this, data);
    }

    @Override
    protected Object visit(ASTSetAddNode astSetAddNode, Object o) {
        return null;
    }

    @Override
    protected Object visit(ASTSetSubNode astSetSubNode, Object o) {
        return null;
    }

    @Override
    protected Object visit(ASTSetMultNode astSetMultNode, Object o) {
        return null;
    }

    @Override
    protected Object visit(ASTSetDivNode astSetDivNode, Object o) {
        return null;
    }

    @Override
    protected Object visit(ASTSetModNode astSetModNode, Object o) {
        return null;
    }

    @Override
    protected Object visit(ASTSetAndNode astSetAndNode, Object o) {
        return null;
    }

    @Override
    protected Object visit(ASTSetOrNode astSetOrNode, Object o) {
        return null;
    }

    @Override
    protected Object visit(ASTSetXorNode astSetXorNode, Object o) {
        return null;
    }

    @Override
    protected Object visit(ASTSetShiftLeftNode astSetShiftLeftNode, Object o) {
        return null;
    }

    @Override
    protected Object visit(ASTSetShiftRightNode astSetShiftRightNode, Object o) {
        return null;
    }

    @Override
    protected Object visit(ASTSetShiftRightUnsignedNode astSetShiftRightUnsignedNode, Object o) {
        return null;
    }

    @Override
    protected Object visit(ASTGetDecrementNode astGetDecrementNode, Object o) {
        return null;
    }

    @Override
    protected Object visit(ASTGetIncrementNode astGetIncrementNode, Object o) {
        return null;
    }

    @Override
    protected Object visit(ASTDecrementGetNode astDecrementGetNode, Object o) {
        return null;
    }

    @Override
    protected Object visit(ASTIncrementGetNode astIncrementGetNode, Object o) {
        return null;
    }

    @Override
    protected Object visit(ASTJxltLiteral astJxltLiteral, Object o) {
        return null;
    }

    @Override
    protected Object visit(ASTAnnotation astAnnotation, Object o) {
        return null;
    }

    @Override
    protected Object visit(ASTAnnotatedStatement astAnnotatedStatement, Object o) {
        return null;
    }

    @Override
    protected Object visit(ASTQualifiedIdentifier astQualifiedIdentifier, Object o) {
        return null;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object visit(ASTOrNode node, Object data) {
        SortedSet<String> uids = processData(data);
        SortedSet<String> ourUids = new TreeSet<>();
        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            JexlNode child = node.jjtGetChild(i);
            if (isJunction(child)) {
                SortedSet<String> childUids = (SortedSet<String>) child.jjtAccept(this, data);
                ourUids.addAll(childUids);
            } else if (isLeaf(child)) {
                if (!uids.isEmpty()) {
                    // a parent intersection can be used to bound the scan for a grandchild leaf (AND - OR - LEAF)
                    SortedSet<String> childUids = scanForLeaf(child, uids.first(), uids.last());
                    ourUids.addAll(childUids);
                } else {
                    SortedSet<String> childUids = scanForLeaf(child);
                    ourUids.addAll(childUids);
                }
            }
        }
        uids.addAll(ourUids);
        return uids;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object visit(ASTAndNode node, Object data) {
        SortedSet<String> uids = processData(data);
        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            JexlNode child = node.jjtGetChild(i);
            if (isJunction(child)) {
                SortedSet<String> childUids = (SortedSet<String>) child.jjtAccept(this, data);
                uids = processIntersectionUids(uids, childUids);
                if (uids.isEmpty()) {
                    return uids;
                }
            } else if (isLeaf(child)) {
                if (!uids.isEmpty()) {
                    SortedSet<String> childUids = scanForLeaf(child, uids.first(), uids.last());
                    uids = processIntersectionUids(uids, childUids);
                } else {
                    SortedSet<String> childUids = scanForLeaf(child);
                    uids = processIntersectionUids(uids, childUids);
                }

                if (uids.isEmpty()) {
                    return uids;
                }
            }
        }
        return uids;
    }

    /**
     * Intersects child uids with an existing set of uids
     *
     * @param uids
     *            existing uids
     * @param childUids
     *            uids for child
     * @return the intersected uids
     */
    private SortedSet<String> processIntersectionUids(SortedSet<String> uids, SortedSet<String> childUids) {
        if (uids.isEmpty()) {
            // initial state, child is the first scan of the query
            return childUids;
        }

        uids.retainAll(childUids);
        return uids;
    }

    /**
     * Processes the data object by casting it to a SortedSet of uids or returns an empty collection
     *
     * @param data
     *            the data passed around
     * @return a sorted set
     */
    @SuppressWarnings("unchecked")
    private SortedSet<String> processData(Object data) {
        if (data instanceof SortedSet) {
            return (SortedSet<String>) data;
        }
        return new TreeSet<>();
    }

    private boolean isLeaf(JexlNode node) {
        JexlNode deref = JexlASTHelper.dereference(node);
        if (deref instanceof ASTEQNode || deref instanceof ASTERNode) {
            String field = JexlASTHelper.getIdentifier(deref);
            return indexedFields.contains(field);
        }
        return false;
    }

    private boolean isJunction(JexlNode node) {
        JexlNode deref = JexlASTHelper.dereference(node);
        //  @formatter:off
        return deref instanceof ASTAndNode ||
                        deref instanceof ASTOrNode ||
                        deref instanceof ASTReference ||
                        deref instanceof ASTReferenceExpression ||
                        deref instanceof ASTNotNode;
        //  @formatter:on
    }

    private SortedSet<String> scanForLeaf(JexlNode node) {

        SortedSet<String> uids = new TreeSet<>();
        Range range = rangeBuilder.rangeFromTerm(row, node);

        try (Scanner scanner = client.createScanner(tableName, auths)) {
            scanner.setRange(range);

            for (Map.Entry<Key,Value> entry : scanner) {
                uids.add(dtUidFromKey(entry.getKey()));
            }
        } catch (TableNotFoundException e) {
            throw new RuntimeException(e);
        }

        return uids;
    }

    // assumes single datatype
    private SortedSet<String> scanForLeaf(JexlNode node, String startUid, String stopUid) {
        SortedSet<String> uids = new TreeSet<>();
        Range range = rangeBuilder.rangeFromTerm(row, node, startUid, null);

        try (Scanner scanner = client.createScanner(tableName, auths)) {
            scanner.setRange(range);

            for (Map.Entry<Key,Value> entry : scanner) {
                uids.add(dtUidFromKey(entry.getKey()));
            }
        } catch (TableNotFoundException e) {
            throw new RuntimeException(e);
        }

        return uids;
    }

    private String dtUidFromKey(Key key) {
        String cq = key.getColumnQualifier().toString();
        int index = cq.indexOf('\u0000');
        return cq.substring(index + 1);
    }

    @Override
    public Object visit(ASTNotNode node, Object data) {
        // node.childrenAccept(this, data);
        return data;
    }

    @Override
    public Object visit(ASTEQNode node, Object data) {
        String field = JexlASTHelper.getIdentifier(node);
        if (indexedFields.contains(field)) {
            return scanForLeaf(node);
        }
        return data;
    }

    @Override
    public Object visit(ASTNENode node, Object data) {
        throw new IllegalStateException("Encountered a NOT-EQUALS node. This shouldn't happen.");
    }

    @Override
    public Object visit(ASTLTNode node, Object data) {
        throw new IllegalStateException("LT node should be handled by bounded range");
    }

    @Override
    public Object visit(ASTGTNode node, Object data) {
        throw new IllegalStateException("GT node should be handled by bounded range");
    }

    @Override
    public Object visit(ASTLENode node, Object data) {
        throw new IllegalStateException("LE node should be handled by bounded range");
    }

    @Override
    public Object visit(ASTGENode node, Object data) {
        throw new IllegalStateException("GE node should be handled by bounded range");
    }

    @Override
    public Object visit(ASTERNode node, Object data) {
        String field = JexlASTHelper.getIdentifier(node);
        if (indexedFields.contains(field)) {
            return scanForLeaf(node);
        }
        return data;
    }

    @Override
    public Object visit(ASTNRNode node, Object data) {
        throw new IllegalStateException("Negated Regexes not supported.");
    }

    @Override
    protected Object visit(ASTSWNode astswNode, Object o) {
        return null;
    }

    @Override
    protected Object visit(ASTNSWNode astnswNode, Object o) {
        return null;
    }

    @Override
    protected Object visit(ASTEWNode astewNode, Object o) {
        return null;
    }

    @Override
    protected Object visit(ASTNEWNode astnewNode, Object o) {
        return null;
    }

    @Override
    protected Object visit(ASTAddNode astAddNode, Object o) {
        return null;
    }

    @Override
    protected Object visit(ASTSubNode astSubNode, Object o) {
        return null;
    }

    // short circuits
    @Override
    public Object visit(ASTBlock node, Object data) {
        return null;
    }

    @Override
    public Object visit(ASTIfStatement node, Object data) {
        return null;
    }

    @Override
    public Object visit(ASTWhileStatement node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTDoWhileStatement astDoWhileStatement, Object o) {
        return null;
    }

    @Override
    protected Object visit(ASTContinue astContinue, Object o) {
        return null;
    }

    @Override
    protected Object visit(ASTBreak astBreak, Object o) {
        return null;
    }

    @Override
    public Object visit(ASTForeachStatement node, Object data) {
        return null;
    }

    @Override
    public Object visit(ASTReturnStatement node, Object data) {
        return null;
    }

    @Override
    public Object visit(ASTAssignment node, Object data) {
        return null;
    }

    @Override
    public Object visit(ASTVar node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTDefineVars astDefineVars, Object o) {
        return null;
    }

    @Override
    public Object visit(ASTTernaryNode node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTNullpNode astNullpNode, Object o) {
        return null;
    }

    @Override
    public Object visit(ASTBitwiseOrNode node, Object data) {
        return null;
    }

    @Override
    public Object visit(ASTBitwiseXorNode node, Object data) {
        return null;
    }

    @Override
    public Object visit(ASTBitwiseAndNode node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTShiftLeftNode astShiftLeftNode, Object o) {
        return null;
    }

    @Override
    protected Object visit(ASTShiftRightNode astShiftRightNode, Object o) {
        return null;
    }

    @Override
    protected Object visit(ASTShiftRightUnsignedNode astShiftRightUnsignedNode, Object o) {
        return null;
    }

    @Override
    public Object visit(ASTMulNode node, Object data) {
        return null;
    }

    @Override
    public Object visit(ASTDivNode node, Object data) {
        return null;
    }

    @Override
    public Object visit(ASTModNode node, Object data) {
        return null;
    }

    @Override
    public Object visit(ASTUnaryMinusNode node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTUnaryPlusNode astUnaryPlusNode, Object o) {
        return null;
    }

    @Override
    public Object visit(ASTBitwiseComplNode node, Object data) {
        return null;
    }

    @Override
    public Object visit(ASTIdentifier node, Object data) {
        return null;
    }

    @Override
    public Object visit(ASTNullLiteral node, Object data) {
        return null;
    }

    @Override
    public Object visit(ASTTrueNode node, Object data) {
        return null;
    }

    @Override
    public Object visit(ASTFalseNode node, Object data) {
        return null;
    }

    @Override
    public Object visit(ASTNumberLiteral node, Object data) {
        return null;
    }

    @Override
    public Object visit(ASTStringLiteral node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTRegexLiteral astRegexLiteral, Object o) {
        return null;
    }

    @Override
    protected Object visit(ASTSetLiteral astSetLiteral, Object o) {
        return null;
    }

    @Override
    protected Object visit(ASTExtendedLiteral astExtendedLiteral, Object o) {
        return null;
    }

    @Override
    public Object visit(ASTArrayLiteral node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTRangeNode astRangeNode, Object o) {
        return null;
    }

    @Override
    public Object visit(ASTMapLiteral node, Object data) {
        return null;
    }

    @Override
    public Object visit(ASTMapEntry node, Object data) {
        return null;
    }

    @Override
    public Object visit(ASTEmptyFunction node, Object data) {
        return null;
    }

    @Override
    public Object visit(ASTSizeFunction node, Object data) {
        return null;
    }

    @Override
    public Object visit(ASTFunctionNode node, Object data) {
        return null;
    }

    @Override
    public Object visit(ASTMethodNode node, Object data) {
        return null;
    }

    @Override
    public Object visit(ASTConstructorNode node, Object data) {
        return null;
    }

    @Override
    public Object visit(ASTArrayAccess node, Object data) {
        return null;
    }

    @Override
    protected Object visit(ASTIdentifierAccess astIdentifierAccess, Object o) {
        return null;
    }

    @Override
    protected Object visit(ASTArguments astArguments, Object o) {
        return null;
    }
}
