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
import org.apache.commons.jexl2.parser.ParserVisitor;
import org.apache.commons.jexl2.parser.SimpleNode;

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
public class OrderOptimizedUidScanner extends AbstractUidScanner implements ParserVisitor {

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
    public Object visit(SimpleNode node, Object data) {
        return null;
    }

    // short circuits
    @Override
    public Object visit(ASTBlock node, Object data) {
        return null;
    }

    @Override
    public Object visit(ASTAmbiguous node, Object data) {
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
    public Object visit(ASTTernaryNode node, Object data) {
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
    public Object visit(ASTAdditiveNode node, Object data) {
        return null;
    }

    @Override
    public Object visit(ASTAdditiveOperator node, Object data) {
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
    public Object visit(ASTArrayLiteral node, Object data) {
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
    public Object visit(ASTSizeMethod node, Object data) {
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
}
