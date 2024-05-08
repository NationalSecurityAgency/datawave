package datawave.query.jexl.visitors;

import static datawave.query.jexl.nodes.QueryPropertyMarker.MarkerType.DELAYED;

import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTEQNode;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ASTNENode;
import org.apache.commons.jexl3.parser.ASTOrNode;
import org.apache.commons.jexl3.parser.JexlNode;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import datawave.core.query.jexl.nodes.QueryPropertyMarker;
import datawave.core.query.jexl.visitors.BaseVisitor;
import datawave.core.query.jexl.visitors.TreeFlatteningRebuildingVisitor;
import datawave.query.iterator.NestedIterator;
import datawave.query.iterator.logic.IndexIteratorBridge;
import datawave.query.jexl.nodes.QueryPropertyMarker;

/**
 * Visitor builds a map from all nonEvent fields contained within delayed subtrees to their respective processing nodes. This map then can be used with the
 * DelayedNonEventIndexContext to fetch these values at evaluation time.
 */
public class DelayedNonEventSubTreeVisitor extends BaseVisitor {
    private IteratorBuildingVisitor iteratorBuildingVisitor;
    private Set<String> nonEventFields;
    private Multimap<String,JexlNode> delayedNonEventFieldMapNodes;

    public static Multimap<String,JexlNode> getDelayedNonEventFieldMap(IteratorBuildingVisitor iteratorBuildingVisitor, ASTJexlScript script,
                    Set<String> nonEventFields) {
        // ensure we are flattened
        ASTJexlScript copy = TreeFlatteningRebuildingVisitor.flatten(script);

        // run the visitor on the copy
        DelayedNonEventSubTreeVisitor visitor = new DelayedNonEventSubTreeVisitor(iteratorBuildingVisitor, nonEventFields);
        copy.jjtAccept(visitor, null);

        return visitor.delayedNonEventFieldMapNodes;
    }

    private DelayedNonEventSubTreeVisitor(IteratorBuildingVisitor iteratorBuildingVisitor, Set<String> nonEventFields) {
        this.iteratorBuildingVisitor = iteratorBuildingVisitor;
        this.nonEventFields = nonEventFields;
        delayedNonEventFieldMapNodes = HashMultimap.create();
    }

    /**
     * Check if the node is an instance of a DelayedPredicate and if so capture all nonEvent delayed nodes, ranges, and node operators into
     * delayedNonEventFieldMapNodes.
     *
     * @param node
     *            the node
     * @param data
     *            the data
     * @return the result of the visit
     */
    @Override
    public Object visit(ASTAndNode node, Object data) {
        QueryPropertyMarker.Instance instance = QueryPropertyMarker.findInstance(node);
        if (instance.isType(DELAYED) || (data != null && (boolean) data)) {
            // strip off only the delayed predicate, leave any other markers intact since they may be necessary to properly process inside the
            // IteratorBuildingVisitor
            JexlNode candidate = instance.isType(DELAYED) ? instance.getSource() : node;

            // noinspection ConstantConditions
            extractDelayedFields(candidate);

            // continue processing the tree in case the IteratorBuildingVisitor decided to discard leafs. The IteratorBuildingVisitor will only identify a root
            // if it has includes and for our purposes we need to keep processing.
            return candidate.childrenAccept(this, true);
        }

        // it wasn't a delayed node, process down the tree
        return node.childrenAccept(this, data);
    }

    @Override
    public Object visit(ASTOrNode node, Object data) {
        return processDelayedFields(node, data);
    }

    @Override
    public Object visit(ASTEQNode node, Object data) {
        return processDelayedFields(node, data);
    }

    @Override
    public Object visit(ASTNENode node, Object data) {
        return processDelayedFields(node, data);
    }

    private Object processDelayedFields(JexlNode node, Object data) {
        if (data != null && (boolean) data) {
            // continue processing the tree in case the IteratorBuildingVisitor decided to discard leafs. This is not the most efficient or ideal
            // but because the IteratorBuildingVisitor will only identify a root if it has includes we have to look after each recursion.
            extractDelayedFields(node);
        }

        return node.childrenAccept(this, data);
    }

    private void extractDelayedFields(JexlNode node) {
        // reset the root before processing a a node
        iteratorBuildingVisitor.resetRoot();

        // use the iterator building visitor to build a field to node map for each delayed node
        node.jjtAccept(iteratorBuildingVisitor, null);
        NestedIterator<Key> root = iteratorBuildingVisitor.root();
        if (root != null) {
            for (NestedIterator<Key> leaf : root.leaves()) {
                // only IndexIteratorBridge nodes matter, everything else can be ignored
                if (leaf instanceof IndexIteratorBridge) {
                    String fieldName = ((IndexIteratorBridge) leaf).getField();
                    if (nonEventFields.contains(fieldName)) {
                        JexlNode leafNode = ((IndexIteratorBridge) leaf).getSourceNode();
                        JexlNode targetNode = leafNode;

                        delayedNonEventFieldMapNodes.put(fieldName, targetNode);
                    }
                }
            }
        }
    }
}
