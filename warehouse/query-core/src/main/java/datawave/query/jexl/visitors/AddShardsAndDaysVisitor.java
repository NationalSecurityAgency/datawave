package datawave.query.jexl.visitors;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import datawave.query.Constants;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.JexlNodeFactory;
import datawave.util.StringUtils;
import org.apache.commons.jexl3.parser.ASTAssignment;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ASTReferenceExpression;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.commons.jexl3.parser.JexlNodes;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * When SHARDS_AND_DAYS hints are supplied via the OPTIONS function, they should be added back into the query tree either by updating an existing
 * SHARDS_AND_DAYS assignment node if one exists, or by creating and appending a new SHARDS_AND_DAYS assignment node into the query tree structure.
 */
public class AddShardsAndDaysVisitor extends RebuildingVisitor {

    private static final Joiner JOINER = Joiner.on(',').skipNulls();

    @SuppressWarnings("unchecked")
    public static <T extends JexlNode> T update(T node, String shardsAndDays) {
        if (node == null) {
            return null;
        }

        if (shardsAndDays == null) {
            return node;
        }

        // @formatter:off
        List<String> validShardsAndDays = Arrays.stream(StringUtils.split(shardsAndDays, ','))
                        .map(String::trim)              // Strip whitespace.
                        .filter(s -> !s.isEmpty())      // Remove empty strings.
                        .distinct()                     // Remove duplicates.
                        .collect(Collectors.toList());
        // @formatter:on
        if (validShardsAndDays.isEmpty()) {
            return node;
        }

        AddShardsAndDaysVisitor visitor = new AddShardsAndDaysVisitor(validShardsAndDays);
        T modifiedCopy = (T) node.jjtAccept(visitor, null);
        // If the shards and days hints were not added to an existing SHARDS_AND_DAYS node, then we need to create one and add it to the query body.
        if (!visitor.updatedShardsAndDays) {
            addNewShardAndDaysNode(modifiedCopy, shardsAndDays);
        }
        return modifiedCopy;
    }

    // Create and append a new SHARDS_AND_DAYS hint node to the given query tree.
    private static void addNewShardAndDaysNode(JexlNode node, String shardsAndDays) {
        // If the root node is an ASTJexlScript, go down one level before adding the SHARDS_AND_DAYS node.
        if (node instanceof ASTJexlScript) {
            addNewShardAndDaysNode(node.jjtGetChild(0), shardsAndDays);
        } else {
            // Otherwise, add the SHARDS_AND_DAYS node.
            addNewShardsAndDaysNode(node, shardsAndDays);
        }
    }

    // Create and append a new SHARDS_AND_DAYS hint node and AND it to the given node.
    private static void addNewShardsAndDaysNode(JexlNode node, String shardsAndDays) {
        JexlNode originalParent = node.jjtGetParent();
        JexlNode shardAndHintNode = JexlNodes.wrap(JexlNodeFactory.createAssignment(Constants.SHARD_DAY_HINT, shardsAndDays));
        // If the current node is unwrapped, wrap it before ANDing it with the new SHARDS_AND_DAYS node.
        JexlNode leftSide = !(node instanceof ASTReferenceExpression) ? JexlNodes.wrap(node) : node;
        JexlNode andNode = JexlNodeFactory.createAndNode(Lists.newArrayList(leftSide, shardAndHintNode));
        // Update the grandparent's lineage.
        if (originalParent != null) {
            JexlNodes.replaceChild(originalParent, node, andNode);
        }
    }

    private final List<String> shardsAndDays;
    private boolean updatedShardsAndDays = false;

    private AddShardsAndDaysVisitor(List<String> shardsAndDays) {
        this.shardsAndDays = shardsAndDays;
    }

    @Override
    public Object visit(ASTAssignment node, Object data) {
        ASTAssignment copy = (ASTAssignment) super.visit(node, data);
        String identifier = JexlASTHelper.getIdentifier(copy);
        // If this is a SHARDS_AND_DAYS hint node, append the new shards and days hints to its literal value. Only do this to the first SHARDS_AND_DAYS node
        // that is found, if any.
        if (Constants.SHARD_DAY_HINT.equals(identifier) && !updatedShardsAndDays) {
            String nodeShardsAndDays = JexlASTHelper.getLiteralValue(copy).toString();
            String mergedNodeShardsAndDays = mergeShardsAndDays(nodeShardsAndDays);
            this.updatedShardsAndDays = true;
            return JexlNodeFactory.createAssignment(Constants.SHARD_DAY_HINT, mergedNodeShardsAndDays);
        }
        return copy;
    }

    // Merge the given shards and days with the shard and days in this visitor's list.
    private String mergeShardsAndDays(String shardsAndDays) {
        // Remove any duplicate shards and days hints from the initial list.
        String[] values = StringUtils.split(shardsAndDays, ',');
        for (String value : values) {
            this.shardsAndDays.remove(value);
        }
        // If they were all duplicates, return the original string. Otherwise append them.
        if (this.shardsAndDays.isEmpty()) {
            return shardsAndDays;
        } else if (shardsAndDays.isEmpty()) {
            return JOINER.join(this.shardsAndDays);
        } else {
            return shardsAndDays + "," + JOINER.join(this.shardsAndDays);
        }
    }

}
