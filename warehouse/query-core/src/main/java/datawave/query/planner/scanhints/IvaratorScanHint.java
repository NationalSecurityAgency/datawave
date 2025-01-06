package datawave.query.planner.scanhints;

import org.apache.commons.jexl3.parser.JexlNode;

import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.NodeTypeCount;

/**
 * Detect ivarators and set the executor scan hint to ivarator
 */
public class IvaratorScanHint extends ExecutorScanHintRule {
    private static final String DEFAULT_TABLE = "shard";
    private static final String DEFAULT_HINT = "ivarator";

    public IvaratorScanHint() {
        super();
        setTable(DEFAULT_TABLE);
        setHintValue(DEFAULT_HINT);
    }

    @Override
    public boolean isChainable() {
        return true;
    }

    @Override
    public Boolean apply(JexlNode jexlNode) {
        if (jexlNode == null) {
            return false;
        }

        NodeTypeCount nodeCount = JexlASTHelper.getIvarators(jexlNode);
        int totalIvarators = JexlASTHelper.getIvaratorCount(nodeCount);

        return totalIvarators > 0;
    }
}
