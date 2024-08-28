package datawave.query.planner.scanhints;

import org.apache.commons.jexl3.parser.JexlNode;

import datawave.query.config.ScanHintRule;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.NodeTypeCount;

public class IvaratorScanHint implements ScanHintRule<JexlNode> {
    @Override
    public boolean isChainable() {
        return false;
    }

    @Override
    public String getTable() {
        return "shard";
    }

    @Override
    public String getHintName() {
        return "scan_type";
    }

    @Override
    public String getHintValue() {
        return "ivarator";
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
