package datawave.query.planner.scanhints;

import org.apache.commons.jexl3.parser.JexlNode;

import datawave.query.config.ScanHintRule;

/**
 * Scan hints that are used to set a priority in accumulo,
 *
 * @see <a href="https://accumulo.apache.org/docs/2.x/administration/scan-executors">https://accumulo.apache.org/docs/2.x/administration/scan-executors</a>
 */
public abstract class PriorityScanHintRule implements ScanHintRule<JexlNode> {
    private static final String SCAN_HINT = "priority";

    @Override
    public String getHintName() {
        return SCAN_HINT;
    }
}
