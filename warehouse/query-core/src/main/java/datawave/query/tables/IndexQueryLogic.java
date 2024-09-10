package datawave.query.tables;

import com.google.common.base.Preconditions;

import datawave.query.planner.IndexQueryPlanner;

/**
 *
 */
public class IndexQueryLogic extends ShardQueryLogic {

    public IndexQueryLogic() {
        super();
        setLimitAnyFieldLookups(false);
        this.setQueryPlanner(new IndexQueryPlanner());
    }

    public IndexQueryLogic(IndexQueryLogic other) {
        super(other);
        setLimitAnyFieldLookups(false);
    }

    @Override
    public IndexQueryLogic clone() {
        return new IndexQueryLogic(this);
    }

    @Override
    public void setFullTableScanEnabled(boolean fullTableScanEnabled) {
        Preconditions.checkArgument(!fullTableScanEnabled, "The IndexQueryLogic does not support full-table scans");

        super.setFullTableScanEnabled(fullTableScanEnabled);
    }

}
