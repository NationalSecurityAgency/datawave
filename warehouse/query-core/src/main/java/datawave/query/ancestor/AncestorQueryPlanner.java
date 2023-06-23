package datawave.query.ancestor;

import datawave.query.CloseableIterable;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.exceptions.DatawaveQueryException;
import datawave.query.exceptions.InvalidQueryException;
import datawave.query.jexl.visitors.RootNegationCheckVisitor;
import datawave.query.planner.DefaultQueryPlanner;
import datawave.query.planner.QueryPlan;
import datawave.query.tables.ScannerFactory;
import datawave.query.util.MetadataHelper;
import datawave.query.util.Tuple2;
import org.apache.commons.jexl2.parser.JexlNode;

/**
 *
 */
public class AncestorQueryPlanner extends DefaultQueryPlanner {
    public AncestorQueryPlanner() {
        super();
    }

    public AncestorQueryPlanner(long maxRangesPerQueryPiece) {
        super(maxRangesPerQueryPiece);
    }

    public AncestorQueryPlanner(long maxRangesPerQueryPiece, boolean limitScanners) {
        super(maxRangesPerQueryPiece, limitScanners);
    }

    public AncestorQueryPlanner(AncestorQueryPlanner other) {
        super(other);
    }

    /**
     * Test for top level negation in query and throw exception otherwise proceed
     *
     * @param scannerFactory
     *            the scanner factory
     * @param metadataHelper
     *            the metadata helper
     * @param config
     *            the shard config
     * @param queryTree
     *            the query tree
     * @return the query ranges
     * @throws DatawaveQueryException
     *             for issues related to the query
     */
    @Override
    public Tuple2<CloseableIterable<QueryPlan>,Boolean> getQueryRanges(ScannerFactory scannerFactory, MetadataHelper metadataHelper,
                    ShardQueryConfiguration config, JexlNode queryTree) throws DatawaveQueryException {
        if (RootNegationCheckVisitor.hasTopLevelNegation(queryTree)) {
            throw new InvalidQueryException();
        }

        return super.getQueryRanges(scannerFactory, metadataHelper, config, queryTree);
    }
}
