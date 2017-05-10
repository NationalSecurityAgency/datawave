package datawave.query.rewrite.ancestor;

import datawave.query.rewrite.CloseableIterable;
import datawave.query.rewrite.config.RefactoredShardQueryConfiguration;
import datawave.query.rewrite.exceptions.DatawaveQueryException;
import datawave.query.rewrite.exceptions.InvalidQueryException;
import datawave.query.rewrite.jexl.visitors.RootNegationCheckVisitor;
import datawave.query.rewrite.planner.DefaultQueryPlanner;
import datawave.query.rewrite.planner.QueryPlan;
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
     * @param metadataHelper
     * @param config
     * @param queryTree
     * @return
     * @throws DatawaveQueryException
     */
    @Override
    public Tuple2<CloseableIterable<QueryPlan>,Boolean> getQueryRanges(ScannerFactory scannerFactory, MetadataHelper metadataHelper,
                    RefactoredShardQueryConfiguration config, JexlNode queryTree) throws DatawaveQueryException {
        if (RootNegationCheckVisitor.hasTopLevelNegation(queryTree)) {
            throw new InvalidQueryException();
        }
        
        return super.getQueryRanges(scannerFactory, metadataHelper, config, queryTree);
    }
}
