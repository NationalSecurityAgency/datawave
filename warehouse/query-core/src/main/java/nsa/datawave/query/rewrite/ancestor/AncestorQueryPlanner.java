package nsa.datawave.query.rewrite.ancestor;

import nsa.datawave.query.rewrite.CloseableIterable;
import nsa.datawave.query.rewrite.config.RefactoredShardQueryConfiguration;
import nsa.datawave.query.rewrite.exceptions.DatawaveQueryException;
import nsa.datawave.query.rewrite.exceptions.InvalidQueryException;
import nsa.datawave.query.rewrite.jexl.visitors.RootNegationCheckVisitor;
import nsa.datawave.query.rewrite.planner.DefaultQueryPlanner;
import nsa.datawave.query.rewrite.planner.QueryPlan;
import nsa.datawave.query.tables.ScannerFactory;
import nsa.datawave.query.util.MetadataHelper;
import nsa.datawave.query.util.Tuple2;
import org.apache.commons.jexl2.parser.JexlNode;

/**
 *
 */
public class AncestorQueryPlanner extends DefaultQueryPlanner {
    
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
