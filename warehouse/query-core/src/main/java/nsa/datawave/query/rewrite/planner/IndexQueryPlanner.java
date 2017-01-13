package nsa.datawave.query.rewrite.planner;

import java.util.concurrent.ExecutionException;

import nsa.datawave.query.rewrite.config.RefactoredShardQueryConfiguration;
import nsa.datawave.query.rewrite.exceptions.CannotExpandUnfieldedTermFatalException;
import nsa.datawave.query.rewrite.exceptions.DatawaveQueryException;
import nsa.datawave.query.rewrite.exceptions.FullTableScansDisallowedException;
import nsa.datawave.query.rewrite.exceptions.NoResultsException;
import nsa.datawave.query.rewrite.iterator.FieldIndexOnlyQueryIterator;
import nsa.datawave.query.rewrite.jexl.visitors.AllTermsIndexedVisitor;
import nsa.datawave.query.tables.ScannerFactory;
import nsa.datawave.query.util.DateIndexHelper;
import nsa.datawave.query.util.MetadataHelper;
import nsa.datawave.webservice.query.Query;
import nsa.datawave.webservice.query.configuration.QueryData;
import nsa.datawave.webservice.query.exception.DatawaveErrorCode;
import nsa.datawave.webservice.query.exception.QueryException;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.commons.jexl2.parser.ASTJexlScript;

/**
 *
 */
public class IndexQueryPlanner extends DefaultQueryPlanner {
    public IndexQueryPlanner(IndexQueryPlanner indexQueryPlanner) {
        super(indexQueryPlanner);
    }
    
    public IndexQueryPlanner() {
        super();
    }
    
    @Override
    public IteratorSetting getQueryIterator(MetadataHelper metadataHelper, RefactoredShardQueryConfiguration config, Query settings, String queryString,
                    Boolean isFullTable) throws DatawaveQueryException {
        if (isFullTable) {
            QueryException qe = new QueryException(DatawaveErrorCode.FULL_TABLE_SCAN_DISALLOWED);
            throw new FullTableScansDisallowedException(qe);
        }
        
        IteratorSetting cfg = super.getQueryIterator(metadataHelper, config, settings, queryString, isFullTable);
        if (null == cfg) {
            try {
                cfg = settingFuture.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
        
        cfg.setIteratorClass(FieldIndexOnlyQueryIterator.class.getName());
        
        return cfg;
    }
    
    @Override
    protected ASTJexlScript updateQueryTree(ScannerFactory scannerFactory, MetadataHelper metadataHelper, DateIndexHelper dateIndexHelper,
                    RefactoredShardQueryConfiguration config, String query, QueryData queryData, Query settings) throws DatawaveQueryException {
        // we want all terms expanded (except when max terms is reached)
        config.setExpandAllTerms(true);
        
        // update the query tree
        ASTJexlScript script = super.updateQueryTree(scannerFactory, metadataHelper, dateIndexHelper, config, query, queryData, settings);
        
        return limitQueryTree(script, config);
    }
    
    protected ASTJexlScript limitQueryTree(ASTJexlScript script, RefactoredShardQueryConfiguration config) throws NoResultsException {
        // Assert that all of the terms in the query are indexed (so we can
        // completely use the field index)
        // Also removes any spurious _ANYFIELD_ nodes left in from upstream
        try {
            return AllTermsIndexedVisitor.isIndexed(script, config, metadataHelper);
        } catch (CannotExpandUnfieldedTermFatalException e) {
            QueryException qe = new QueryException(DatawaveErrorCode.INDETERMINATE_INDEX_STATUS, e);
            throw new NoResultsException(qe);
        }
    }
    
    @Override
    public IndexQueryPlanner clone() {
        return new IndexQueryPlanner(this);
    }
}
