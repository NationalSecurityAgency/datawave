package datawave.query.planner;

import java.util.concurrent.ExecutionException;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.commons.jexl3.parser.ASTJexlScript;

import datawave.microservice.query.Query;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.exceptions.DatawaveQueryException;
import datawave.query.exceptions.EmptyUnfieldedTermExpansionException;
import datawave.query.exceptions.FullTableScansDisallowedException;
import datawave.query.exceptions.NoResultsException;
import datawave.query.iterator.FieldIndexOnlyQueryIterator;
import datawave.query.jexl.visitors.AllTermsIndexedVisitor;
import datawave.query.tables.ScannerFactory;
import datawave.query.util.DateIndexHelper;
import datawave.query.util.MetadataHelper;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;

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
    public IteratorSetting getQueryIterator(MetadataHelper metadataHelper, ShardQueryConfiguration config, String queryString, Boolean isFullTable,
                    boolean isPreload) throws DatawaveQueryException {
        if (isFullTable) {
            QueryException qe = new QueryException(DatawaveErrorCode.FULL_TABLE_SCAN_DISALLOWED);
            throw new FullTableScansDisallowedException(qe);
        }

        IteratorSetting cfg = super.getQueryIterator(metadataHelper, config, queryString, isFullTable, isPreload);
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
                    ShardQueryConfiguration config, String query, Query settings) throws DatawaveQueryException {
        // we want all terms expanded (except when max terms is reached)
        config.setExpandAllTerms(true);

        // update the query tree
        ASTJexlScript script = super.updateQueryTree(scannerFactory, metadataHelper, dateIndexHelper, config, query, settings);

        return limitQueryTree(script, config);
    }

    protected ASTJexlScript limitQueryTree(ASTJexlScript script, ShardQueryConfiguration config) throws NoResultsException {
        // Assert that all of the terms in the query are indexed (so we can
        // completely use the field index)
        try {
            return AllTermsIndexedVisitor.isIndexed(script, config, metadataHelper);
        } catch (EmptyUnfieldedTermExpansionException e) {
            QueryException qe = new QueryException(DatawaveErrorCode.INDETERMINATE_INDEX_STATUS, e);
            throw new NoResultsException(qe);
        }
    }

    @Override
    public IndexQueryPlanner clone() {
        return new IndexQueryPlanner(this);
    }
}
