package nsa.datawave.query.rewrite.planner;

import nsa.datawave.query.rewrite.CloseableIterable;
import nsa.datawave.query.rewrite.config.RefactoredShardQueryConfiguration;
import nsa.datawave.query.rewrite.exceptions.CannotExpandUnfieldedTermFatalException;
import nsa.datawave.query.rewrite.exceptions.DatawaveQueryException;
import nsa.datawave.query.rewrite.exceptions.FullTableScansDisallowedException;
import nsa.datawave.query.rewrite.exceptions.NoResultsException;
import nsa.datawave.query.rewrite.iterator.facets.DynamicFacetIterator;
import nsa.datawave.query.rewrite.iterator.facets.FacetedTableIterator;
import nsa.datawave.query.rewrite.jexl.visitors.AllTermsIndexedVisitor;
import nsa.datawave.query.rewrite.tables.facets.FacetCheck;
import nsa.datawave.query.rewrite.tables.facets.FacetQueryPlanVisitor;
import nsa.datawave.query.rewrite.tables.facets.FacetedConfiguration;
import nsa.datawave.query.rewrite.tables.facets.FacetedSearchType;
import nsa.datawave.query.tables.ScannerFactory;
import nsa.datawave.query.util.DateIndexHelper;
import nsa.datawave.query.util.MetadataHelper;
import nsa.datawave.query.util.Tuple2;
import nsa.datawave.webservice.query.Query;
import nsa.datawave.webservice.query.configuration.QueryData;
import nsa.datawave.webservice.query.exception.DatawaveErrorCode;
import nsa.datawave.webservice.query.exception.QueryException;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.log4j.Logger;

import com.google.common.base.Joiner;

/**
 *
 */
public class FacetedQueryPlanner extends IndexQueryPlanner {
    
    protected FacetedConfiguration facetedConfig;
    
    private static final Logger log = Logger.getLogger(FacetedQueryPlanner.class);
    
    boolean usePrecomputedFacets = false;
    
    /**
     * @param documentcount
     */
    public FacetedQueryPlanner(FacetedSearchType type) {
        facetedConfig = new FacetedConfiguration();
        facetedConfig.setType(type);
    }
    
    public FacetedQueryPlanner() {
        this(FacetedSearchType.DAY_COUNT);
    }
    
    public FacetedQueryPlanner(final FacetedConfiguration config) {
        facetedConfig = config;
    }
    
    @Override
    public IteratorSetting getQueryIterator(MetadataHelper metadataHelper, RefactoredShardQueryConfiguration config, Query settings, String queryString,
                    Boolean isFullTable) throws DatawaveQueryException {
        if (isFullTable) {
            QueryException qe = new QueryException(DatawaveErrorCode.FULL_TABLE_SCAN_DISALLOWED);
            throw new FullTableScansDisallowedException(qe);
        }
        
        IteratorSetting cfg = super.getQueryIterator(metadataHelper, config, settings, queryString, isFullTable);
        if (!usePrecomputedFacets)
            cfg.setIteratorClass(DynamicFacetIterator.class.getName());
        else {
            config.setShardTableName("FacetsNating");
            cfg.setIteratorClass(FacetedTableIterator.class.getName());
        }
        
        cfg.addOption(DynamicFacetIterator.FACETED_SEARCH_TYPE, facetedConfig.getType().toString());
        cfg.addOption(DynamicFacetIterator.FACETED_MINIMUM, Integer.valueOf(facetedConfig.getMinimumFacetCount()).toString());
        cfg.addOption(DynamicFacetIterator.FACETED_SEARCH_FIELDS, Joiner.on(",").join(facetedConfig.getFacetedFields()));
        
        if (log.isTraceEnabled())
            log.trace("Configuration is " + facetedConfig);
        
        return cfg;
    }
    
    @Override
    public Tuple2<CloseableIterable<QueryPlan>,Boolean> getQueryRanges(ScannerFactory scannerFactory, MetadataHelper metadataHelper,
                    RefactoredShardQueryConfiguration config, JexlNode queryTree) throws DatawaveQueryException {
        if (usePrecomputedFacets) {
            config.setBypassExecutabilityCheck();
            FacetQueryPlanVisitor visitor = new FacetQueryPlanVisitor(config, metadataHelper, facetedConfig.getFacetedFields());
            queryTree.jjtAccept(visitor, null);
            return new Tuple2<CloseableIterable<QueryPlan>,Boolean>(visitor, false);
            
        } else {
            return new Tuple2<CloseableIterable<QueryPlan>,Boolean>(this.getFullScanRange(config, queryTree), false);
        }
        
    }
    
    @Override
    protected ASTJexlScript updateQueryTree(ScannerFactory scannerFactory, MetadataHelper metadataHelper, DateIndexHelper dateIndexHelper,
                    RefactoredShardQueryConfiguration config, String query, QueryData queryData, Query settings) throws DatawaveQueryException {
        // we want all terms expanded (except when max terms is reached)
        config.setExpandAllTerms(true);
        
        // update the query tree
        ASTJexlScript script = super.updateQueryTree(scannerFactory, metadataHelper, dateIndexHelper, config, query, queryData, settings);
        
        return script;
    }
    
    @Override
    protected ASTJexlScript limitQueryTree(ASTJexlScript script, RefactoredShardQueryConfiguration config) throws NoResultsException {
        // Assert that all of the terms in the query are indexed (so we can completely use the field index)
        // Also removes any spurious _ANYFIELD_ nodes left in from upstream
        try {
            switch (facetedConfig.getType()) {
                case DAY_COUNT:
                case SHARD_COUNT:
                    return script;
                default:
                    if (!isPrecomputedFacet(script, config)) {
                        return AllTermsIndexedVisitor.isIndexed(script, config, metadataHelper);
                    } else {
                        usePrecomputedFacets = true;
                        return script;
                    }
            }
        } catch (CannotExpandUnfieldedTermFatalException e) {
            throw new NoResultsException(e);
        }
    }
    
    protected boolean isPrecomputedFacet(ASTJexlScript script, RefactoredShardQueryConfiguration config) throws NoResultsException {
        // Assert that all of the terms in the query are indexed (so we can completely use the field index)
        // Also removes any spurious _ANYFIELD_ nodes left in from upstream
        try {
            FacetCheck check = new FacetCheck(config, metadataHelper);
            if (null != script.jjtAccept(check, null))
                return true;
        } catch (RuntimeException e) {
            return false;
        }
        return false;
    }
    
    /**
     * @param facetedConfig
     */
    public void setConfiguration(FacetedConfiguration facetedConfig) {
        this.facetedConfig = facetedConfig;
    }
    
    public FacetedConfiguration getConfiguration() {
        return facetedConfig;
    }
}
