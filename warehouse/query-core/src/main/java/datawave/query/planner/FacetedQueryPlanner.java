package datawave.query.planner;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.log4j.Logger;

import com.google.common.base.Joiner;

import datawave.microservice.query.Query;
import datawave.query.CloseableIterable;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.exceptions.DatawaveQueryException;
import datawave.query.exceptions.EmptyUnfieldedTermExpansionException;
import datawave.query.exceptions.FullTableScansDisallowedException;
import datawave.query.exceptions.NoResultsException;
import datawave.query.iterator.facets.DynamicFacetIterator;
import datawave.query.iterator.facets.FacetedTableIterator;
import datawave.query.jexl.visitors.AllTermsIndexedVisitor;
import datawave.query.tables.ScannerFactory;
import datawave.query.tables.facets.FacetCheck;
import datawave.query.tables.facets.FacetQueryPlanVisitor;
import datawave.query.tables.facets.FacetedConfiguration;
import datawave.query.tables.facets.FacetedSearchType;
import datawave.query.util.DateIndexHelper;
import datawave.query.util.MetadataHelper;
import datawave.query.util.Tuple2;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;

public class FacetedQueryPlanner extends IndexQueryPlanner {

    protected FacetedConfiguration facetedConfig;

    private static final Logger log = Logger.getLogger(FacetedQueryPlanner.class);

    boolean usePrecomputedFacets = false;

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
    public IteratorSetting getQueryIterator(MetadataHelper metadataHelper, ShardQueryConfiguration config, String queryString, Boolean isFullTable,
                    boolean isPreload) throws DatawaveQueryException {

        if (isFullTable) {
            QueryException qe = new QueryException(DatawaveErrorCode.FULL_TABLE_SCAN_DISALLOWED);
            throw new FullTableScansDisallowedException(qe);
        }

        IteratorSetting cfg = super.getQueryIterator(metadataHelper, config, queryString, isFullTable, isPreload);
        if (!usePrecomputedFacets)
            cfg.setIteratorClass(DynamicFacetIterator.class.getName());
        else {
            config.setShardTableName(facetedConfig.getFacetTableName());
            cfg.setIteratorClass(FacetedTableIterator.class.getName());
        }

        cfg.addOption(DynamicFacetIterator.FACETED_SEARCH_TYPE, facetedConfig.getType().toString());
        cfg.addOption(DynamicFacetIterator.FACETED_MINIMUM, Integer.toString(facetedConfig.getMinimumFacetCount()));
        cfg.addOption(DynamicFacetIterator.FACETED_SEARCH_FIELDS, Joiner.on(",").join(facetedConfig.getFacetedFields()));

        if (log.isTraceEnabled())
            log.trace("Configuration is " + facetedConfig);

        return cfg;
    }

    @Override
    public Tuple2<CloseableIterable<QueryPlan>,Boolean> getQueryRanges(ScannerFactory scannerFactory, MetadataHelper metadataHelper,
                    ShardQueryConfiguration config, JexlNode queryTree) throws DatawaveQueryException {
        if (usePrecomputedFacets) {
            config.setBypassExecutabilityCheck();
            FacetQueryPlanVisitor visitor = new FacetQueryPlanVisitor(config, facetedConfig, metadataHelper, facetedConfig.getFacetedFields());
            queryTree.jjtAccept(visitor, null);
            return new Tuple2<>(visitor, false);

        } else {
            return new Tuple2<>(this.getFullScanRange(config, queryTree), false);
        }

    }

    @Override
    protected ASTJexlScript updateQueryTree(ScannerFactory scannerFactory, MetadataHelper metadataHelper, DateIndexHelper dateIndexHelper,
                    ShardQueryConfiguration config, String query, Query settings) throws DatawaveQueryException {
        // we want all terms expanded (except when max terms is reached)
        config.setExpandAllTerms(true);

        // update the query tree

        return super.updateQueryTree(scannerFactory, metadataHelper, dateIndexHelper, config, query, settings);
    }

    @Override
    protected ASTJexlScript limitQueryTree(ASTJexlScript script, ShardQueryConfiguration config) throws NoResultsException {
        // Assert that all of the terms in the query are indexed (so we can completely use the field index)
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
        } catch (EmptyUnfieldedTermExpansionException e) {
            throw new NoResultsException(e);
        }
    }

    protected boolean isPrecomputedFacet(ASTJexlScript script, ShardQueryConfiguration config) throws NoResultsException {
        // Assert that all of the terms in the query are indexed (so we can completely use the field index)
        // Also removes any spurious _ANYFIELD_ nodes left in from upstream
        try {
            FacetCheck check = new FacetCheck(config, facetedConfig, metadataHelper);
            if (null != script.jjtAccept(check, null))
                return true;
        } catch (RuntimeException e) {
            return false;
        }
        return false;
    }

    public void setConfiguration(FacetedConfiguration facetedConfig) {
        this.facetedConfig = facetedConfig;
    }

    public FacetedConfiguration getConfiguration() {
        return facetedConfig;
    }
}
