package nsa.datawave.query.rewrite.planner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import nsa.datawave.query.rewrite.CloseableIterable;
import nsa.datawave.query.rewrite.config.RefactoredShardQueryConfiguration;
import nsa.datawave.query.rewrite.exceptions.DatawaveFatalQueryException;
import nsa.datawave.query.rewrite.exceptions.DatawaveQueryException;
import nsa.datawave.query.rewrite.jexl.visitors.BooleanOptimizationRebuildingVisitor;
import nsa.datawave.query.rewrite.jexl.visitors.JexlStringBuildingVisitor;
import nsa.datawave.query.rewrite.jexl.visitors.PrintingVisitor;
import nsa.datawave.query.rewrite.jexl.visitors.TreeFlatteningRebuildingVisitor;
import nsa.datawave.query.rewrite.jexl.visitors.TreeWrappingRebuildingVisitor;
import nsa.datawave.query.rewrite.util.QueryStopwatch;
import nsa.datawave.util.time.TraceStopwatch;
import nsa.datawave.query.tables.ScannerFactory;
import nsa.datawave.query.util.DateIndexHelper;
import nsa.datawave.query.util.MetadataHelper;
import nsa.datawave.webservice.query.Query;
import nsa.datawave.webservice.query.configuration.QueryData;

import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.google.common.collect.Lists;

public class BooleanChunkingQueryPlanner extends DefaultQueryPlanner {
    private static final Logger log = Logger.getLogger(BooleanChunkingQueryPlanner.class);
    
    public BooleanChunkingQueryPlanner(BooleanChunkingQueryPlanner booleanChunkingQueryPlanner) {
        super(booleanChunkingQueryPlanner);
    }
    
    @Override
    protected ASTJexlScript updateQueryTree(ScannerFactory scannerFactory, MetadataHelper metadataHelper, DateIndexHelper dateIndexHelper,
                    RefactoredShardQueryConfiguration config, String query, QueryData queryData, Query settings) throws DatawaveQueryException {
        ASTJexlScript queryTree = super.updateQueryTree(scannerFactory, metadataHelper, dateIndexHelper, config, query, queryData, settings);
        
        if (queryTree == null) {
            return null;
        }
        
        final QueryStopwatch timer = config.getTimers();
        
        TraceStopwatch stopwatch = timer.newStartedStopwatch("BooleanChunkingQueryPlanner - Flatten duplicate nodes in query");
        
        queryTree = TreeFlatteningRebuildingVisitor.flatten(queryTree);
        
        if (log.isDebugEnabled() && queryTree != null) {
            log.debug("Query after flattening tree");
            for (String line : PrintingVisitor.formattedQueryStringList(queryTree)) {
                log.debug(line);
            }
            log.debug("");
        }
        
        stopwatch.stop();
        stopwatch = timer.newStartedStopwatch("BooleanChunkingQueryPlanner - Lift OR nodes in query tree");
        
        queryTree = BooleanOptimizationRebuildingVisitor.optimize(queryTree);
        
        if (log.isDebugEnabled() && queryTree != null) {
            log.debug("Query after optimizing boolean logic");
            for (String line : PrintingVisitor.formattedQueryStringList(queryTree)) {
                log.debug(line);
            }
            log.debug("");
        }
        
        stopwatch.stop();
        stopwatch = timer.newStartedStopwatch("BooleanChunkingQueryPlanner - Apply parenthesis to the new OR nodes");
        
        queryTree = TreeWrappingRebuildingVisitor.wrap(queryTree);
        
        if (log.isDebugEnabled() && queryTree != null) {
            log.debug("Query after wrapping tree");
            for (String line : PrintingVisitor.formattedQueryStringList(queryTree)) {
                log.debug(line);
            }
            log.debug("");
        }
        
        stopwatch.stop();
        
        return queryTree;
    }
    
    @Override
    protected CloseableIterable<QueryData> process(ScannerFactory scannerFactory, MetadataHelper metadataHelper, DateIndexHelper dateIndexHelper,
                    RefactoredShardQueryConfiguration config, String query, Query settings) throws DatawaveQueryException {
        final QueryData queryData = new QueryData();
        final ArrayList<QueryData> data = Lists.newArrayList();
        
        ASTJexlScript queryTree = updateQueryTree(scannerFactory, metadataHelper, dateIndexHelper, config, query, queryData, settings);
        if (queryTree == null) {
            return DefaultQueryPlanner.emptyCloseableIterator();
        }
        
        String newQueryString = JexlStringBuildingVisitor.buildQuery(queryTree);
        
        if (StringUtils.isBlank(newQueryString)) {
            throw new DatawaveFatalQueryException("Query string after query modification was empty. Cannot run query.");
        } else if (log.isDebugEnabled()) {
            log.debug("Final query passed to QueryIterator:");
            log.debug(newQueryString);
        }
        
        queryData.setQuery(newQueryString);
        data.add(queryData);
        
        return new CloseableIterable<QueryData>() {
            @Override
            public Iterator<QueryData> iterator() {
                return data.iterator();
            }
            
            @Override
            public void close() throws IOException {}
        };
    }
    
    @Override
    public BooleanChunkingQueryPlanner clone() {
        return new BooleanChunkingQueryPlanner(this);
    }
}
