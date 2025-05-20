package datawave.query.planner;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.google.common.collect.Lists;

import datawave.core.query.configuration.QueryData;
import datawave.microservice.query.Query;
import datawave.query.CloseableIterable;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.exceptions.DatawaveQueryException;
import datawave.query.jexl.visitors.BooleanOptimizationRebuildingVisitor;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.query.jexl.visitors.PrintingVisitor;
import datawave.query.jexl.visitors.TreeFlatteningRebuildingVisitor;
import datawave.query.jexl.visitors.TreeWrappingRebuildingVisitor;
import datawave.query.tables.ScannerFactory;
import datawave.query.util.DateIndexHelper;
import datawave.query.util.MetadataHelper;
import datawave.query.util.QueryStopwatch;
import datawave.util.time.TraceStopwatch;

public class BooleanChunkingQueryPlanner extends DefaultQueryPlanner {
    private static final Logger log = Logger.getLogger(BooleanChunkingQueryPlanner.class);

    public BooleanChunkingQueryPlanner(BooleanChunkingQueryPlanner booleanChunkingQueryPlanner) {
        super(booleanChunkingQueryPlanner);
    }

    @Override
    protected ASTJexlScript updateQueryTree(ScannerFactory scannerFactory, MetadataHelper metadataHelper, DateIndexHelper dateIndexHelper,
                    ShardQueryConfiguration config, String query, Query settings) throws DatawaveQueryException {
        ASTJexlScript queryTree = super.updateQueryTree(scannerFactory, metadataHelper, dateIndexHelper, config, query, settings);

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
                    ShardQueryConfiguration config, String query, Query settings) throws DatawaveQueryException {
        ASTJexlScript queryTree = updateQueryTree(scannerFactory, metadataHelper, dateIndexHelper, config, query, settings);
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

        final ArrayList<QueryData> data = Lists.newArrayList();
        final QueryData queryData = new QueryData().withQuery(newQueryString);
        data.add(queryData);

        return new CloseableIterable<>() {
            @Override
            public Iterator<QueryData> iterator() {
                return data.iterator();
            }

            @Override
            public void close() {}
        };
    }

    @Override
    public BooleanChunkingQueryPlanner clone() {
        return new BooleanChunkingQueryPlanner(this);
    }
}
