package datawave.query.scheduler;

import com.google.common.base.Function;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.core.query.configuration.QueryData;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.query.planner.QueryPlan;
import datawave.query.tables.SessionOptions;
import datawave.query.tables.async.ScannerChunk;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableDeletedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.TableOfflineException;
import org.apache.accumulo.core.clientImpl.ClientContext;
import org.apache.accumulo.core.clientImpl.TabletLocator;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.manager.state.tables.TableState;
import org.apache.commons.jexl3.parser.ParseException;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PushdownFunction implements Function<QueryData,List<ScannerChunk>> {

    /**
     * Logger
     */
    private static final Logger log = Logger.getLogger(PushdownFunction.class);

    /**
     * Configuration object
     */
    private ShardQueryConfiguration config;

    /**
     * Tablet locator
     */
    private TabletLocator tl;

    /**
     * Set of query plans
     */
    protected Set<Integer> queryPlanSet;
    protected Collection<IteratorSetting> customSettings;

    protected TableId tableId;

    public PushdownFunction(TabletLocator tl, ShardQueryConfiguration config, Collection<IteratorSetting> settings, TableId tableId) {
        this.tl = tl;
        this.config = config;
        queryPlanSet = Sets.newHashSet();
        this.customSettings = settings;
        this.tableId = tableId;

    }

    public List<ScannerChunk> apply(QueryData qd) {
        Multimap<String,QueryPlan> serverPlan = ArrayListMultimap.create();
        List<ScannerChunk> chunks = Lists.newArrayList();
        try {

            redistributeQueries(serverPlan, tl, new QueryPlan(qd));

            for (String server : serverPlan.keySet()) {
                Collection<QueryPlan> plans = serverPlan.get(server);
                Set<QueryPlan> reducedSet = Sets.newHashSet(plans);
                for (QueryPlan plan : reducedSet) {
                    Integer hashCode = plan.hashCode();
                    if (queryPlanSet.contains(hashCode)) {
                        continue;
                    } else
                        queryPlanSet.clear();

                    queryPlanSet.add(hashCode);
                    try {

                        SessionOptions options = new SessionOptions();

                        if (log.isTraceEnabled()) {
                            log.trace("setting ranges" + plan.getRanges());
                            log.trace("range set size" + plan.getSettings().size());
                        }
                        for (IteratorSetting setting : plan.getSettings()) {
                            options.addScanIterator(setting);
                        }

                        for (IteratorSetting setting : customSettings) {
                            options.addScanIterator(setting);
                        }

                        for (String cf : plan.getColumnFamilies()) {
                            options.fetchColumnFamily(new Text(cf));
                        }

                        options.setQueryConfig(this.config);

                        chunks.add(new ScannerChunk(options, plan.getRanges(), qd, server));
                    } catch (Exception e) {
                        log.error(e);
                        throw new AccumuloException(e);
                    }
                }
            }

        } catch (AccumuloException e) {
            throw new RuntimeException(e);
        } catch (AccumuloSecurityException e) {
            throw new RuntimeException(e);
        } catch (TableNotFoundException e) {
            throw new RuntimeException(e);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        return chunks;
    }

    protected void redistributeQueries(Multimap<String,QueryPlan> serverPlan, TabletLocator tl, QueryPlan currentPlan)
                    throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
        List<Range> ranges = Lists.newArrayList(currentPlan.getRanges());
        if (!ranges.isEmpty()) {
            Map<String,Map<KeyExtent,List<Range>>> binnedRanges = binRanges(tl, config.getClient(), ranges);

            for (String server : binnedRanges.keySet()) {
                Map<KeyExtent,List<Range>> hostedExtentMap = binnedRanges.get(server);

                Iterable<Range> rangeIter = Lists.newArrayList();

                for (Map.Entry<KeyExtent,List<Range>> rangeEntry : hostedExtentMap.entrySet()) {
                    if (log.isTraceEnabled())
                        log.trace("Adding range from " + rangeEntry.getValue());
                    rangeIter = Iterables.concat(rangeIter, rangeEntry.getValue());
                }

                if (log.isTraceEnabled())
                    log.trace("Adding query tree " + JexlStringBuildingVisitor.buildQuery(currentPlan.getQueryTree()) + " " + currentPlan.getSettings().size()
                                    + " for " + server);

                serverPlan.put(server, new QueryPlan(currentPlan.getTableName(), currentPlan.getQueryTree(), rangeIter, currentPlan.getSettings(),
                                currentPlan.getColumnFamilies()));
            }
        }

    }

    protected Map<String,Map<KeyExtent,List<Range>>> binRanges(TabletLocator tl, AccumuloClient client, List<Range> ranges)
                    throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
        Map<String,Map<KeyExtent,List<Range>>> binnedRanges = new HashMap<>();

        int lastFailureSize = Integer.MAX_VALUE;

        while (true) {

            binnedRanges.clear();
            ClientContext ctx = AccumuloConnectionFactory.getClientContext(client);
            List<Range> failures = tl.binRanges(ctx, ranges, binnedRanges);

            if (!failures.isEmpty()) {
                // tried to only do table state checks when failures.size()
                // == ranges.size(), however this did
                // not work because nothing ever invalidated entries in the
                // tabletLocator cache... so even though
                // the table was deleted the tablet locator entries for the
                // deleted table were not cleared... so
                // need to always do the check when failures occur
                if (failures.size() >= lastFailureSize)
                    if (!ctx.tableNodeExists(tableId))
                        throw new TableDeletedException(tableId.canonical());
                    else if (ctx.getTableState(tableId) == TableState.OFFLINE)
                        throw new TableOfflineException("Table " + tableId + " is offline");

                lastFailureSize = failures.size();

                if (log.isTraceEnabled())
                    log.trace("Failed to bin " + failures.size() + " ranges, tablet locations were null, retrying in 100ms");
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            } else {
                break;
            }

        }

        // truncate the ranges to within the tablets... this makes it easier
        // to know what work
        // needs to be redone when failures occurs and tablets have merged
        // or split
        Map<String,Map<KeyExtent,List<Range>>> binnedRanges2 = new HashMap<>();
        for (Map.Entry<String,Map<KeyExtent,List<Range>>> entry : binnedRanges.entrySet()) {
            Map<KeyExtent,List<Range>> tabletMap = new HashMap<>();
            binnedRanges2.put(entry.getKey(), tabletMap);
            for (Map.Entry<KeyExtent,List<Range>> tabletRanges : entry.getValue().entrySet()) {
                Range tabletRange = tabletRanges.getKey().toDataRange();
                List<Range> clippedRanges = new ArrayList<>();
                tabletMap.put(tabletRanges.getKey(), clippedRanges);
                for (Range range : tabletRanges.getValue())
                    clippedRanges.add(tabletRange.clip(range));
            }
        }

        binnedRanges.clear();
        binnedRanges.putAll(binnedRanges2);

        return binnedRanges;
    }

}
