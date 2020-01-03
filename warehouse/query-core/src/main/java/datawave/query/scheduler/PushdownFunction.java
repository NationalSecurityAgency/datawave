package datawave.query.scheduler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import datawave.query.config.ShardQueryConfiguration;
import datawave.query.planner.QueryPlan;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.Locations;
import org.apache.accumulo.core.data.TabletId;
import org.apache.accumulo.core.data.Range;
import org.apache.commons.jexl2.parser.ParseException;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.query.tables.SessionOptions;
import datawave.query.tables.async.ScannerChunk;
import datawave.webservice.query.configuration.QueryData;

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
     * Set of query plans
     */
    protected Set<Integer> queryPlanSet;
    protected Collection<IteratorSetting> customSettings;
    
    protected String tableName;
    
    public PushdownFunction(ShardQueryConfiguration config, Collection<IteratorSetting> settings, String tableName) {
        this.config = config;
        queryPlanSet = Sets.newHashSet();
        this.customSettings = settings;
        this.tableName = tableName;
    }
    
    public List<ScannerChunk> apply(QueryData qd) {
        Multimap<String,QueryPlan> serverPlan = ArrayListMultimap.create();
        List<ScannerChunk> chunks = Lists.newArrayList();
        try {
            
            redistributeQueries(serverPlan, new QueryPlan(qd));
            
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
                        
                        chunks.add(new ScannerChunk(options, Lists.newArrayList(plan.getRanges()), server));
                        
                    } catch (Exception e) {
                        log.error(e);
                        throw new AccumuloException(e);
                    }
                }
            }
            
        } catch (AccumuloException | AccumuloSecurityException | TableNotFoundException | ParseException e) {
            throw new RuntimeException(e);
        }
        return chunks;
    }
    
    protected void redistributeQueries(Multimap<String,QueryPlan> serverPlan, QueryPlan currentPlan) throws AccumuloException, AccumuloSecurityException,
                    TableNotFoundException {
        
        List<Range> ranges = Lists.newArrayList(currentPlan.getRanges());
        if (!ranges.isEmpty()) {
            Map<String,Map<TabletId,List<Range>>> binnedRanges = binRanges(ranges);
            
            for (String server : binnedRanges.keySet()) {
                Map<TabletId,List<Range>> hostedExtentMap = binnedRanges.get(server);
                
                Iterable<Range> rangeIter = Lists.newArrayList();
                
                for (Entry<TabletId,List<Range>> rangeEntry : hostedExtentMap.entrySet()) {
                    if (log.isTraceEnabled())
                        log.trace("Adding range from " + rangeEntry.getValue());
                    rangeIter = Iterables.concat(rangeIter, rangeEntry.getValue());
                }
                
                if (log.isTraceEnabled())
                    log.trace("Adding query tree " + JexlStringBuildingVisitor.buildQuery(currentPlan.getQueryTree()) + " " + currentPlan.getSettings().size()
                                    + " for " + server);
                
                serverPlan.put(server, new QueryPlan(currentPlan.getQueryTree(), rangeIter, currentPlan.getSettings(), currentPlan.getColumnFamilies()));
                
            }
        }
        
    }
    
    protected Map<String,Map<TabletId,List<Range>>> binRanges(List<Range> ranges) throws AccumuloException, AccumuloSecurityException, TableNotFoundException {
        Map<String,Map<TabletId,List<Range>>> binnedRanges = new HashMap<>();
        Locations locations = config.getClient().tableOperations().locate(tableName, ranges);
        Map<TabletId,List<Range>> tabletToRange = locations.groupByTablet();
        for (TabletId tid : tabletToRange.keySet()) {
            binnedRanges.put(locations.getTabletLocation(tid), tabletToRange);
        }
        
        // truncate the ranges to within the tablets... this makes it easier
        // to know what work
        // needs to be redone when failures occurs and tablets have merged
        // or split
        Map<String,Map<TabletId,List<Range>>> binnedRanges2 = new HashMap<>();
        for (Entry<String,Map<TabletId,List<Range>>> entry : binnedRanges.entrySet()) {
            Map<TabletId,List<Range>> tabletMap = new HashMap<>();
            binnedRanges2.put(entry.getKey(), tabletMap);
            for (Entry<TabletId,List<Range>> tabletRanges : entry.getValue().entrySet()) {
                Range tabletRange = tabletRanges.getKey().toRange();
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
