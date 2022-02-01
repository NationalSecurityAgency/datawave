package datawave.query.jexl.lookups;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import datawave.query.Constants;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.tables.ScannerFactory;
import datawave.query.tables.ScannerSession;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.collections.map.HashedMap;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * An asynchronous index lookup which Looks up field names from the index which match the provided set of terms, and optionally limits them to the specified
 * fields
 */
public class FieldNameIndexLookup extends AsyncIndexLookup {
    private static final Logger log = Logger.getLogger(FieldNameIndexLookup.class);
    
    protected Set<String> terms;
    
    protected Future<Boolean> timedScanFuture;
    protected long lookupStartTimeMillis = Long.MAX_VALUE;
    protected CountDownLatch lookupStartedLatch;
    
    private final Collection<ScannerSession> sessions = Lists.newArrayList();
    private ExpandedFieldCache fieldCache = new ExpandedFieldCache(); // Todo - check if we already performed lookups for certain fields
    
    /**
     *
     * @param config
     *            the shard query configuration, not null
     * @param scannerFactory
     *            the scanner factory, not null
     * @param fields
     *            the fields to limit the lookup, may be null
     * @param terms
     *            the terms to match against, not null
     * @param execService
     *            the executor service, not null
     */
    public FieldNameIndexLookup(ShardQueryConfiguration config, ScannerFactory scannerFactory, Set<String> fields, Set<String> terms,
                    ExecutorService execService) {
        super(config, scannerFactory, true, execService);
        
        this.fields = new HashSet<>();
        if (fields != null) {
            this.fields.addAll(fields);
        }
        
        this.terms = new HashSet<>(terms);
    }
    
    @Override
    public void submit() {
        if (indexLookupMap == null) {
            indexLookupMap = new IndexLookupMap(config.getMaxUnfieldedExpansionThreshold(), config.getMaxValueExpansionThreshold());
            
            Iterator<Entry<Key,Value>> iter = Iterators.emptyIterator();
            Map<String,Collection<String>> alreadyExpandedTerms = new HashMap<>();
            ScannerSession bs;
            
            try {
                if (!fields.isEmpty()) {
                    for (String term : terms) {
                        // Check if this term has already been looked up against the index, and if so, pull the expansions from the cache rather than looking
                        // them up again.
                        if (fieldCache.containsExpansionsFor(term)) { // todo - confirm with Ivan that we should be checking against the expanded terms
                            alreadyExpandedTerms.put(term, fieldCache.getExpansions(term));
                        } else {
                            Set<Range> ranges = Collections.singleton(ShardIndexQueryTableStaticMethods.getLiteralRange(term));
                            if (config.getLimitAnyFieldLookups()) {
                                log.trace("Creating configureTermMatchOnly");
                                bs = ShardIndexQueryTableStaticMethods.configureTermMatchOnly(config, scannerFactory, config.getIndexTableName(), ranges,
                                                Collections.singleton(term), Collections.emptySet(), false, true);
                            } else {
                                log.trace("Creating configureLimitedDiscovery");
                                bs = ShardIndexQueryTableStaticMethods.configureLimitedDiscovery(config, scannerFactory, config.getIndexTableName(), ranges,
                                                Collections.singleton(term), Collections.emptySet(), false, true);
                            }
                            
                            // Fetch the limited field names for the given rows
                            for (String field : fields) {
                                bs.getOptions().fetchColumnFamily(new Text(field));
                            }
                            
                            sessions.add(bs);
                            
                            iter = Iterators.concat(iter, bs);
                            
                        }
                    }
                }
                timedScanFuture = execService.submit(createTimedCallable(iter, alreadyExpandedTerms));
                
            } catch (TableNotFoundException e) {
                log.error(e);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
    
    @Override
    public synchronized IndexLookupMap lookup() {
        if (!sessions.isEmpty()) {
            try {
                // for field name lookups, we wait indefinitely
                timedScanWait(timedScanFuture, lookupStartedLatch, lookupStartTimeMillis, Long.MAX_VALUE);
            } finally {
                for (ScannerSession sesh : sessions) {
                    scannerFactory.close(sesh);
                }
                sessions.clear();
            }
        }
        
        return indexLookupMap;
    }
    
    protected Callable<Boolean> createTimedCallable(final Iterator<Entry<Key,Value>> iter, Map<String,Collection<String>> alreadyExpandedTerms) {
        lookupStartedLatch = new CountDownLatch(1);
        
        return () -> {
            lookupStartTimeMillis = System.currentTimeMillis();
            lookupStartedLatch.countDown();
            
            final Text holder = new Text();
            
            try {
                while (iter.hasNext()) {
                    Entry<Key,Value> entry = iter.next();
                    if (log.isTraceEnabled()) {
                        log.trace("Index entry: " + entry.getKey());
                    }
                    
                    entry.getKey().getRow(holder);
                    String row = holder.toString();
                    
                    entry.getKey().getColumnFamily(holder);
                    String colfam = holder.toString();
                    
                    entry.getKey().getColumnQualifier(holder);
                    
                    if (config.getDatatypeFilter() != null && !config.getDatatypeFilter().isEmpty()) {
                        try {
                            String dataType = holder.toString().split(Constants.NULL)[1];
                            if (!config.getDatatypeFilter().contains(dataType))
                                continue;
                        } catch (Exception e) {
                            // skip the bad key
                            continue;
                        }
                    }
                    // We are only returning a mapping of field name to field value, no need to
                    // determine cardinality and such at this point.
                    indexLookupMap.put(colfam, row);
                    
                    // if we passed the term expansion threshold, then simply return
                    if (indexLookupMap.isKeyThresholdExceeded()) {
                        break;
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                for (ScannerSession session : sessions) {
                    scannerFactory.close(session);
                }
            }
            
            // Add the expansions that we have previously found
            for (String term : alreadyExpandedTerms.keySet()) {
                Collection<String> expandedTerms = alreadyExpandedTerms.get(term);
                indexLookupMap.putAll(term, expandedTerms);
            }
            
            return true;
        };
    }
}
