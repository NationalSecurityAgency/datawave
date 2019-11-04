package datawave.query.jexl.lookups;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Semaphore;

import datawave.query.Constants;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.tables.ScannerFactory;
import datawave.query.tables.ScannerSession;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class FieldNameLookup extends IndexLookup {
    private static final Logger log = Logger.getLogger(FieldNameLookup.class);
    /**
     * Terms to lookup in index
     */
    protected Set<String> terms;
    /**
     * Field names that we limit to.
     */
    protected Set<Text> fields;
    
    protected Set<String> typeFilterSet = Sets.newHashSet();
    
    protected Semaphore sem = new Semaphore(1);
    
    protected IndexLookupMap fieldToTerms = null;
    
    public FieldNameLookup(Set<String> fields, Set<String> terms) {
        this.fields = new HashSet<>();
        
        if (fields != null) {
            for (String field : fields) {
                this.fields.add(new Text(field));
            }
            
        }
        
        this.terms = new HashSet<>(terms);
    }
    
    public void setTypeFilterSet(Set<String> limitSet) {
        typeFilterSet.addAll(limitSet);
    }
    
    public boolean supportReference() {
        return true;
    }
    
    @Override
    public IndexLookupMap lookup(ShardQueryConfiguration config, ScannerFactory scannerFactory, long lookupTimer) {
        try {
            sem.acquire();
        } catch (InterruptedException e1) {
            throw new RuntimeException(e1);
        }
        if (null != fieldToTerms) {
            return fieldToTerms;
        }
        
        fieldToTerms = new IndexLookupMap(config.getMaxUnfieldedExpansionThreshold(), config.getMaxValueExpansionThreshold());
        
        final Text holder = new Text();
        
        Iterator<Entry<Key,Value>> iter = Iterators.emptyIterator();
        
        Collection<ScannerSession> sessions = Lists.newArrayList();
        
        ScannerSession bs = null;
        
        try {
            
            if (!fields.isEmpty()) {
                for (String term : terms) {
                    
                    Set<Range> ranges = Collections.singleton(ShardIndexQueryTableStaticMethods.getLiteralRange(term));
                    if (limitToTerms) {
                        log.trace("Creating configureTermMatchOnly");
                        bs = ShardIndexQueryTableStaticMethods.configureTermMatchOnly(config, scannerFactory, config.getIndexTableName(), ranges,
                                        Collections.singleton(term), Collections.emptySet(), false, true);
                    } else {
                        log.trace("Creating configureLimitedDiscovery");
                        bs = ShardIndexQueryTableStaticMethods.configureLimitedDiscovery(config, scannerFactory, config.getIndexTableName(), ranges,
                                        Collections.singleton(term), Collections.emptySet(), false, true);
                    }
                    /**
                     * Fetch the limited field names for the given rows
                     */
                    for (Text field : fields) {
                        bs.getOptions().fetchColumnFamily(field);
                    }
                    
                    sessions.add(bs);
                    
                    iter = Iterators.concat(iter, bs);
                }
            }
            
            if (iter != null) {
                
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
                    
                    if (!typeFilterSet.isEmpty()) {
                        try {
                            String dataType = holder.toString().split(Constants.NULL)[1];
                            if (!typeFilterSet.contains(dataType))
                                continue;
                        } catch (Exception e) {
                            // skip the bad key
                            continue;
                        }
                    }
                    // We are only returning a mapping of field name to field value, no need to
                    // determine cardinality and such at this point.
                    fieldToTerms.put(colfam, row);
                    
                    // if we passed the term expansion threshold, then simply return
                    if (fieldToTerms.isKeyThresholdExceeded()) {
                        return fieldToTerms;
                    }
                }
            }
        } catch (TableNotFoundException e) {
            log.error(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            
            sem.release();
            
            for (ScannerSession session : sessions) {
                scannerFactory.close(session);
            }
        }
        
        return fieldToTerms;
    }
}
