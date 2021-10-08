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
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

public class FieldNameIndexLookup extends IndexLookup {
    private static final Logger log = Logger.getLogger(FieldNameIndexLookup.class);
    
    /**
     * Terms to lookup in index
     */
    protected Set<String> terms;
    
    public FieldNameIndexLookup(ShardQueryConfiguration config, ScannerFactory scannerFactory, Set<String> fields, Set<String> terms) {
        super(config, scannerFactory, true);
        this.fields = fields;
        this.terms = new HashSet<>(terms);
    }
    
    @Override
    public synchronized IndexLookupMap lookup() {
        indexLookupMap = new IndexLookupMap(config.getMaxUnfieldedExpansionThreshold(), config.getMaxValueExpansionThreshold());
        
        final Text holder = new Text();
        
        Iterator<Entry<Key,Value>> iter = Iterators.emptyIterator();
        
        Collection<ScannerSession> sessions = Lists.newArrayList();
        
        ScannerSession bs;
        
        try {
            if (!fields.isEmpty()) {
                for (String term : terms) {
                    
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
                    /**
                     * Fetch the limited field names for the given rows
                     */
                    for (String field : fields) {
                        bs.getOptions().fetchColumnFamily(new Text(field));
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
            }
        } catch (TableNotFoundException e) {
            log.error(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            for (ScannerSession session : sessions) {
                scannerFactory.close(session);
            }
        }
        
        return indexLookupMap;
    }
}
