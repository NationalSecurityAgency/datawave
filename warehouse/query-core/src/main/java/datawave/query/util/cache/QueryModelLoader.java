package datawave.query.util.cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;

import datawave.query.model.QueryModel;

import datawave.util.StringUtils;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.google.common.collect.Maps;

/**
 * 
 */
public class QueryModelLoader extends AccumuloLoader<Entry<String,String>,Entry<QueryModel,Set<String>>> {
    
    private static final Logger log = Logger.getLogger(NormalizerLoader.class);
    
    protected Set<String> allFields = null;
    
    /**
     * Fetch a query model loader without a known set of fields
     *
     * @param client
     *            a client
     * @param tableName
     *            the table name
     * @param auths
     *            set of auths
     */
    public QueryModelLoader(AccumuloClient client, String tableName, Set<Authorizations> auths) {
        this(client, tableName, auths, null);
    }

    public QueryModelLoader(AccumuloClient client, String tableName, Set<Authorizations> auths, Set<String> allFields) {
        super(client, tableName, auths, new ArrayList<>());
        
        this.allFields = allFields;
        
        if (log.isDebugEnabled())
            log.debug("Initializing");
        
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see datawave.util.cache.AccumuloLoader#buildRange(java.lang.Object)
     */
    @Override
    protected Range buildRange(Entry<String,String> key) {
        
        return new Range();
    }
    
    @Override
    protected void build(Entry<String,String> key) throws TableNotFoundException {
        if (null != key) {
            super.build(key, key.getKey());
        }
    }
    
    @Override
    protected Collection<Text> getColumnFamilies(Entry<String,String> key) {
        
        Collection<Text> arr = new ArrayList<>();
        arr.add(new Text(key.getValue()));
        return arr;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.google.common.cache.CacheLoader#load(java.lang.Object)
     */
    @Override
    public Entry<QueryModel,Set<String>> load(Entry<String,String> key) throws Exception {
        
        Entry<QueryModel,Set<String>> queryModelEntry = entryCache.get(key);
        if (null == queryModelEntry) {
            build(key);
            
            queryModelEntry = entryCache.get(key);
        }
        
        return queryModelEntry;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see datawave.util.cache.AccumuloLoader#store(java.lang.Object, org.apache.accumulo.core.data.Key, org.apache.accumulo.core.data.Value)
     */
    @Override
    protected boolean store(Entry<String,String> storeKey, Key key, Value value) {
        if (log.isDebugEnabled())
            log.debug("== Requesting to store (" + storeKey.getKey() + "," + storeKey.getValue() + ") " + key);
        
        Set<String> indexOnlyFields = new HashSet<>(); // will hold index only fields
        // We need the entire Model so we can do both directions.
        
        Entry<QueryModel,Set<String>> modelEntry = entryCache.get(storeKey);
        
        QueryModel queryModel = null;
        
        if (null == modelEntry)
            queryModel = new QueryModel();
        else
            queryModel = modelEntry.getKey();
        
        if (null != key.getColumnQualifier()) {
            String original = key.getRow().toString();
            Text cq = key.getColumnQualifier();
            String[] parts = StringUtils.split(cq.toString(), "\0");
            if (parts.length > 1 && null != parts[0] && !parts[0].isEmpty()) {
                String replacement = parts[0];
                
                for (String part : parts) {
                    if ("forward".equalsIgnoreCase(part)) {
                        // Do *not* add a forward mapping entry
                        // when the replacement does not exist in the database
                        if (allFields == null || allFields.contains(replacement)) {
                            queryModel.addTermToModel(original, replacement);
                        } else if (log.isTraceEnabled()) {
                            log.trace("Ignoring forward mapping of " + replacement + " for " + original + " because the metadata table has no reference to it");
                            
                        }
                        
                    } else if ("reverse".equalsIgnoreCase(part)) {
                        queryModel.addTermToReverseModel(original, replacement);
                    } else if ("index_only".equalsIgnoreCase(part)) {
                        indexOnlyFields.add(replacement);
                    }
                }
            }
        }
        
        entryCache.put(storeKey, Maps.immutableEntry(queryModel, indexOnlyFields));
        
        return true;
        
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see datawave.query.util.cache.Loader#merge(java.lang.Object, java.lang.Object)
     */
    @Override
    protected void merge(Entry<String,String> key, Entry<QueryModel,Set<String>> value) throws Exception {
        throw new UnsupportedOperationException("Cannot nest in the query model cache");
    }
}
