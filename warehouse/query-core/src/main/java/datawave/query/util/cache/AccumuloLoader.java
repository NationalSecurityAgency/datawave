package datawave.query.util.cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map.Entry;
import java.util.Set;

import datawave.ingest.util.cache.Loader;
import datawave.security.util.ScannerHelper;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

/**
 * Description: Accumulo loading mechanism using the guava caches
 * 
 * Design: Based on Loader, which is further based on the Loader mechanism designed by Google
 * 
 * Purpose: Enables us to load elements from accumulo using the guava cache
 */
public abstract class AccumuloLoader<K,V> extends Loader<K,V> {
    
    private static final Logger log = Logger.getLogger(AccumuloLoader.class);
    
    /**
     * Our connector.
     */
    protected AccumuloClient client;
    /**
     * Table we are pulling data from.
     */
    protected String tableName;
    /**
     * Authorizations for the user performing the load.
     */
    protected Set<Authorizations> auths;
    /**
     * List of column families to pull
     */
    protected Collection<Text> columnFamilyList;
    
    protected AccumuloLoader(AccumuloClient client, String tableName, Set<Authorizations> auths, Collection<Text> columnFamilyList) {
        
        super();
        this.client = client;
        this.tableName = tableName;
        this.auths = auths;
        this.columnFamilyList = new ArrayList<>(columnFamilyList);
        
        if (log.isDebugEnabled())
            log.debug("Initializing");
    }
    
    /**
     * Builds the range for our lookup.
     * 
     * @param key
     * @return
     */
    protected abstract Range buildRange(K key);
    
    /**
     * Retrieves the column families for the provided key.
     * 
     * @param key
     * @return
     */
    protected Collection<Text> getColumnFamilies(K key) {
        return columnFamilyList;
    }
    
    @Override
    protected void build(K key) throws TableNotFoundException {
        build(key, tableName);
    }
    
    /**
     * Builds the key using the provided table name, so that we load the data into our cache.
     * 
     * @param key
     * @param myTableName
     * @throws TableNotFoundException
     */
    protected void build(K key, String myTableName) throws TableNotFoundException {
        if (log.isDebugEnabled())
            log.debug("Building cache from accumulo");
        synchronized (entryCache) {
            try (Scanner scanner = ScannerHelper.createScanner(client, myTableName, auths)) {
                Range range = null;
                if (key == null) {
                    if (log.isDebugEnabled())
                        log.debug("Key is null, infinite range");
                    range = new Range();
                } else
                    range = buildRange(key);
                
                scanner.setRange(range);
                
                for (Text cf : getColumnFamilies(key)) {
                    if (log.isDebugEnabled())
                        log.debug("== Fetching CF " + cf);
                    scanner.fetchColumnFamily(cf);
                }
                
                for (Entry<Key,Value> entry : scanner) {
                    
                    if (!store(key, entry.getKey(), entry.getValue())) {
                        log.warn("Did not accept " + entry.getKey());
                    }
                    
                }
            }
        }
    }
    
    /**
     * Store the key/value pair and the key into the underlying cache
     * 
     * @param key
     * @param accKey
     * @param accValue
     * @return
     */
    protected abstract boolean store(K key, Key accKey, Value accValue);
    
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof AccumuloLoader) {
            AccumuloLoader loaderObj = AccumuloLoader.class.cast(obj);
            if (client.instanceOperations().getInstanceID().equals(loaderObj.client.instanceOperations().getInstanceID())) {
                if (tableName.equals(loaderObj.tableName))
                    return true;
            }
        }
        return false;
    }
    
    @Override
    public int hashCode() {
        return tableName.hashCode() + 31 + client.instanceOperations().getInstanceID().hashCode();
    }
    
}
