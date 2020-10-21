package datawave.iterators;

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.core.data.thrift.IterInfo;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.IteratorUtil.IterInfoComparator;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.system.SynchronizedIterator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IteratorUtils {
    
    private static final Logger LOG = LoggerFactory.getLogger(IteratorUtils.class);
    
    /**
     * Fetch the correct configuration key prefix for the given scope. Throws an IllegalArgumentException if no property exists for the given scope.
     */
    private static Property getProperty(IteratorScope scope) {
        requireNonNull(scope);
        switch (scope) {
            case scan:
                return Property.TABLE_ITERATOR_SCAN_PREFIX;
            case minc:
                return Property.TABLE_ITERATOR_MINC_PREFIX;
            case majc:
                return Property.TABLE_ITERATOR_MAJC_PREFIX;
            default:
                throw new IllegalStateException("Could not find configuration property for IteratorScope");
        }
    }
    
    private static void parseIteratorConfiguration(IteratorScope scope, List<IterInfo> iters, Map<String,Map<String,String>> ssio,
                    Map<String,Map<String,String>> allOptions, AccumuloConfiguration conf) {
        parseIterConf(scope, iters, allOptions, conf);
        
        mergeOptions(ssio, allOptions);
    }
    
    private static void mergeOptions(Map<String,Map<String,String>> ssio, Map<String,Map<String,String>> allOptions) {
        for (Entry<String,Map<String,String>> entry : ssio.entrySet()) {
            if (entry.getValue() == null)
                continue;
            Map<String,String> options = allOptions.get(entry.getKey());
            if (options == null) {
                allOptions.put(entry.getKey(), entry.getValue());
            } else {
                options.putAll(entry.getValue());
            }
        }
    }
    
    private static void parseIterConf(IteratorScope scope, List<IterInfo> iters, Map<String,Map<String,String>> allOptions, AccumuloConfiguration conf) {
        final Property scopeProperty = getProperty(scope);
        final String scopePropertyKey = scopeProperty.getKey();
        
        for (Entry<String,String> entry : conf.getAllPropertiesWithPrefix(scopeProperty).entrySet()) {
            String suffix = entry.getKey().substring(scopePropertyKey.length());
            String suffixSplit[] = suffix.split("\\.", 3);
            
            if (suffixSplit.length == 1) {
                String sa[] = entry.getValue().split(",");
                int prio = Integer.parseInt(sa[0]);
                String className = sa[1];
                iters.add(new IterInfo(prio, className, suffixSplit[0]));
            } else if (suffixSplit.length == 3 && suffixSplit[1].equals("opt")) {
                String iterName = suffixSplit[0];
                String optName = suffixSplit[2];
                
                Map<String,String> options = allOptions.get(iterName);
                if (options == null) {
                    options = new HashMap<>();
                    allOptions.put(iterName, options);
                }
                
                options.put(optName, entry.getValue());
                
            } else {
                throw new IllegalArgumentException("Invalid iterator format: " + entry.getKey());
            }
        }
        
        Collections.sort(iters, new IterInfoComparator());
    }
    
    @SuppressWarnings("unchecked")
    public static <K extends WritableComparable<?>,V extends Writable> SortedKeyValueIterator<K,V> loadIterators(IteratorScope scope,
                    SortedKeyValueIterator<K,V> source, KeyExtent extent, AccumuloConfiguration conf, List<IterInfo> ssiList,
                    Map<String,Map<String,String>> ssio, IteratorEnvironment env) throws IOException {
        
        List<IterInfo> iters = new ArrayList<>(ssiList);
        Map<String,Map<String,String>> allOptions = new HashMap<>();
        parseIteratorConfiguration(scope, iters, ssio, allOptions, conf);
        // wrap the source in a SynchronizedIterator in case any of the additional configured iterators
        // want to use threading
        SortedKeyValueIterator<K,V> prev = new SynchronizedIterator<>(source);
        
        try {
            for (IterInfo iterInfo : iters) {
                
                Class<? extends SortedKeyValueIterator<K,V>> clazz = (Class<? extends SortedKeyValueIterator<K,V>>) Class.forName(iterInfo.className)
                                .asSubclass(SortedKeyValueIterator.class);
                
                SortedKeyValueIterator<K,V> skvi = clazz.newInstance();
                Map<String,String> options = allOptions.get(iterInfo.iterName);
                if (options == null)
                    options = Collections.emptyMap();
                skvi.init(prev, options, env);
                prev = skvi;
            }
        } catch (ClassNotFoundException e) {
            LOG.error(e.toString());
            throw new RuntimeException(e);
        } catch (InstantiationException e) {
            LOG.error(e.toString());
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            LOG.error(e.toString());
            throw new RuntimeException(e);
        }
        return prev;
    }
    
}
