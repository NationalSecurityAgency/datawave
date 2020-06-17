package datawave.webservice.query.cache;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Multimap;
import org.apache.accumulo.core.trace.Span;

import javax.annotation.PostConstruct;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Provides caching for trace configurations as well as for our root query spans, so that they can be started/stopped across threads
 */
public class QueryTraceCache {
    private Cache<String,Multimap<String,PatternWrapper>> patternCache;
    private Cache<String,Span> spanCache;
    private final List<CacheListener> patternListeners = new ArrayList<>();
    
    @PostConstruct
    private void init() {
        patternCache = CacheBuilder.newBuilder().build();
        spanCache = CacheBuilder.newBuilder().build();
    }
    
    public boolean containsKey(String id) {
        return patternCache.asMap().containsKey(id);
    }
    
    public Multimap<String,PatternWrapper> get(String id) {
        return patternCache.getIfPresent(id);
    }
    
    public Span getSpan(String queryId) {
        return spanCache.getIfPresent(queryId);
    }
    
    public void put(String id, Multimap<String,PatternWrapper> traceInfo) {
        patternCache.put(id, traceInfo);
        synchronized (patternListeners) {
            for (CacheListener listener : patternListeners)
                listener.cacheEntryModified(id, traceInfo);
        }
    }
    
    public void put(String queryId, Span span) {
        spanCache.put(queryId, span);
    }
    
    public Multimap<String,PatternWrapper> putIfAbsent(String id, Multimap<String,PatternWrapper> traceInfo) {
        return patternCache.asMap().putIfAbsent(id, traceInfo);
    }
    
    public void remove(String id) {
        patternCache.invalidate(id);
    }
    
    public void removeSpan(String queryId) {
        spanCache.invalidate(queryId);
    }
    
    public void clear() {
        patternCache.asMap().clear();
        spanCache.asMap().clear();
    }
    
    public void addListener(CacheListener listener) {
        synchronized (patternListeners) {
            patternListeners.add(listener);
        }
    }
    
    public void removeListener(CacheListener listener) {
        synchronized (patternListeners) {
            patternListeners.remove(listener);
        }
    }
    
    public interface CacheListener {
        void cacheEntryModified(String key, Multimap<String,PatternWrapper> traceInfo);
    }
    
    /**
     * A simple wrapper around the Pattern class that allows it to be used in hash tables with equality checking based on the string version of the pattern. We
     * do this so that we can have the more efficient pre-compiled patterns, but still store them in a hash table.
     */
    public static class PatternWrapper implements Serializable {
        private static final long serialVersionUID = 1L;
        private final Pattern pattern;
        
        public static PatternWrapper wrap(String regex) {
            return regex == null ? null : new PatternWrapper(regex);
        }
        
        public PatternWrapper(String regex) {
            pattern = Pattern.compile(regex);
        }
        
        public boolean matches(String text) {
            return pattern.matcher(text).matches();
        }
        
        @Override
        public String toString() {
            return "PatternWrapper [pattern=" + pattern + "]";
        }
        
        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((pattern == null) ? 0 : pattern.pattern().hashCode());
            return result;
        }
        
        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            PatternWrapper other = (PatternWrapper) obj;
            if (pattern == null) {
                if (other.pattern != null)
                    return false;
            } else if (!pattern.pattern().equals(other.pattern.pattern()))
                return false;
            return true;
        }
    }
}
