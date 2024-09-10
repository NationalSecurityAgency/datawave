package datawave.webservice.query.cache;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import javax.annotation.PostConstruct;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Multimap;

/**
 *
 */
public class QueryTraceCache {
    private Cache<String,Multimap<String,PatternWrapper>> cache;
    private final List<CacheListener> listeners = new ArrayList<>();

    @PostConstruct
    private void init() {
        cache = CacheBuilder.newBuilder().build();
    }

    public boolean containsKey(String id) {
        return cache.asMap().containsKey(id);
    }

    public Multimap<String,PatternWrapper> get(String id) {
        return cache.getIfPresent(id);
    }

    public void put(String id, Multimap<String,PatternWrapper> traceInfo) {
        cache.put(id, traceInfo);
        synchronized (listeners) {
            for (CacheListener listener : listeners)
                listener.cacheEntryModified(id, traceInfo);
        }
    }

    public Multimap<String,PatternWrapper> putIfAbsent(String id, Multimap<String,PatternWrapper> traceInfo) {
        return cache.asMap().putIfAbsent(id, traceInfo);
    }

    public void remove(String id) {
        cache.invalidate(id);
    }

    public void clear() {
        cache.asMap().clear();
    }

    public void addListener(CacheListener listener) {
        synchronized (listeners) {
            listeners.add(listener);
        }
    }

    public void removeListener(CacheListener listener) {
        synchronized (listeners) {
            listeners.remove(listener);
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
