package datawave.webservice.query.cache.limits;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class InMemoryStore implements QueryLimitStore {

    private final ConcurrentMap<String,Integer> inmemoryStore = new ConcurrentHashMap<>();

    @Override
    public Integer setQueryLimit(String dn, int limit) {
        return inmemoryStore.put(dn, limit);
    }

    @Override
    public Integer getQueryLimit(String dn, Integer defaultValue) {
        return inmemoryStore.getOrDefault(dn, defaultValue);
    }
}
