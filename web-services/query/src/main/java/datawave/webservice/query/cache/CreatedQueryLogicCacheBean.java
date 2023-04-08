package datawave.webservice.query.cache;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import datawave.configuration.DatawaveEmbeddedProjectStageHolder;
import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.core.query.logic.QueryLogic;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.util.Pair;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.deltaspike.core.api.exclude.Exclude;
import org.apache.log4j.Logger;

import javax.annotation.PreDestroy;
import javax.annotation.security.PermitAll;
import javax.annotation.security.RunAs;
import javax.ejb.ConcurrencyManagement;
import javax.ejb.ConcurrencyManagementType;
import javax.ejb.LocalBean;
import javax.ejb.Singleton;
import javax.ejb.Startup;
import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Startup
@Singleton
@RunAs("InternalUser")
@PermitAll
@ConcurrencyManagement(ConcurrencyManagementType.BEAN)
@LocalBean
@Exclude(ifProjectStage = DatawaveEmbeddedProjectStageHolder.DatawaveEmbedded.class)
public class CreatedQueryLogicCacheBean {
    
    public static class Triple {
        final String userID;
        final QueryLogic<?> logic;
        final AccumuloClient client;
        
        public Triple(String userID, QueryLogic<?> logic, AccumuloClient client) {
            super();
            this.userID = userID;
            this.logic = logic;
            this.client = client;
        }
        
        @Override
        public int hashCode() {
            return ((new HashCodeBuilder()).append(userID).append(logic).append(client)).toHashCode();
        }
        
        @Override
        public boolean equals(Object o) {
            if (o instanceof Triple) {
                Triple other = (Triple) o;
                return other.userID.equals(this.userID) && other.logic.equals(this.logic) && other.client.equals(this.client);
            }
            
            return false;
        }
    }
    
    private static final Function<Triple,Pair<QueryLogic<?>,AccumuloClient>> tripToPair = from -> {
        if (null == from) {
            return null;
        }
        return new Pair<>(from.logic, from.client);
    };
    
    // returns the logic and connection fields in a triple as a pair
    private static final Function<Entry<Pair<String,Long>,Triple>,Entry<String,Pair<QueryLogic<?>,AccumuloClient>>> toPair = from -> {
        if (from == null) {
            return null;
        } else {
            return Maps.immutableEntry(from.getKey().getFirst(), tripToPair.apply(from.getValue()));
        }
    };
    
    private static final Logger log = Logger.getLogger(CreatedQueryLogicCacheBean.class);
    
    @Inject
    private AccumuloConnectionFactory connectionFactory;
    private final ConcurrentHashMap<Pair<String,Long>,Triple> cache = new ConcurrentHashMap<>();
    
    /**
     * Add the provided QueryLogic to the QueryLogicCache.
     * 
     * @param queryId
     * @param userId
     * @param logic
     * @param client
     * @return true if there was no previous mapping for the given queryId in the cache.
     */
    public boolean add(String queryId, String userId, QueryLogic<?> logic, AccumuloClient client) {
        Triple value = new Triple(userId, logic, client);
        long updateTime = System.currentTimeMillis();
        return cache.putIfAbsent(new Pair<>(queryId, updateTime), value) == null;
    }
    
    public Pair<QueryLogic<?>,AccumuloClient> poll(String id) {
        Entry<Pair<String,Long>,Triple> entry = get(id);
        return entry == null ? null : tripToPair.apply(entry.getValue());
    }
    
    public Map<String,Pair<QueryLogic<?>,AccumuloClient>> snapshot() {
        HashMap<Pair<String,Long>,Triple> snapshot = Maps.newHashMap(cache);
        HashMap<String,Pair<QueryLogic<?>,AccumuloClient>> tformMap = Maps.newHashMapWithExpectedSize(cache.size());
        
        for (Entry<String,Pair<QueryLogic<?>,AccumuloClient>> tform : Iterables.transform(snapshot.entrySet(), toPair)) {
            tformMap.put(tform.getKey(), tform.getValue());
        }
        return tformMap;
    }
    
    public Map<String,Pair<QueryLogic<?>,AccumuloClient>> entriesOlderThan(final Long now, final Long expiration) {
        Iterable<Entry<Pair<String,Long>,Triple>> iter = Iterables.filter(cache.entrySet(), input -> {
            Long timeInserted = input.getKey().getSecond();
            
            // If this entry was inserted more than the TTL 'time' ago, do not return it
            if ((now - expiration) > timeInserted) {
                return true;
            }
            
            return false;
        });
        
        Map<String,Pair<QueryLogic<?>,AccumuloClient>> result = Maps.newHashMapWithExpectedSize(32);
        for (Entry<Pair<String,Long>,Triple> entry : iter) {
            result.put(entry.getKey().getFirst(), tripToPair.apply(entry.getValue()));
        }
        
        return result;
    }
    
    public Pair<QueryLogic<?>,AccumuloClient> pollIfOwnedBy(String queryId, String userId) {
        Entry<Pair<String,Long>,Triple> entry = get(queryId);
        if (entry != null) {
            if (userId.equals(entry.getValue().userID)) {
                return tripToPair.apply(entry.getValue());
            } else {
                cache.put(entry.getKey(), entry.getValue());
            }
        }
        
        return null;
    }
    
    public void clearQueryLogics(long currentTimeMs, long timeToLiveMs) {
        Set<Entry<String,Pair<QueryLogic<?>,AccumuloClient>>> entrySet = entriesOlderThan(currentTimeMs, timeToLiveMs).entrySet();
        clearQueryLogics(entrySet);
    }
    
    private void clearQueryLogics(Set<Entry<String,Pair<QueryLogic<?>,AccumuloClient>>> entrySet) {
        int count = 0;
        for (Entry<String,Pair<QueryLogic<?>,AccumuloClient>> entry : entrySet) {
            Pair<QueryLogic<?>,AccumuloClient> activePair = poll(entry.getKey());
            
            if (activePair == null) {
                if (log.isDebugEnabled()) {
                    log.debug("Could not find identified pair needing qlCache eviction. Someone removed him from underneath us");
                }
                continue;
            }
            
            try {
                activePair.getFirst().close();
            } catch (Exception ex) {
                log.error("Exception caught while closing an uninitialized query logic.", ex);
            }
            
            try {
                connectionFactory.returnClient(activePair.getSecond());
            } catch (Exception ex) {
                log.error("Could not return connection from: " + entry.getKey() + " - " + activePair, ex);
            }
            
            count++;
        }
        
        if (count > 0 && log.isDebugEnabled()) {
            log.debug(count + " entries evicted from the query logic cache.");
        }
    }
    
    @PreDestroy
    public void shutdown() {
        clearQueryLogics(snapshot().entrySet());
    }
    
    /**
     * Finds and entry in the underlying cache with the given queryId. Returns null if no such element in the map is found. Will only return the
     * arbitrarily-found 'first' entry as we shouldn't have such a collision in the first place
     * 
     * @param queryId
     * @return
     */
    private Entry<Pair<String,Long>,Triple> get(String queryId) {
        for (Pair<String,Long> key : cache.keySet()) {
            if (key.getFirst().equals(queryId)) {
                return Maps.immutableEntry(key, cache.remove(key));
            }
        }
        
        return null;
    }
}
