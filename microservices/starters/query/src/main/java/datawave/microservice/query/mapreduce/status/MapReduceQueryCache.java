package datawave.microservice.query.mapreduce.status;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.springframework.util.MultiValueMap;

import datawave.microservice.authorization.user.DatawaveUserDetails;
import datawave.microservice.query.Query;
import datawave.microservice.query.mapreduce.status.cache.MapReduceQueryIdByJobIdCache;
import datawave.microservice.query.mapreduce.status.cache.MapReduceQueryIdByUsernameCache;
import datawave.microservice.query.mapreduce.status.cache.MapReduceQueryStatusCache;
import datawave.microservice.query.mapreduce.status.cache.util.CacheUpdater;
import datawave.microservice.query.mapreduce.status.cache.util.LockedCacheUpdateUtil;
import datawave.webservice.query.exception.QueryException;

public class MapReduceQueryCache {
    private MapReduceQueryStatusCache queryStatusCache;
    private MapReduceQueryIdByJobIdCache queryIdByJobIdCache;
    private MapReduceQueryIdByUsernameCache queryIdByUsernameCache;
    
    private LockedCacheUpdateUtil<MapReduceQueryStatus> queryStatusLockedCacheUpdateUtil;
    
    public MapReduceQueryCache(MapReduceQueryStatusCache queryStatusCache, MapReduceQueryIdByJobIdCache queryIdByJobIdCache,
                    MapReduceQueryIdByUsernameCache queryIdByUsernameCache) {
        this.queryStatusCache = queryStatusCache;
        this.queryIdByJobIdCache = queryIdByJobIdCache;
        this.queryIdByUsernameCache = queryIdByUsernameCache;
        this.queryStatusLockedCacheUpdateUtil = new LockedCacheUpdateUtil<>(queryStatusCache);
    }
    
    public MapReduceQueryStatus createQuery(String mrQueryId, String jobName, MultiValueMap<String,String> parameters, DatawaveUserDetails currentUser)
                    throws InterruptedException {
        addQueryIdByUsernameLookup(currentUser.getUsername(), mrQueryId);
        return queryStatusCache.create(mrQueryId, jobName, parameters, null, currentUser);
    }
    
    public MapReduceQueryStatus createQuery(String mrQueryId, String jobName, MultiValueMap<String,String> parameters, Query query,
                    DatawaveUserDetails currentUser) throws InterruptedException {
        addQueryIdByUsernameLookup(currentUser.getUsername(), mrQueryId);
        return queryStatusCache.create(mrQueryId, jobName, parameters, query, currentUser);
    }
    
    public MapReduceQueryStatus getQueryStatus(String mrQueryId) {
        return queryStatusCache.get(mrQueryId);
    }
    
    public MapReduceQueryStatus updateQueryStatus(String mrQueryId, CacheUpdater<MapReduceQueryStatus> updater, long waitTimeMillis, long leaseTimeMillis)
                    throws QueryException, InterruptedException {
        return queryStatusLockedCacheUpdateUtil.lockedUpdate(mrQueryId, updater, waitTimeMillis, leaseTimeMillis);
    }
    
    public MapReduceQueryStatus removeQuery(String mrQueryId) throws InterruptedException {
        MapReduceQueryStatus mapReduceQueryStatus = queryStatusCache.get(mrQueryId);
        queryStatusCache.remove(mrQueryId);
        removeQueryIdByJobIdLookup(mapReduceQueryStatus.getJobId());
        removeQueryIdByUsernameLookup(mapReduceQueryStatus.getCurrentUser().getUsername(), mrQueryId);
        return mapReduceQueryStatus;
    }
    
    public String putQueryIdByJobIdLookup(String jobId, String mrQueryId) {
        return queryIdByJobIdCache.update(jobId, mrQueryId);
    }
    
    public String lookupQueryIdByJobId(String jobId) {
        return queryIdByJobIdCache.get(jobId);
    }
    
    public void removeQueryIdByJobIdLookup(String jobId) {
        queryIdByJobIdCache.remove(jobId);
    }
    
    private Set<String> addQueryIdByUsernameLookup(String username, String mrQueryId) throws InterruptedException {
        Set<String> mrQueryIds = null;
        if (queryIdByUsernameCache.tryLock(username, TimeUnit.SECONDS.toMillis(30), TimeUnit.SECONDS.toMillis(30))) {
            try {
                mrQueryIds = queryIdByUsernameCache.get(username);
                if (mrQueryIds == null) {
                    mrQueryIds = new HashSet<>();
                }
                mrQueryIds.add(mrQueryId);
                queryIdByUsernameCache.update(username, mrQueryIds);
            } finally {
                queryIdByUsernameCache.unlock(username);
            }
        }
        return mrQueryIds;
    }
    
    public Set<String> lookupQueryIdsByUsername(String username) {
        Set<String> queryIds = queryIdByUsernameCache.get(username);
        if (queryIds == null) {
            queryIds = new HashSet<>();
        }
        return queryIds;
    }
    
    public Set<String> removeQueryIdByUsernameLookup(String username, String mrQueryId) throws InterruptedException {
        Set<String> mrQueryIds = null;
        if (queryIdByUsernameCache.tryLock(username, TimeUnit.SECONDS.toMillis(30), TimeUnit.SECONDS.toMillis(30))) {
            try {
                mrQueryIds = queryIdByUsernameCache.get(username);
                if (mrQueryIds != null) {
                    mrQueryIds.remove(mrQueryId);
                    if (mrQueryIds.isEmpty()) {
                        queryIdByUsernameCache.removeAll(username);
                    } else {
                        queryIdByUsernameCache.update(username, mrQueryIds);
                    }
                }
            } finally {
                queryIdByUsernameCache.unlock(username);
            }
        }
        return mrQueryIds;
    }
    
    public void removeAllQueryIdByUsernameLookups(String username) {
        queryIdByUsernameCache.removeAll(username);
    }
}
