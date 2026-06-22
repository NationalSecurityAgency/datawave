package datawave.core.iterators.querylock;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;

import datawave.core.iterators.filesystem.FileSystemCache;

/**
 * Created on 2/6/17.
 */
public interface QueryLock {

    void startQuery() throws Exception;

    boolean isQueryRunning();

    void stopQuery() throws Exception;

    void cleanup() throws Exception;

    class Builder {
        private String queryId;
        private String hdfsSiteConfigs;
        private FileSystemCache fsCache;
        private String zookeeperConfig;
        private long cleanupInterval;
        private String ivaratorURIs;
        private String fstURIs;

        public Builder forZookeeper(String zookeeperConfig, long cleanupInterval) {
            this.zookeeperConfig = zookeeperConfig;
            this.cleanupInterval = cleanupInterval;
            return this;
        }

        public Builder forHdfs(String hdfsSiteConfigs) {
            this.hdfsSiteConfigs = hdfsSiteConfigs;
            return this;
        }

        public Builder forFSCache(FileSystemCache fsCache) {
            this.fsCache = fsCache;
            return this;
        }

        public Builder forIvaratorDirs(String ivaratorURIs) {
            this.ivaratorURIs = ivaratorURIs;
            return this;
        }

        public Builder forFstDirs(String fstURI) {
            this.fstURIs = fstURI;
            return this;
        }

        public Builder forQueryId(String queryId) {
            this.queryId = queryId;
            return this;
        }

        private boolean isEmpty(String s) {
            return (s == null || s.isEmpty());
        }

        public QueryLock build() throws ConfigException, MalformedURLException {
            List<QueryLock> locks = new ArrayList<>();
            if (!isEmpty(zookeeperConfig)) {
                if (queryId == null) {
                    throw new IllegalArgumentException("Cannot create a query lock without a query id");
                }

                locks.add(new ZookeeperQueryLock(zookeeperConfig, cleanupInterval, queryId));
            }

            if (!isEmpty(fstURIs) || (!isEmpty(ivaratorURIs) && ivaratorURIs.contains("hdfs://"))) {
                if (!isEmpty(hdfsSiteConfigs) || fsCache != null) {
                    if (queryId == null) {
                        throw new IllegalArgumentException("Cannot create a query lock without a query id");
                    }

                    if (!isEmpty(fstURIs)) {
                        if (fsCache == null) {
                            locks.add(new HdfsQueryLock(hdfsSiteConfigs, fstURIs, queryId));
                        } else {
                            locks.add(new HdfsQueryLock(fsCache, fstURIs, queryId));
                        }
                    }
                    if (!isEmpty(ivaratorURIs) && ivaratorURIs.contains("hdfs://")) {
                        if (fsCache == null) {
                            locks.add(new HdfsQueryLock(hdfsSiteConfigs, ivaratorURIs, queryId));
                        } else {
                            locks.add(new HdfsQueryLock(fsCache, ivaratorURIs, queryId));
                        }
                    }
                } else {
                    throw new IllegalArgumentException("Configured hdfs URIs but missing hdfs configuration");
                }
            }

            if (locks.size() == 1) {
                return locks.get(0);
            } else if (locks.size() > 1) {
                return new CombinedQueryLock(locks);
            } else {
                return null;
            }

        }
    }
}
