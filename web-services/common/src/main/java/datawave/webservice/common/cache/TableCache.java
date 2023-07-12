package datawave.webservice.common.cache;

import java.io.Serializable;
import java.util.Date;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.webservice.common.connection.AccumuloConnectionFactory;

public interface TableCache extends Callable<Boolean>, Serializable {

    String getTableName();

    String getConnectionPoolName();

    String getAuths();

    long getReloadInterval();

    Date getLastRefresh();

    AccumuloConnectionFactory getConnectionFactory();

    InMemoryInstance getInstance();

    SharedCacheCoordinator getWatcher();

    Future<Boolean> getReference();

    long getMaxRows();

    void setTableName(String tableName);

    void setConnectionPoolName(String connectionPoolName);

    void setAuths(String auths);

    void setReloadInterval(long reloadInterval);

    void setLastRefresh(Date lastRefresh);

    void setConnectionFactory(AccumuloConnectionFactory connectionFactory);

    void setInstance(InMemoryInstance instance);

    void setWatcher(SharedCacheCoordinator watcher);

    void setReference(Future<Boolean> reference);

    void setMaxRows(long maxRows);

    Boolean call() throws Exception;

}
