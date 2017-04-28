package nsa.datawave.webservice.common.cache;

import java.io.Serializable;
import java.util.Date;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import nsa.datawave.webservice.common.connection.AccumuloConnectionFactory;

import nsa.datawave.accumulo.inmemory.InMemoryInstance;

public interface TableCache extends Callable<Boolean>, Serializable {
    
    public String getTableName();
    
    public String getConnectionPoolName();
    
    public String getAuths();
    
    public long getReloadInterval();
    
    public Date getLastRefresh();
    
    public AccumuloConnectionFactory getConnectionFactory();
    
    public InMemoryInstance getInstance();
    
    public SharedCacheCoordinator getWatcher();
    
    public Future<Boolean> getReference();
    
    public long getMaxRows();
    
    public void setTableName(String tableName);
    
    public void setConnectionPoolName(String connectionPoolName);
    
    public void setAuths(String auths);
    
    public void setReloadInterval(long reloadInterval);
    
    public void setLastRefresh(Date lastRefresh);
    
    public void setConnectionFactory(AccumuloConnectionFactory connectionFactory);
    
    public void setInstance(InMemoryInstance instance);
    
    public void setWatcher(SharedCacheCoordinator watcher);
    
    public void setReference(Future<Boolean> reference);
    
    public void setMaxRows(long maxRows);
    
    public Boolean call() throws Exception;
    
}
