package datawave.core.common.cache;

import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.core.common.connection.AccumuloConnectionFactory;
import datawave.core.common.result.TableCacheDescription;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;

import java.util.List;

/**
 * Object that caches data from Accumulo tables.
 */
public interface AccumuloTableCache extends AutoCloseable {

    String MOCK_USERNAME = "";
    PasswordToken MOCK_PASSWORD = new PasswordToken(new byte[0]);

    void setConnectionFactory(AccumuloConnectionFactory connectionFactory);

    InMemoryInstance getInstance();

    void submitReloadTasks();

    public void reloadTableCache(String tableName);

    public List<TableCacheDescription> getTableCaches();
}
