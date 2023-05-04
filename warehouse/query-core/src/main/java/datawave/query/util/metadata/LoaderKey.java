package datawave.query.util.metadata;

import org.apache.accumulo.core.client.AccumuloClient;

/**
 * 
 */
public class LoaderKey {
    protected AccumuloClient client;
    protected String table;
    protected String user;
    
    public LoaderKey(AccumuloClient client, String table, String user) {
        this.client = client;
        this.table = table;
        this.user = user;
    }
    
    public int hashCode() {
        return client.instanceOperations().getInstanceId().hashCode() + table.hashCode() + user.hashCode();
    }
    
    public boolean equals(Object other) {
        if (other instanceof LoaderKey) {
            LoaderKey otherLoader = (LoaderKey) other;
            return otherLoader.client.instanceOperations().getInstanceId().equals(client.instanceOperations().getInstanceId())
                            && otherLoader.table.equals(table) && otherLoader.user.equals(user);
        }
        return false;
    }
    
    @Override
    public String toString() {
        return client.instanceOperations().getInstanceId() + "/" + "/" + table + "/" + user;
    }
    
}
