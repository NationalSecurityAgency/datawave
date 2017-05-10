package datawave.query.util.metadata;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;

/**
 * 
 */
public class LoaderKey {
    protected Instance instance;
    protected Connector connector;
    protected String table;
    protected String user;
    
    public LoaderKey(Instance instance, Connector connector, String table, String user) {
        this.instance = instance;
        this.connector = connector;
        this.table = table;
        this.user = user;
    }
    
    public int hashCode() {
        return instance.hashCode() + table.hashCode() + user.hashCode();
    }
    
    public boolean equals(Object other) {
        if (other instanceof LoaderKey) {
            LoaderKey otherLoader = (LoaderKey) other;
            return otherLoader.instance.toString().equals(instance.toString()) && otherLoader.table.equals(table) && otherLoader.user.equals(user);
        }
        return false;
    }
    
    @Override
    public String toString() {
        return new StringBuilder().append(instance.getInstanceID()).append("/").append(connector.getInstance().getInstanceName()).append("/").append("/")
                        .append(table).append("/").append(user).toString();
    }
    
}
