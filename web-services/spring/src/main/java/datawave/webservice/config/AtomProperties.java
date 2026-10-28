package datawave.webservice.config;

public class AtomProperties {

    private String systemName;
    private String tableName;
    private String externalHostName;
    private int externalPort;
    private String connectionPoolName;

    public String getSystemName() {
        return systemName;
    }

    public void setSystemName(String systemName) {
        this.systemName = systemName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getExternalHostName() {
        return externalHostName;
    }

    public void setExternalHostName(String externalHostName) {
        this.externalHostName = externalHostName;
    }

    public int getExternalPort() {
        return externalPort;
    }

    public void setExternalPort(int externalPort) {
        this.externalPort = externalPort;
    }

    public String getConnectionPoolName() {
        return connectionPoolName;
    }

    public void setConnectionPoolName(String connectionPoolName) {
        this.connectionPoolName = connectionPoolName;
    }
}
