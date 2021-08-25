package datawave.webservice.common.result;

public class ConnectionPoolProperties {
    
    protected String username;
    protected String password;
    protected String instance;
    protected String zookeepers;
    protected int lowPriorityPoolSize;
    protected int normalPriorityPoolSize;
    protected int highPriorityPoolSize;
    protected int adminPriorityPoolSize;
    
    public String getUsername() {
        return username;
    }
    
    public String getPassword() {
        return password;
    }
    
    public String getInstance() {
        return instance;
    }
    
    public String getZookeepers() {
        return zookeepers;
    }
    
    public int getLowPriorityPoolSize() {
        return lowPriorityPoolSize;
    }
    
    public int getNormalPriorityPoolSize() {
        return normalPriorityPoolSize;
    }
    
    public int getHighPriorityPoolSize() {
        return highPriorityPoolSize;
    }
    
    public int getAdminPriorityPoolSize() {
        return adminPriorityPoolSize;
    }
    
}
