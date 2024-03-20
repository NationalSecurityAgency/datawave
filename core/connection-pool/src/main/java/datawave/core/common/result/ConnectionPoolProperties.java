package datawave.core.common.result;

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

    public void setUsername(String username) {
        this.username = username;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setInstance(String instance) {
        this.instance = instance;
    }

    public void setZookeepers(String zookeepers) {
        this.zookeepers = zookeepers;
    }

    public void setLowPriorityPoolSize(int lowPriorityPoolSize) {
        this.lowPriorityPoolSize = lowPriorityPoolSize;
    }

    public void setNormalPriorityPoolSize(int normalPriorityPoolSize) {
        this.normalPriorityPoolSize = normalPriorityPoolSize;
    }

    public void setHighPriorityPoolSize(int highPriorityPoolSize) {
        this.highPriorityPoolSize = highPriorityPoolSize;
    }

    public void setAdminPriorityPoolSize(int adminPriorityPoolSize) {
        this.adminPriorityPoolSize = adminPriorityPoolSize;
    }

}
