package datawave.zookeeper;

public class ZkPojoPublisherProducerConfiguration {
    
    private String rootNamespace;
    
    private String hdfsConfigUrls;
    
    public String getRootNamespace() {
        return rootNamespace;
    }
    
    public void setRootNamespace(String rootNamespace) {
        this.rootNamespace = rootNamespace;
    }
    
    public String getHdfsConfigUrls() {
        return hdfsConfigUrls;
    }
    
    public void setHdfsConfigUrls(String hdfsConfigUrls) {
        this.hdfsConfigUrls = hdfsConfigUrls;
    }
}
