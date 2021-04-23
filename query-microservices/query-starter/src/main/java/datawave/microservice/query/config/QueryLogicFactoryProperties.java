package datawave.microservice.query.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "query.logic.factory")
public class QueryLogicFactoryProperties {
    private String xmlBeansPath = "classpath:QueryLogicFactory.xml";
    private int maxPageSize = 10000;
    private long pageByteTrigger = 0;
    
    public String getXmlBeansPath() {
        return xmlBeansPath;
    }
    
    public void setXmlBeansPath(String xmlBeansPath) {
        this.xmlBeansPath = xmlBeansPath;
    }
    
    public int getMaxPageSize() {
        return maxPageSize;
    }
    
    public void setMaxPageSize(int maxPageRecords) {
        this.maxPageSize = maxPageRecords;
    }
    
    public long getPageByteTrigger() {
        return pageByteTrigger;
    }
    
    public void setPageByteTrigger(long pageByteTrigger) {
        this.pageByteTrigger = pageByteTrigger;
    }
}
