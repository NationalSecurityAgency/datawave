package datawave.microservice.query.logic.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.HashMap;
import java.util.Map;

@ConfigurationProperties(prefix = "query.logic.factory")
public class QueryLogicFactoryProperties {
    private String xmlBeansPath = "classpath:QueryLogicFactory.xml";
    private int maxPageSize = 10000;
    private long pageByteTrigger = 0;
    
    private Map<String,String> querySyntaxParsers = new HashMap<>();
    
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
    
    public Map<String,String> getQuerySyntaxParsers() {
        return querySyntaxParsers;
    }
    
    public void setQuerySyntaxParsers(Map<String,String> querySyntaxParsers) {
        this.querySyntaxParsers = querySyntaxParsers;
    }
}
