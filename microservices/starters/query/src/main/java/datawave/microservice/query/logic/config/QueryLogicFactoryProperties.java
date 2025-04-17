package datawave.microservice.query.logic.config;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "datawave.query.logic.factory")
public class QueryLogicFactoryProperties {
    private String xmlBeansPath = "classpath:QueryLogicFactory.xml,EdgeQueryLogicFactory.xml";
    private Map<String,String> queryLogicsByName = new LinkedHashMap<>();
    private int maxPageSize = 10000;
    private long pageByteTrigger = 0;
    
    private Map<String,String> querySyntaxParsers = new HashMap<>();
    
    public String getXmlBeansPath() {
        return xmlBeansPath;
    }
    
    public void setXmlBeansPath(String xmlBeansPath) {
        this.xmlBeansPath = xmlBeansPath;
    }
    
    public Map<String,String> getQueryLogicsByName() {
        return queryLogicsByName;
    }
    
    public void setQueryLogicsByName(Map<String,String> queryLogicsByName) {
        this.queryLogicsByName = queryLogicsByName;
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
