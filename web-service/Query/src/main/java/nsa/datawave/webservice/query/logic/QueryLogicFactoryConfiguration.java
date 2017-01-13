package nsa.datawave.webservice.query.logic;

import java.util.Map;

public class QueryLogicFactoryConfiguration {
    private int maxPageSize = 0;
    private long pageByteTrigger = 0;
    private Map<String,QueryLogic<?>> logicClasses = null;
    
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
