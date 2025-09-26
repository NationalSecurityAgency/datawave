package datawave.webservice.query.logic;

import java.util.Map;

public class QueryLogicFactoryConfiguration {
    // The logicMap is the list of logics that can be loaded.
    // This is a map of logic name to bean name.
    // If empty, then any query logic bean can be loaded.
    // in that case the bean name is the logic name.
    private Map<String,String> logicMap = null;
    private int maxPageSize = 0;
    private long pageByteTrigger = 0;

    public Map<String,String> getLogicMap() {
        return logicMap;
    }

    public void setLogicMap(Map<String,String> logicMap) {
        this.logicMap = logicMap;
    }

    public boolean hasLogicMap() {
        return logicMap != null && !logicMap.isEmpty();
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
