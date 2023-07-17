package datawave.webservice.results.cached;

import java.util.HashMap;
import java.util.Map;

public class CachedResultsConfiguration {

    private int defaultPageSize = 20;
    private int maxPageSize = 0;
    private long pageByteTrigger = 0;
    private Map<String,String> parameters = new HashMap<>();

    public int getDefaultPageSize() {
        return defaultPageSize;
    }

    public void setDefaultPageSize(int defaultPageSize) {
        this.defaultPageSize = defaultPageSize;
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

    public Map<String,String> getParameters() {
        return parameters;
    }

    public void setParameters(Map<String,String> parameters) {
        this.parameters = parameters;
    }

    public int getRowsPerBatch() {
        return Integer.parseInt(getParameters().get("ROWS_PER_BATCH"));
    }
}
