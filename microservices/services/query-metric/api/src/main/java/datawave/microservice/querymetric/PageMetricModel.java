package datawave.microservice.querymetric;

import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class PageMetricModel extends BaseQueryMetric.PageMetric {
    
    private NumberFormat nf = NumberFormat.getIntegerInstance();
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd HHmmss");
    
    public PageMetricModel(BaseQueryMetric.PageMetric pageMetric) {
        super(pageMetric);
    }
    
    public String getPageNumberStr() {
        return numToString(getPageNumber(), 1);
    }
    
    public String getPageRequestedStr() {
        return getPageRequested() > 0 ? sdf.format(new Date(getPageRequested())) : "";
    }
    
    public String getPageReturnedStr() {
        return getPageReturned() > 0 ? sdf.format(new Date(getPageReturned())) : "";
    }
    
    public String getReturnTimeStr() {
        return nf.format(getReturnTime());
    }
    
    public String getPageSizeStr() {
        return nf.format(getPagesize());
    }
    
    public String getCallTimeStr() {
        return numToString(getCallTime(), 0);
    }
    
    public String getLoginTimeStr() {
        return numToString(getLoginTime(), 0);
    }
    
    public String getSerializationTimeStr() {
        return numToString(getSerializationTime(), 0);
    }
    
    public String getBytesWrittenStr() {
        return numToString(getBytesWritten(), 0);
    }
    
    private String numToString(long number, long minValue) {
        return number < minValue ? "" : nf.format(number);
    }
}
