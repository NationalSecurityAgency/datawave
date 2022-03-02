package datawave.query.config;

import datawave.query.planner.DefaultQueryPlanner;
import datawave.webservice.common.logging.ThreadConfigurableLogger;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.Serializable;

public class FieldIndexHole implements Serializable, Comparable<FieldIndexHole> {
    
    private static final Logger log = ThreadConfigurableLogger.getLogger(FieldIndexHole.class);
    
    private static final long serialVersionUID = -6778479621810682281L;
    
    private String fieldName;
    private String startDate;
    private String endDate;
    public static int NOT_SAME_FIELD = Integer.MAX_VALUE;
    
    public FieldIndexHole() {
        
    }
    
    public FieldIndexHole(String name, String start) {
        fieldName = name;
        startDate = start;
    }
    
    public FieldIndexHole(String name, String start, String end) {
        fieldName = name;
        startDate = start;
        endDate = end;
    }
    
    public FieldIndexHole(String field, String[] dateRange) {
        fieldName = field;
        setStartDate(dateRange[0]);
        setEndDate(dateRange[1]);
    }
    
    @Override
    public int compareTo(FieldIndexHole otherHole) {
        
        int comparison = fieldName.compareTo(otherHole.getFieldName());
        if (comparison == 0) {
            comparison = startDate.compareTo(otherHole.startDate);
            if (comparison == 0) {
                comparison = endDate.compareTo(otherHole.endDate);
            }
        } else {
            return NOT_SAME_FIELD;
        }
        
        return comparison;
    }
    
    /**
     * Does the hole overlap the specified date range
     */
    public boolean overlaps(String start, String end) {
        
        if (startDate.compareTo(end) <= 0 && endDate.compareTo(start) >= 0) {
            return true;
        }
        
        return false;
    }
    
    public String getFieldName() {
        return fieldName;
    }
    
    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }
    
    public String getStartDate() {
        return startDate;
    }
    
    public void setStartDate(String startDate) {
        this.startDate = startDate;
    }
    
    public String getEndDate() {
        return endDate;
    }
    
    public void setEndDate(String endDate) {
        this.endDate = endDate;
        if (endDate.compareTo(startDate) < 0)
            log.warn("End date " + endDate + " came before start date " + startDate);
    }
    
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append('(');
        builder.append('[').append("fieldName").append(':').append(fieldName).append(']');
        builder.append(',');
        builder.append('[').append(startDate).append(',').append(endDate).append(']');
        builder.append(')');
        return builder.toString();
    }
}
