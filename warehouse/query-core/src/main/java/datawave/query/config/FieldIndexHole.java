package datawave.query.config;

import java.io.Serializable;

public class FieldIndexHole implements Serializable, Comparable<FieldIndexHole> {
    
    private static final long serialVersionUID = -6778479621810682281L;
    
    private String startDate;
    private String endDate;
    
    public FieldIndexHole() {}
    
    public FieldIndexHole(String[] dateRange) {
        setStartDate(dateRange[0]);
        setEndDate(dateRange[1]);
    }
    
    @Override
    public int compareTo(FieldIndexHole otherHole) {
        
        int comparison = startDate.compareTo(otherHole.startDate);
        if (comparison == 0) {
            comparison = endDate.compareTo(otherHole.endDate);
        }
        
        return comparison;
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
    }
}
