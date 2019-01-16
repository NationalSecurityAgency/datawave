package datawave.webservice.result;

public interface TotalResultsAware {
    
    void setTotalResults(long totalResults);
    
    long getTotalResults();
}
