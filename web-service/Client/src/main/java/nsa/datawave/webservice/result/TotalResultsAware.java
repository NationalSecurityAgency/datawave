package nsa.datawave.webservice.result;

public interface TotalResultsAware {
    
    public void setTotalResults(long totalResults);
    
    public long getTotalResults();
}
