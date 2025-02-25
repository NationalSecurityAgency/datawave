package datawave.microservice.querymetric;

public enum QueryMetricType {
    
    /*
     * DISTRIBUTED: PageMetrics do not know where they are in sequence and will be given a number after the last stored page, sourceCount, nextCount, seekCount,
     * yieldCount, docRanges, fiRanges will be added to the stored total COMPLETE: PageMetrics know where they are in sequence and the pageNumber that they
     * contain will be used sourceCount, nextCount, seekCount, yieldCount, docRanges, fiRanges are understood to be the new total
     */
    DISTRIBUTED, COMPLETE
    
}
