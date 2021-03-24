package datawave.ingest.mapreduce.handler.facet;

public interface FacetedEstimator<T> {
    /**
     * Estimate the cardinality of a given input. Implementations shall know the method for producing the cardinality of this object.
     *
     * @param input
     * @return
     */
    FacetValue estimate(T input);
    
}
