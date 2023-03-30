package datawave.ingest.mapreduce.handler.stats;

import com.clearspring.analytics.stream.cardinality.ICardinality;

public interface CardinalityEstimator<T> {
    
    /**
     * Estimate the cardinality of a given input. The concrete class shall know the method for producing the cardinality of this object.
     * 
     * @param input
     *            the input
     * @return a cardinality
     */
    ICardinality estimate(T input);
}
