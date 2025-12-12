package datawave.ingest.mapreduce.handler.facet;

import com.google.common.collect.Multimap;

import datawave.ingest.data.config.NormalizedContentInterface;

public interface FacetedEstimator<T> {
    /**
     * Estimate the cardinality of a given input. Implementations shall know the method for producing the cardinality of this object.
     *
     * @param input
     *            the given input
     * @return a FacetValue
     */
    FacetValue estimate(T input, Multimap<String,NormalizedContentInterface> eventFields);

}
