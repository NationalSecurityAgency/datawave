package datawave.query.jexl.functions;

import datawave.query.predicate.EventDataQueryFilter;

import java.util.Set;

public interface TermFrequencyAggregatorFactory {
    TermFrequencyAggregator create(Set<String> fieldsToAggregate, EventDataQueryFilter attrFilter, int maxNextCount);
}
