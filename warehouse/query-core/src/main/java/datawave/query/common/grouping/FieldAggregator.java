package datawave.query.common.grouping;

import datawave.query.attributes.Attribute;
import org.apache.commons.lang.builder.ToStringBuilder;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * This class provides functionality to aggregate values for specified target fields using specified aggregation operations.
 */
public class FieldAggregator {
    
    private final Map<String,Map<AggregateOperation,Aggregator<?>>> aggregatorMap;
    
    public FieldAggregator() {
        aggregatorMap = new HashMap<>();
    }
    
    public FieldAggregator(Set<String> sumFields, Set<String> maxFields, Set<String> minFields, Set<String> countFields, Set<String> averageFields) {
        this();
        populateAggregators(sumFields, SumAggregator::new);
        populateAggregators(maxFields, MaxAggregator::new);
        populateAggregators(minFields, MinAggregator::new);
        populateAggregators(countFields, CountAggregator::new);
        populateAggregators(averageFields, AverageAggregator::new);
    }
    
    /**
     * Add an aggregator supplied by the given constructor for each of the given fields to the aggregator map.
     * 
     * @param fields
     *            the fields
     * @param constructor
     *            the aggregator constructor
     */
    private void populateAggregators(Set<String> fields, Function<String,Aggregator<?>> constructor) {
        if (fields != null) {
            for (String field : fields) {
                Aggregator<?> aggregator = constructor.apply(field);
                Map<AggregateOperation,Aggregator<?>> map = aggregatorMap.get(field);
                if (map == null) {
                    map = new HashMap<>();
                    this.aggregatorMap.put(field, map);
                }
                map.put(aggregator.getOperation(), aggregator);
            }
        }
    }
    
    public void aggregate(Field field) {
        if (aggregatorMap.containsKey(field.getBase())) {
            Collection<Aggregator<?>> aggregators = this.aggregatorMap.get(field.getBase()).values();
            for (Attribute<?> attribute : field.getAttributes()) {
                aggregators.forEach(aggregator -> aggregator.aggregate(attribute));
            }
        }
    }
    
    public void aggregateAll(Collection<Field> fields) {
        fields.forEach(this::aggregate);
    }
    
    /**
     * Return the map of fields to their aggregators.
     *
     * @return the aggregator map.
     */
    public Map<String,Map<AggregateOperation,Aggregator<?>>> getAggregatorMap() {
        return aggregatorMap;
    }
    
    public Aggregator<?> getAggregator(String field, AggregateOperation operation) {
        Map<AggregateOperation,Aggregator<?>> map = aggregatorMap.get(field);
        if (map != null) {
            return map.get(operation);
        }
        return null;
    }
    
    /**
     * Return the set of all fields being aggregated.
     * 
     * @return the fields
     */
    public Collection<String> getFieldsToAggregate() {
        return aggregatorMap.keySet();
    }
    
    /**
     * Merge the given aggregator into this aggregated fields.
     * 
     * @param aggregator
     *            the aggregator to merge.
     */
    public void mergeAggregator(Aggregator<?> aggregator) {
        if (aggregator.hasAggregation()) {
            Map<AggregateOperation,Aggregator<?>> map = aggregatorMap.computeIfAbsent(aggregator.getField(), k -> new HashMap<>());
            if (map.containsKey(aggregator.getOperation())) {
                Aggregator<?> currentAggregator = map.get(aggregator.getOperation());
                if (currentAggregator.hasAggregation()) {
                    currentAggregator.merge(aggregator);
                } else {
                    map.put(aggregator.getOperation(), aggregator);
                }
            } else {
                map.put(aggregator.getOperation(), aggregator);
            }
        }
        
    }
    
    /**
     * Merge the given aggregated fields into this aggregated fields.
     * 
     * @param other
     *            the aggregated fields to merge in
     */
    public void merge(FieldAggregator other) {
        for (String field : other.aggregatorMap.keySet()) {
            // If we already have aggregators for this field, merge the aggregators for the current field from the other aggregated fields into this one.
            if (this.aggregatorMap.containsKey(field)) {
                Map<AggregateOperation,Aggregator<?>> thisMap = this.aggregatorMap.get(field);
                Map<AggregateOperation,Aggregator<?>> otherMap = other.aggregatorMap.get(field);
                for (AggregateOperation operation : otherMap.keySet()) {
                    if (thisMap.containsKey(operation)) {
                        Aggregator<?> currentAggregator = thisMap.get(operation);
                        Aggregator<?> otherAggregator = otherMap.get(operation);
                        if (currentAggregator.hasAggregation() && otherAggregator.hasAggregation()) {
                            currentAggregator.merge(otherAggregator);
                        } else if (otherAggregator.hasAggregation()) {
                            thisMap.put(operation, otherAggregator);
                        }
                    } else {
                        thisMap.put(operation, otherMap.get(operation));
                    }
                }
            } else {
                // If no aggregators exist in this aggregated fields for the current field, add all aggregators for it.
                this.aggregatorMap.put(field, new HashMap<>(other.aggregatorMap.get(field)));
            }
        }
    }
    
    @Override
    public String toString() {
        return aggregatorMap.toString();
    }
    
    /**
     * A factory that will generate new {@link FieldAggregator} with the designated sum, max, min, count, and average aggregation field targets.
     */
    public static class Factory {
        
        private final Set<String> sumFields;
        private final Set<String> maxFields;
        private final Set<String> minFields;
        private final Set<String> countFields;
        private final Set<String> averageFields;
        private final Set<String> allFields;
        
        public Factory() {
            this.sumFields = new HashSet<>();
            this.maxFields = new HashSet<>();
            this.minFields = new HashSet<>();
            this.countFields = new HashSet<>();
            this.averageFields = new HashSet<>();
            this.allFields = new HashSet<>();
        }
        
        /**
         * Set the fields for which to find the aggregated sum.
         * 
         * @param fields
         *            the fields
         * @return this factory
         */
        public Factory withSumFields(Set<String> fields) {
            addFields(this.sumFields, fields);
            return this;
        }
        
        /**
         * Set the fields for which to find the aggregated sum.
         *
         * @param fields
         *            the fields
         * @return this factory
         */
        public Factory withSumFields(String... fields) {
            addFields(this.sumFields, fields);
            return this;
        }
        
        /**
         * Set the fields for which to find the aggregated max.
         * 
         * @param fields
         *            the fields
         * @return this factory
         */
        public Factory withMaxFields(Set<String> fields) {
            addFields(this.maxFields, fields);
            return this;
        }
        
        /**
         * Set the fields for which to find the aggregated max.
         *
         * @param fields
         *            the fields
         * @return this factory
         */
        public Factory withMaxFields(String... fields) {
            addFields(this.maxFields, fields);
            return this;
        }
        
        /**
         * Set the fields for which to find the aggregated min.
         * 
         * @param fields
         *            the fields
         * @return this factory
         */
        public Factory withMinFields(Set<String> fields) {
            addFields(this.minFields, fields);
            return this;
        }
        
        /**
         * Set the fields for which to find the aggregated min.
         *
         * @param fields
         *            the fields
         * @return this factory
         */
        public Factory withMinFields(String... fields) {
            addFields(this.minFields, fields);
            return this;
        }
        
        /**
         * Set the fields for which to find the total number of times seen.
         * 
         * @param fields
         *            the fields
         * @return this factory
         */
        public Factory withCountFields(Set<String> fields) {
            addFields(this.countFields, fields);
            return this;
        }
        
        /**
         * Set the fields for which to find the aggregated count.
         *
         * @param fields
         *            the fields
         * @return this factory
         */
        public Factory withCountFields(String... fields) {
            addFields(this.countFields, fields);
            return this;
        }
        
        /**
         * Set the fields for which to find the aggregated average.
         * 
         * @param fields
         *            the fields
         * @return this factory
         */
        public Factory withAverageFields(Set<String> fields) {
            addFields(this.averageFields, fields);
            return this;
        }
        
        /**
         * Set the fields for which to find the aggregated average.
         *
         * @param fields
         *            the fields
         * @return this factory
         */
        public Factory withAverageFields(String... fields) {
            addFields(this.averageFields, fields);
            return this;
        }
        
        /**
         * Add the given fields into the given set.
         *
         * @param set
         *            the set to add the fields to
         * @param fields
         *            the fields to add
         */
        private void addFields(Set<String> set, Collection<String> fields) {
            if (fields != null) {
                set.addAll(fields);
                allFields.addAll(fields);
            }
        }
        
        private void addFields(Set<String> set, String... fields) {
            addFields(set, Arrays.asList(fields));
        }
        
        /**
         * Return a new {@link FieldAggregator} with the configured target aggregation fields.
         * 
         * @return a new {@link FieldAggregator} instance
         */
        public FieldAggregator newInstance() {
            return hasFieldsToAggregate() ? new FieldAggregator(sumFields, maxFields, minFields, countFields, averageFields) : new FieldAggregator();
        }
        
        /**
         * Return whether this factory has any target aggregation fields set.
         * 
         * @return true if this factory has any target aggregation fields, or false otherwise
         */
        public boolean hasFieldsToAggregate() {
            return !allFields.isEmpty();
        }
        
        /**
         * Return whether the given field matches a target aggregation field in this factory.
         * 
         * @param field
         *            the field
         * @return true if the given field is a target for aggregation, or false otherwise
         */
        public boolean isFieldToAggregate(String field) {
            return allFields.contains(field);
        }
        
        @Override
        public String toString() {
            return new ToStringBuilder(this).append("sumFields", sumFields).append("maxFields", maxFields).append("minFields", minFields)
                            .append("countFields", countFields).append("averageFields", averageFields).append("allFields", allFields).toString();
        }
    }
}
