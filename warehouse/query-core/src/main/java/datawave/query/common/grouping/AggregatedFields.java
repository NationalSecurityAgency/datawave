package datawave.query.common.grouping;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import datawave.query.attributes.Attribute;
import datawave.query.jexl.JexlASTHelper;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang.builder.ToStringBuilder;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * This class provides functionality to aggregate values for specified target fields using specified aggregation operations.
 */
public class AggregatedFields {
    
    private final Multimap<String,Aggregator<?>> aggregatorMap;
    
    public AggregatedFields() {
        aggregatorMap = HashMultimap.create();
    }
    
    public AggregatedFields(Set<String> sumFields, Set<String> maxFields, Set<String> minFields, Set<String> countFields, Set<String> averageFields) {
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
            fields.forEach(field -> aggregatorMap.put(field, constructor.apply(field)));
        }
    }
    
    /**
     * Aggregate each of the given attributes into the aggregators for the given field.
     * 
     * @param field
     *            the target aggregation field
     * @param values
     *            the values to aggregate
     */
    public void aggregateAll(String field, Collection<Attribute<?>> values) {
        if (aggregatorMap.containsKey(field)) {
            Collection<Aggregator<?>> aggregators = this.aggregatorMap.get(field);
            for (Attribute<?> value : values) {
                // Handle multi-value attributes.
                if (value.getData() instanceof Collection<?>) {
                    @SuppressWarnings("unchecked")
                    Collection<Attribute<?>> collection = (Collection<Attribute<?>>) value;
                    aggregators.forEach(aggregator -> aggregator.aggregateAll(collection));
                } else {
                    aggregators.forEach(aggregator -> aggregator.aggregate(value));
                }
            }
        }
    }
    
    /**
     * Return the map of fields to their aggregators.
     * 
     * @return the aggregator map.
     */
    public Multimap<String,Aggregator<?>> getAggregatorMap() {
        return aggregatorMap;
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
     * Returns an unmodifiable set of all column visibilities for all aggregators in this {@link AggregatedFields}. Possibly empty, but never null.
     * 
     * @return the column visibilities
     */
    public Set<ColumnVisibility> getColumnVisibilities() {
        // @formatter:off
        return aggregatorMap.entries().stream()
                        .map(Map.Entry::getValue)
                        .map(Aggregator::getColumnVisibilities)
                        .flatMap(Set::stream)
                        .collect(Collectors.toSet());
        // @formatter:on
    }
    
    /**
     * Merge the given aggregator into this aggregated fields.
     * 
     * @param aggregator
     *            the aggregator to merge.
     */
    public void mergeAggregator(Aggregator<?> aggregator) {
        if (aggregatorMap.containsKey(aggregator.getField())) {
            Optional<Aggregator<?>> optional = aggregatorMap.get(aggregator.getField()).stream()
                            .filter(a -> a.getOperation().equals(aggregator.getOperation())).findFirst();
            if (optional.isPresent()) {
                optional.get().merge(aggregator);
            } else {
                aggregatorMap.put(aggregator.getField(), aggregator);
            }
        } else {
            aggregatorMap.put(aggregator.getField(), aggregator);
        }
    }
    
    /**
     * Merge the given aggregated fields into this aggregated fields.
     * 
     * @param other
     *            the aggregated fields to merge in
     */
    public void merge(AggregatedFields other) {
        for (String field : other.aggregatorMap.keySet()) {
            // If we already have aggregators for this field, merge the aggregators for the current field from the other aggregated fields into this one.
            if (this.aggregatorMap.containsKey(field)) {
                for (Aggregator<?> otherAggregator : other.aggregatorMap.get(field)) {
                    boolean matchFound = false;
                    // If a match is found for the current aggregation operation, merge the aggregators.
                    for (Aggregator<?> aggregator : this.aggregatorMap.get(field)) {
                        if (aggregator.getOperation().equals(otherAggregator.getOperation())) {
                            aggregator.merge(otherAggregator);
                            matchFound = true;
                            break;
                        }
                    }
                    // Otherwise simply add the aggregator to the aggregator map.
                    if (!matchFound) {
                        this.aggregatorMap.put(field, otherAggregator);
                    }
                }
            } else {
                // If no aggregators exist in this aggregated fields for the current field, add all aggregators for it.
                this.aggregatorMap.putAll(field, other.aggregatorMap.get(field));
            }
        }
    }
    
    @Override
    public String toString() {
        return aggregatorMap.toString();
    }
    
    /**
     * A factory that will generate new {@link AggregatedFields} with the designated sum, max, min, count, and average aggregation field targets.
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
            addFields(fields, this.sumFields);
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
            addFields(fields, this.maxFields);
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
            addFields(fields, this.minFields);
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
            addFields(fields, this.countFields);
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
            addFields(fields, this.averageFields);
            return this;
        }
        
        /**
         * Add the given fields into the given set.
         * 
         * @param fields
         *            the fields to add
         * @param set
         *            the set to add the fields to
         */
        private void addFields(Set<String> fields, Set<String> set) {
            if (fields != null) {
                set.addAll(fields);
                allFields.addAll(set);
            }
        }
        
        /**
         * Return a new {@link AggregatedFields} with the configured target aggregation fields.
         * 
         * @return a new {@link AggregatedFields} instance
         */
        public AggregatedFields newInstance() {
            return hasFieldsToAggregate() ? new AggregatedFields(sumFields, maxFields, minFields, countFields, averageFields) : new AggregatedFields();
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
        
        /**
         * Returns a new {@link Factory} instance with all target aggregation fields of this factory after their identifiers are deconstructed.
         * 
         * @return a new {@link Factory} instance with deconstructed fields
         */
        public Factory deconstructIdentifiers() {
            Set<String> sumFields = this.sumFields.stream().map(JexlASTHelper::deconstructIdentifier).collect(Collectors.toSet());
            Set<String> maxFields = this.maxFields.stream().map(JexlASTHelper::deconstructIdentifier).collect(Collectors.toSet());
            Set<String> minFields = this.minFields.stream().map(JexlASTHelper::deconstructIdentifier).collect(Collectors.toSet());
            Set<String> countFields = this.countFields.stream().map(JexlASTHelper::deconstructIdentifier).collect(Collectors.toSet());
            Set<String> averageFields = this.averageFields.stream().map(JexlASTHelper::deconstructIdentifier).collect(Collectors.toSet());
            return new Factory().withSumFields(sumFields).withMaxFields(maxFields).withMinFields(minFields).withCountFields(countFields)
                            .withAverageFields(averageFields);
        }
        
        @Override
        public String toString() {
            return new ToStringBuilder(this).append("sumFields", sumFields).append("maxFields", maxFields).append("minFields", minFields)
                            .append("countFields", countFields).append("averageFields", averageFields).append("allFields", allFields).toString();
        }
    }
}
