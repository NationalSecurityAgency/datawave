package datawave.query.common.grouping;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import datawave.query.attributes.Attribute;
import datawave.query.jexl.JexlASTHelper;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang.builder.ToStringBuilder;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

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
    
    private void populateAggregators(Set<String> fields, Function<String,Aggregator<?>> constructor) {
        if (fields != null) {
            fields.forEach(field -> aggregatorMap.put(field, constructor.apply(field)));
        }
    }
    
    /**
     * Aggregates the given attribute into all aggregators established for the given field.
     * 
     * @param field
     *            the field to aggregate by
     * @param value
     *            the value to aggregate
     */
    public void aggregate(String field, Attribute<?> value) {
        if (aggregatorMap.containsKey(field)) {
            Collection<Aggregator<?>> aggregators = this.aggregatorMap.get(field);
            // Handle multi-value attributes.
            if (value.getData() instanceof Collection<?>) {
                Collection<Attribute<?>> values = (Collection<Attribute<?>>) value;
                aggregators.forEach(aggregator -> aggregator.aggregateAll(values));
            } else {
                aggregators.forEach(aggregator -> aggregator.aggregate(value));
            }
        }
    }
    
    public void aggregateAll(String field, Collection<Attribute<?>> values) {
        if (aggregatorMap.containsKey(field)) {
            Collection<Aggregator<?>> aggregators = this.aggregatorMap.get(field);
            for (Attribute<?> value : values) {
                // Handle multi-value attributes.
                if (value.getData() instanceof Collection<?>) {
                    Collection<Attribute<?>> collection = (Collection<Attribute<?>>) value;
                    aggregators.forEach(aggregator -> aggregator.aggregateAll(collection));
                } else {
                    aggregators.forEach(aggregator -> aggregator.aggregate(value));
                }
            }
        }
    }
    
    public Multimap<String,Aggregator<?>> getAggregatorMap() {
        return aggregatorMap;
    }
    
    public Map<AggregateOperation,Aggregator<?>> getAggregatorsForField(String field) {
        Map<AggregateOperation,Aggregator<?>> map = new HashMap<>();
        for (Aggregator<?> aggregator : aggregatorMap.get(field)) {
            map.put(aggregator.getOperation(), aggregator);
        }
        return map;
    }
    
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
    
    public boolean isEmpty() {
        return aggregatorMap.isEmpty();
    }
    
    @Override
    public String toString() {
        return aggregatorMap.toString();
    }
    
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
    
    public void merge(AggregatedFields other) {
        for (String field : other.aggregatorMap.keySet()) {
            if (this.aggregatorMap.containsKey(field)) {
                for (Aggregator<?> otherAggregator : other.aggregatorMap.get(field)) {
                    boolean matchFound = false;
                    for (Aggregator<?> aggregator : this.aggregatorMap.get(field)) {
                        if (aggregator.getOperation().equals(otherAggregator.getOperation())) {
                            aggregator.merge(otherAggregator);
                            matchFound = true;
                            break;
                        }
                    }
                    if (!matchFound) {
                        this.aggregatorMap.put(field, otherAggregator);
                    }
                }
            } else {
                this.aggregatorMap.putAll(field, other.aggregatorMap.get(field));
            }
        }
    }
    
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
        
        public Factory withSumFields(Set<String> fields) {
            addFields(fields, this.sumFields);
            return this;
        }
        
        public Factory withMaxFields(Set<String> fields) {
            addFields(fields, this.maxFields);
            return this;
        }
        
        public Factory withMinFields(Set<String> fields) {
            addFields(fields, this.minFields);
            return this;
        }
        
        public Factory withCountFields(Set<String> fields) {
            addFields(fields, this.countFields);
            return this;
        }
        
        public Factory withAverageFields(Set<String> fields) {
            addFields(fields, this.averageFields);
            return this;
        }
        
        private void addFields(Set<String> newFields, Set<String> fieldSet) {
            if (newFields != null) {
                fieldSet.addAll(newFields);
                allFields.addAll(fieldSet);
            }
        }
        
        public AggregatedFields newInstance() {
            return hasFieldsToAggregate() ? new AggregatedFields(sumFields, maxFields, minFields, countFields, averageFields) : new AggregatedFields();
        }
        
        public boolean hasFieldsToAggregate() {
            return !allFields.isEmpty();
        }
        
        public boolean isFieldToAggregate(String field) {
            return allFields.contains(field);
        }
        
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
