package datawave.query.util;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.data.MetadataCardinalityCounts;
import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.Type;
import datawave.marking.MarkingFunctions;
import datawave.query.composite.CompositeMetadataHelper;
import datawave.query.model.QueryModel;
import datawave.util.TableName;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang.time.DateUtils;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class MockMetadataHelper extends MetadataHelper {
    protected final Metadata metadata = new Metadata();
    private Set<String> indexOnlyFields = new HashSet<>();
    private final Set<String> expansionFields = new HashSet<>();
    private final Set<String> contentFields = new HashSet<>();
    private final Set<String> riFields = new HashSet<>();
    private Set<String> nonEventFields = new HashSet<>();
    private final Multimap<String, String> fieldsToDatatype = HashMultimap.create();
    protected Multimap<String, Type<?>> dataTypes = HashMultimap.create();
    protected Map<String, Map<String, MetadataCardinalityCounts>> termCounts = new HashMap<>();
    protected Map<String, QueryModel> models = new HashMap<>();
    protected Map<Map.Entry<String, String>, Map<String, Long>> cardinalityByDataTypeForFieldAndDate = Maps.newHashMap();

    private static final Logger log = Logger.getLogger(MockMetadataHelper.class);

    Function<Type<?>, String> function = new Function<Type<?>, String>() {
        @Override
        @Nullable
        public String apply(@Nullable Type<?> input) {
            return input.getClass().getName();
        }
    };

    public MockMetadataHelper() {
        super(createAllFieldMetadataHelper(getClient()), Collections.emptySet(), getClient(), TableName.METADATA, Collections.emptySet(), Collections
                .emptySet());
    }

    private static AllFieldMetadataHelper createAllFieldMetadataHelper(AccumuloClient client) {
        final Set<Authorizations> allMetadataAuths = Collections.emptySet();
        final Set<Authorizations> auths = Collections.emptySet();
        TypeMetadataHelper tmh = new TypeMetadataHelper(Maps.newHashMap(), allMetadataAuths, client, TableName.METADATA, auths, false);
        CompositeMetadataHelper cmh = new CompositeMetadataHelper(client, TableName.METADATA, auths);
        return new AllFieldMetadataHelper(tmh, cmh, client, TableName.METADATA, auths, allMetadataAuths);

    }

    private static AccumuloClient getClient() {
        try {
            return new InMemoryAccumuloClient("root", new InMemoryInstance());
        } catch (AccumuloSecurityException e) {
            throw new RuntimeException(e);
        }
    }

    public void addContentFields(Collection<String> fields) {
        this.contentFields.addAll(fields);
    }

    public void addDataTypes(Collection<String> dataTypes) throws TableNotFoundException, ExecutionException, MarkingFunctions.Exception {
        getMetadata().datatypes.addAll(dataTypes);
    }

    public void addExpansionFields(Collection<String> fields) {
        this.expansionFields.addAll(fields);
    }

    public void addField(String field, String dt) throws TableNotFoundException, ExecutionException, MarkingFunctions.Exception {
        getMetadata().allFields.add(field);
        try {
            this.dataTypes.put(field, Class.forName(dt).asSubclass(Type.class).newInstance());
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            log.error(e);
        }
    }

    public void addFields(Collection<String> fields) throws TableNotFoundException, ExecutionException, MarkingFunctions.Exception {
        getMetadata().allFields.addAll(fields);
        for (String field : fields) {
            this.dataTypes.put(field, new LcNoDiacriticsType());
        }
    }

    public void addFields(Multimap<String, Type<?>> fields) throws TableNotFoundException, ExecutionException, MarkingFunctions.Exception {
        getMetadata().allFields.addAll(fields.keys());
        for (Map.Entry<String, Type<?>> field : fields.entries()) {
            this.dataTypes.put(field.getKey(), field.getValue());
        }
    }

    public void addFieldsToDatatypes(Multimap<String, String> fieldsToDatatype) throws TableNotFoundException, ExecutionException, MarkingFunctions.Exception {
        getMetadata().allFields.addAll(fieldsToDatatype.keySet());
        for (Map.Entry<String, String> field : fieldsToDatatype.entries()) {
            try {
                this.dataTypes.put(field.getKey(), Class.forName(field.getValue()).asSubclass(Type.class).newInstance());
            } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
                this.fieldsToDatatype.putAll(fieldsToDatatype);
            }
        }
    }

    public void addNormalizers(String field, Collection<Type<?>> normalizerSet) {
        this.dataTypes.putAll(field, normalizerSet);
    }

    public void addTermFrequencyFields(Collection<String> fields) throws TableNotFoundException, ExecutionException, MarkingFunctions.Exception {
        getMetadata().termFrequencyFields.addAll(fields);
    }

    @Override
    public Metadata getMetadata() throws TableNotFoundException, ExecutionException, MarkingFunctions.Exception {
        return getMetadata(null);
    }

    @Override
    public Metadata getMetadata(Set<String> ingestTypeFilter) throws TableNotFoundException, ExecutionException, MarkingFunctions.Exception {
        // TODO: filter this?
        return metadata;
    }

    @Override
    public Set<String> getAllFields(Set<String> ingestTypeFilter) throws TableNotFoundException, ExecutionException, MarkingFunctions.Exception {
        Set<String> fields = new HashSet<>();

        if (ingestTypeFilter == null) {
            return Collections.unmodifiableSet(getMetadata().getAllFields());
        } else if (!ingestTypeFilter.isEmpty()) {
            for (Map.Entry<String, String> entry : fieldsToDatatype.entries()) {
                if (ingestTypeFilter.contains(entry.getValue())) {
                    fields.add(entry.getKey());
                }
            }
        }
        return Collections.unmodifiableSet(fields);
    }

    @Override
    public Set<String> getNonEventFields(Set<String> ingestTypeFilter) {
        return nonEventFields;
    }

    @Override
    public Set<String> getIndexOnlyFields(Set<String> ingestTypeFilter) {
        return indexOnlyFields;
    }

    @Override
    public QueryModel getQueryModel(String modelTableName, String modelName, Collection<String> unevaluatedFields) {
        return models.get(modelName);
    }

    @Override
    public boolean isIndexed(String fieldName, Set<String> ingestTypeFilter) throws TableNotFoundException, ExecutionException, MarkingFunctions.Exception {
        // TODO: should try to observe the ingestTypeFilter as well
        return getMetadata().indexedFields.contains(fieldName);
    }

    @Override
    public boolean isReverseIndexed(String fieldName, Set<String> ingestTypeFilter) {
        return this.riFields.contains(fieldName);
    }

    @Override
    public Map<String, Map<String, MetadataCardinalityCounts>> getTermCounts() {
        return termCounts;
    }

    @Override
    public Map<String, Map<String, MetadataCardinalityCounts>> getTermCountsWithRootAuths() {
        return termCounts;
    }

    @Override
    public Set<String> getAllNormalized() throws TableNotFoundException, ExecutionException, MarkingFunctions.Exception {
        return getMetadata().getNormalizedFields();
    }

    @Override
    public Set<Type<?>> getAllDatatypes() {
        return Sets.newHashSet(dataTypes.values());
    }

    @Override
    public Set<Type<?>> getDatatypesForField(String fieldName) throws InstantiationException, IllegalAccessException, TableNotFoundException {
        return getDatatypesForField(fieldName, null);
    }

    @Override
    public Set<Type<?>> getDatatypesForField(String fieldName, Set<String> ingestTypeFilter) {
        if (ingestTypeFilter == null) {
            return new HashSet<>(dataTypes.get(fieldName));
        } else if (!ingestTypeFilter.isEmpty()) {
            Set<Type<?>> types = new HashSet<>(dataTypes.get(fieldName));
            types.removeAll(ingestTypeFilter);
            return types;
        }
        return Collections.emptySet();
    }

    @Override
    public TypeMetadata getTypeMetadata(Set<String> ingestTypeFilter) throws TableNotFoundException {
        TypeMetadata typeMetadata = new TypeMetadata();
        for (String fieldName : dataTypes.keySet()) {
            try {
                typeMetadata.put(fieldName, "test",
                        getDatatypesForField(fieldName)
                                .stream()
                                .map(function)
                                .collect(Collectors.toList())
                                .iterator()
                                .next());
            } catch (InstantiationException | IllegalAccessException e) {
                log.error(e);
            }
        }
        return typeMetadata;
    }

    @Override
    public Multimap<String, Type<?>> getFieldsToDatatypes(Set<String> ingestTypeFilter) {
        Multimap<String, Type<?>> multimap = ArrayListMultimap.create();
        for (String field : dataTypes.keySet()) {
            multimap.putAll(field, getDatatypesForField(field, ingestTypeFilter));
        }
        return multimap;
    }

    @Override
    public Set<String> getFieldsForDatatype(Class<? extends Type<?>> datawaveType, Set<String> ingestTypeFilter) {
        Set<String> fields = new HashSet<>();
        for (String field : dataTypes.keySet()) {
            for (Type<?> type : dataTypes.get(field)) {
                if (datawaveType.isInstance(type)) {
                    fields.add(field);
                    break;
                }
            }
        }
        return fields;
    }

    @Override
    public Set<String> getTermFrequencyFields(Set<String> ingestTypeFilter) throws TableNotFoundException, ExecutionException, MarkingFunctions.Exception {
        return getMetadata().getTermFrequencyFields();
    }

    @Override
    public Set<String> getIndexedFields(Set<String> ingestTypeFilter) throws TableNotFoundException, ExecutionException, MarkingFunctions.Exception {
        return getMetadata().getIndexedFields();
    }

    @Override
    public Set<String> getExpansionFields(Set<String> ingestTypeFilter) {
        return this.expansionFields;
    }

    @Override
    public Set<String> getContentFields(Set<String> ingestTypeFilter) {
        return this.contentFields;
    }

    @Override
    public long getCardinalityForField(String fieldName, String datatype, Date begin, Date end) {
        throw new UnsupportedOperationException("not imeplemented in MockMetadataHelper");
    }

    @Override
    public Set<String> getDatatypes(Set<String> ingestTypeFilter) throws TableNotFoundException, ExecutionException, MarkingFunctions.Exception {
        if (ingestTypeFilter == null) {
            return getMetadata().getDatatypes();
        } else if (!ingestTypeFilter.isEmpty()) {
            Set<String> types = new HashSet<>(getMetadata().getDatatypes());
            types.retainAll(ingestTypeFilter);
            return types;
        }
        return Collections.emptySet();
    }

    @Override
    public Long getCountsByFieldForDays(String fieldName, Date begin, Date end, Set<String> ingestTypeFilter) {
        Preconditions.checkNotNull(fieldName);
        Preconditions.checkNotNull(begin);
        Preconditions.checkNotNull(end);
        Preconditions.checkArgument(begin.before(end));
        Preconditions.checkNotNull(ingestTypeFilter);

        Date truncatedBegin = DateUtils.truncate(begin, Calendar.DATE);
        Date truncatedEnd = DateUtils.truncate(end, Calendar.DATE);

        if (truncatedEnd.getTime() != end.getTime()) {
            // If we don't have the same time for both, we actually tuncated the end,
            // and we want to bump the date range to include the entire end.
            truncatedEnd = new Date(truncatedEnd.getTime() + 86400000);
        }

        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        cal.setTime(truncatedBegin);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        long sum = 0L;
        while (cal.getTime().before(truncatedEnd)) {
            Date curDate = cal.getTime();
            String desiredDate = sdf.format(curDate);

            sum += getCountsByFieldInDayWithTypes(fieldName, desiredDate, ingestTypeFilter);
            cal.add(Calendar.DATE, 1);
        }

        return sum;
    }

    @Override
    public Long getCountsByFieldInDayWithTypes(String fieldName, String date, final Set<String> datatypes) {
        Preconditions.checkNotNull(fieldName);
        Preconditions.checkNotNull(date);
        Preconditions.checkNotNull(datatypes);

        Map<String, Long> countsByType = this.cardinalityByDataTypeForFieldAndDate.get(Maps.immutableEntry(fieldName, date));

        if (null == countsByType) {
            return 0L;
        }

        Iterable<Map.Entry<String, Long>> filteredByType = Iterables.filter(countsByType.entrySet(), input -> datatypes.contains(input.getKey()));

        long sum = 0;
        for (Map.Entry<String, Long> entry : filteredByType) {
            sum += entry.getValue();
        }

        return sum;
    }

    @Override
    protected Multimap<String, String> loadAllFields() {
        return HashMultimap.create();
    }

    @Override
    protected Multimap<String, String> loadIndexOnlyFields() throws TableNotFoundException {
        return super.loadIndexOnlyFields();
    }

    @Override
    protected Multimap<String, String> loadTermFrequencyFields() throws TableNotFoundException {
        return super.loadTermFrequencyFields();
    }

    public void setCardinalities(Map<Map.Entry<String, String>, Map<String, Long>> cardinalities) {
        this.cardinalityByDataTypeForFieldAndDate = cardinalities;
    }

    public void setDataTypes(Multimap<String, Type<?>> dataTypes) {
        this.dataTypes = dataTypes;
    }

    public void setIndexedFields(Set<String> indexedFields) throws TableNotFoundException, ExecutionException, MarkingFunctions.Exception {
        getMetadata().indexedFields = indexedFields;
    }

    public void setNonEventFields(Set<String> fields) {
        this.nonEventFields = fields;
    }

    public void setIndexOnlyFields(Set<String> indexOnlyFields) {
        this.indexOnlyFields = indexOnlyFields;
    }

    public void setNormalizedFields(Set<String> normalizedFields) throws TableNotFoundException, ExecutionException, MarkingFunctions.Exception {
        getMetadata().normalizedFields = normalizedFields;
    }

    public void setQueryModel(String modelName, QueryModel model) {
        models.put(modelName, model);
    }

    public void setReverseIndexFields(Set<String> riFields) {
        this.riFields.addAll(riFields);
    }

    public void setTermCounts(Map<String, Map<String, MetadataCardinalityCounts>> counts) {
        this.termCounts = counts;
    }

}
