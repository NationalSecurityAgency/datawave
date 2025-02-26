package datawave.query.util;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TimeZone;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.ValueFormatException;
import org.apache.accumulo.core.iterators.user.RegExFilter;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.protobuf.InvalidProtocolBufferException;

import datawave.data.ColumnFamilyConstants;
import datawave.data.MetadataCardinalityCounts;
import datawave.data.type.Type;
import datawave.iterators.EdgeMetadataCombiner;
import datawave.iterators.MetadataFColumnSeekingFilter;
import datawave.iterators.filter.EdgeMetadataCQStrippingIterator;
import datawave.marking.MarkingFunctions;
import datawave.query.composite.CompositeMetadata;
import datawave.query.model.Direction;
import datawave.query.model.FieldMapping;
import datawave.query.model.IndexFieldHole;
import datawave.query.model.ModelKeyParser;
import datawave.query.model.QueryModel;
import datawave.security.util.AuthorizationsMinimizer;
import datawave.security.util.ScannerHelper;
import datawave.util.StringUtils;
import datawave.util.UniversalSet;
import datawave.util.time.DateHelper;
import datawave.util.time.TraceStopwatch;
import datawave.webservice.common.connection.WrappedAccumuloClient;

/**
 * <p>
 * Helper class to fetch the set of field names which are only indexed, i.e. do not occur as attributes in the event.
 * </p>
 * 
 * <p>
 * This set would normally includes all tokenized content fields. In terms of keys in the DatawaveMetadata table, this set would contain all rows in the
 * {@code DatawaveMetadata} table which have a {@link ColumnFamilyConstants#COLF_I} but not a {@link ColumnFamilyConstants#COLF_E}
 * </p>
 * 
 * 
 * TODO -- Break this class apart
 * 
 */
@EnableCaching
@Component("metadataHelper")
@Scope("prototype")
public class MetadataHelper {
    private static final Logger log = LoggerFactory.getLogger(MetadataHelper.class);
    
    public static final String NULL_BYTE = "\0";
    
    protected static final Text PV = new Text("pv");
    
    protected static final Function<MetadataEntry,String> toFieldName = new MetadataEntryToFieldName();
    protected static final Function<MetadataEntry,String> toDatatype = new MetadataEntryToDatatype();
    
    protected String getDatatype(Key k) {
        String datatype = k.getColumnQualifier().toString();
        int index = datatype.indexOf('\0');
        if (index >= 0) {
            datatype = datatype.substring(0, index);
        }
        return datatype;
    }
    
    protected final Metadata metadata = new Metadata();
    
    protected final List<Text> metadataIndexColfs = Arrays.asList(ColumnFamilyConstants.COLF_I, ColumnFamilyConstants.COLF_RI);
    protected final List<Text> metadataNormalizedColfs = Arrays.asList(ColumnFamilyConstants.COLF_N);
    protected final List<Text> metadataTypeColfs = Arrays.asList(ColumnFamilyConstants.COLF_T);
    protected final List<Text> metadataCompositeIndexColfs = Arrays.asList(ColumnFamilyConstants.COLF_CI);
    protected final List<Text> metadataCardinalityColfs = Arrays.asList(ColumnFamilyConstants.COLF_COUNT);
    
    protected final AccumuloClient accumuloClient;
    protected final String metadataTableName;
    protected final Set<Authorizations> auths;
    protected Set<Authorizations> fullUserAuths;
    
    protected final AllFieldMetadataHelper allFieldMetadataHelper;
    protected final Collection<Authorizations> allMetadataAuths;
    
    // a set of fields that are dynamically created at evaluation time, and are not registered in the metadata table
    protected Set<String> evaluationOnlyFields = Collections.emptySet();
    
    public MetadataHelper(AllFieldMetadataHelper allFieldMetadataHelper, Collection<Authorizations> allMetadataAuths, AccumuloClient client,
                    String metadataTableName, Set<Authorizations> auths, Set<Authorizations> fullUserAuths) {
        Preconditions.checkNotNull(allFieldMetadataHelper, "An AllFieldMetadataHelper is required by MetadataHelper");
        this.allFieldMetadataHelper = allFieldMetadataHelper;
        
        Preconditions.checkNotNull(allMetadataAuths, "The set of all metadata authorization is required by MetadataHelper");
        this.allMetadataAuths = allMetadataAuths;
        
        Preconditions.checkNotNull(client, "A valid AccumuloClient is required by MetadataHelper");
        this.accumuloClient = client;
        
        Preconditions.checkNotNull(metadataTableName, "The name of the metadata table is required by MetadataHelper");
        this.metadataTableName = metadataTableName;
        
        Preconditions.checkNotNull(auths, "Authorizations are required by MetadataHelper");
        this.auths = auths;
        
        Preconditions.checkNotNull(fullUserAuths, "The full set of user authorizations is required by MetadataHelper");
        this.fullUserAuths = fullUserAuths;
        log.debug("initialized with auths subset: {}", this.auths);
        
        log.trace("Constructor client: {} with auths: {} and metadata table name: {}", accumuloClient.getClass().getCanonicalName(), auths, metadataTableName);
    }
    
    /**
     * allMetadataAuths is a singleton Collection of one Authorizations instance that contains all the auths required to see everything in the Metadata table.
     * <p>
     * This is effectively a <code>userAuths.containsAll(metadataAuths)</code> call.
     * 
     * @param usersAuthsCollection
     *            the user authorizations
     * @param allMetadataAuthsCollection
     *            all metadata authorizations
     * @return true if the user has all metadata authorizations
     */
    public static boolean userHasAllMetadataAuths(Collection<Authorizations> usersAuthsCollection, Collection<Authorizations> allMetadataAuthsCollection) {
        
        // first, minimize the usersAuths:
        Collection<Authorizations> minimizedCollection = AuthorizationsMinimizer.minimize(usersAuthsCollection);
        // now, the first entry in the minimized auths should have everything common to every Authorizations in the set
        // make sure that the first entry contains all the Authorizations in the allMetadataAuths
        Authorizations allMetadataAuths = allMetadataAuthsCollection.iterator().next(); // get the first (and only) one
        Authorizations minimized = minimizedCollection.iterator().next(); // get the first one, which has all auths common to all in the original collection
        return StreamSupport.stream(minimized.spliterator(), false).map(String::new).collect(Collectors.toSet())
                        .containsAll(StreamSupport.stream(allMetadataAuths.spliterator(), false).map(String::new).collect(Collectors.toSet()));
    }
    
    /**
     * allMetadataAuthsCollection is a singleton Collection of one Authorizations instance that contains all of the auths required to see everything in the
     * Metadata table. userAuthsCollection contains the user's auths. This method will return the retention of the user's auths from the
     * allMetadataAuthsCollection.
     *
     * @param usersAuthsCollection
     *            the user authorizations
     * @param allMetadataAuthsCollection
     *            all metadata authorizations
     * @return the user auths that match the metadata auths
     */
    public static Collection<String> getUsersMetadataAuthorizationSubset(Collection<Authorizations> usersAuthsCollection,
                    Collection<Authorizations> allMetadataAuthsCollection) {
        if (log.isTraceEnabled()) {
            log.trace("allMetadataAuthsCollection: {}", allMetadataAuthsCollection);
            log.trace("usersAuthsCollection: {}", usersAuthsCollection);
        }
        // first, minimize the usersAuths:
        Collection<Authorizations> minimizedCollection = AuthorizationsMinimizer.minimize(usersAuthsCollection);
        if (log.isTraceEnabled()) {
            log.trace("minimizedCollection: {}", minimizedCollection);
        }
        
        // now, the first entry in the minimized auths should have everything common to every Authorizations in the set
        // make sure that the first entry contains all the Authorizations in the allMetadataAuths
        Authorizations allMetadataAuths = allMetadataAuthsCollection.iterator().next(); // get the first (and only) one
        Authorizations minimized = minimizedCollection.iterator().next(); // get the first one, which has all auths common to all in the original collection
        if (log.isTraceEnabled()) {
            log.trace("first of users auths minimized: {}", minimized);
        }
        Set<String> minimizedUserAuths = StreamSupport.stream(minimized.spliterator(), false).map(String::new).collect(Collectors.toSet());
        
        Collection<String> minimizedAllMetadataAuths = StreamSupport.stream(allMetadataAuths.spliterator(), false).map(String::new).collect(Collectors.toSet());
        minimizedAllMetadataAuths.retainAll(minimizedUserAuths);
        if (log.isTraceEnabled()) {
            log.trace("minimized to: {}", minimizedAllMetadataAuths);
        }
        return minimizedAllMetadataAuths;
    }
    
    /**
     * Calculates and returns the power set of metadata authorizations
     *
     * @param allMetadataAuthsCollection
     *            all metadata auths
     * @return a set containing every possible combination of metadata auths
     */
    private Set<Set<String>> getAllMetadataAuthsPowerSet(Collection<Authorizations> allMetadataAuthsCollection) {
        
        // first, minimize the usersAuths:
        Collection<Authorizations> minimizedCollection = AuthorizationsMinimizer.minimize(allMetadataAuthsCollection);
        // now, the first entry in the minimized auths should have everything common to every Authorizations in the set
        // make sure that the first entry contains all the Authorizations in the allMetadataAuths
        Authorizations minimized = minimizedCollection.iterator().next(); // get the first one, which has all auths common to all in the original collection
        Set<String> minimizedUserAuths = StreamSupport.stream(minimized.spliterator(), false).map(String::new).collect(Collectors.toSet());
        if (log.isDebugEnabled()) {
            log.debug("minimizedUserAuths: {} with size {}", minimizedUserAuths, minimizedUserAuths.size());
        }
        Set<Set<String>> powerset = Sets.powerSet(minimizedUserAuths);
        Set<Set<String>> set = Sets.newHashSet();
        for (Set<String> sub : powerset) {
            Set<String> serializableSet = Sets.newHashSet(sub);
            set.add(serializableSet);
        }
        return set;
    }
    
    /**
     * Get the mapping of datatypes to TypeMetadata
     *
     * @return a mapping of datatypes to TypeMetadata
     * @throws TableNotFoundException
     *             if no table exists
     */
    public Map<Set<String>,TypeMetadata> getTypeMetadataMap() throws TableNotFoundException {
        Collection<Set<String>> powerset = getAllMetadataAuthsPowerSet(this.allMetadataAuths);
        if (log.isTraceEnabled()) {
            log.trace("powerset: {}", powerset);
        }
        Map<Set<String>,TypeMetadata> map = Maps.newHashMap();
        
        for (Set<String> a : powerset) {
            if (log.isTraceEnabled()) {
                log.trace("get TypeMetadata with auths: {}", a);
            }
            
            Authorizations at = new Authorizations(a.toArray(new String[0]));
            
            if (log.isTraceEnabled()) {
                log.trace("made an Authorizations: {}", at);
            }
            TypeMetadata tm = this.allFieldMetadataHelper.getTypeMetadataHelper().getTypeMetadataForAuths(Collections.singleton(at));
            map.put(a, tm);
        }
        return map;
    }
    
    public String getUsersMetadataAuthorizationSubset() {
        StringBuilder buf = new StringBuilder();
        if (this.auths != null && this.allMetadataAuths != null) {
            for (String auth : MetadataHelper.getUsersMetadataAuthorizationSubset(this.auths, this.allMetadataAuths)) {
                if (buf.length() != 0) {
                    buf.append("&");
                }
                buf.append(auth);
            }
        }
        return buf.toString();
    }
    
    public Collection<Authorizations> getAllMetadataAuths() {
        return allMetadataAuths;
    }
    
    public Set<Authorizations> getAuths() {
        return auths;
    }
    
    public Set<Authorizations> getFullUserAuths() {
        return fullUserAuths;
    }
    
    public AllFieldMetadataHelper getAllFieldMetadataHelper() {
        return this.allFieldMetadataHelper;
    }
    
    /**
     * Get the metadata for all ingest types
     *
     * @return the Metadata for all ingest types
     * @throws TableNotFoundException
     *             if no table exists
     * @throws ExecutionException
     *             if there is a problem scanning accumulo
     */
    public Metadata getMetadata() throws TableNotFoundException, ExecutionException, MarkingFunctions.Exception {
        return getMetadata(null);
    }
    
    /**
     * Get the metadata for a subset of ingest types
     *
     * @return the Metadata for a subset of ingest types
     * @throws TableNotFoundException
     *             if no table exists
     * @throws ExecutionException
     *             if there is a problem scanning accumulo
     * @throws MarkingFunctions.Exception
     *             it can't, remove this
     */
    public Metadata getMetadata(Set<String> ingestTypeFilter) throws TableNotFoundException, ExecutionException, MarkingFunctions.Exception {
        return new Metadata(this, ingestTypeFilter);
    }
    
    /**
     * Fetch the {@link Set} of all fields contained in the database. This will provide a cached view of the fields which is updated every
     * {@code updateInterval} milliseconds.
     *
     * @param ingestTypeFilter
     *            set of ingest types used to restrict the scan
     * @return all fields in the metadata table
     * @throws TableNotFoundException
     *             if no table exists
     */
    public Set<String> getAllFields(Set<String> ingestTypeFilter) throws TableNotFoundException {
        Multimap<String,String> allFields = this.allFieldMetadataHelper.loadAllFields();
        if (log.isTraceEnabled()) {
            log.trace("loadAllFields() with auths: {} returned {}", allFieldMetadataHelper.getAuths(), allFields);
        }
        
        Set<String> fields = new HashSet<>();
        if (ingestTypeFilter == null || ingestTypeFilter.isEmpty()) {
            fields.addAll(allFields.values());
        } else {
            for (String datatype : ingestTypeFilter) {
                fields.addAll(allFields.get(datatype));
            }
        }
        
        // Add any additional fields that are created at evaluation time and are hence not in the metadata table.
        fields.addAll(evaluationOnlyFields);
        
        if (log.isTraceEnabled()) {
            log.trace("getAllFields({}) returning {}", ingestTypeFilter, fields);
        }
        return Collections.unmodifiableSet(fields);
    }
    
    public Set<String> getEvaluationOnlyFields() {
        return Collections.unmodifiableSet(evaluationOnlyFields);
    }
    
    /**
     * Set the evaluation only fields
     * 
     * @param evaluationOnlyFields
     *            a collection of evaluation only fields
     */
    public void setEvaluationOnlyFields(Set<String> evaluationOnlyFields) {
        this.evaluationOnlyFields = (evaluationOnlyFields == null ? Collections.emptySet() : new HashSet<>(evaluationOnlyFields));
    }
    
    /**
     * Get the fields that have values not in the same form as the event (excluding normalization). This would include index only fields, term frequency fields
     * (as the index may contain tokens), and composite fields.
     * 
     * @param ingestTypeFilter
     *            set of ingest types used to restrict the scan
     * @return the non-event fields
     * @throws TableNotFoundException
     *             if no table exists
     */
    public Set<String> getNonEventFields(Set<String> ingestTypeFilter) throws TableNotFoundException {
        
        Set<String> fields = new HashSet<>();
        fields.addAll(getIndexOnlyFields(ingestTypeFilter));
        fields.addAll(getTermFrequencyFields(ingestTypeFilter));
        Multimap<String,String> compToFieldMap = getCompositeToFieldMap(ingestTypeFilter);
        for (String compField : compToFieldMap.keySet()) {
            if (!isOverloadedCompositeField(compToFieldMap, compField)) {
                // a composite is only a non-event field if it is composed from 1 or more non-event fields
                for (String componentField : compToFieldMap.get(compField)) {
                    if (fields.contains(componentField)) {
                        fields.add(compField);
                        break;
                    }
                }
            }
        }
        
        return Collections.unmodifiableSet(fields);
    }
    
    static boolean isOverloadedCompositeField(Multimap<String,String> compositeFieldDefinitions, String compositeFieldName) {
        return isOverloadedCompositeField(compositeFieldDefinitions.get(compositeFieldName), compositeFieldName);
    }
    
    static boolean isOverloadedCompositeField(Collection<String> compFields, String compositeFieldName) {
        if (compFields != null && !compFields.isEmpty())
            return compFields.stream().findFirst().get().equals(compositeFieldName);
        return false;
    }
    
    /**
     * Fetch the {@link Set} of index-only fields.
     *
     * @param ingestTypeFilter
     *            set of ingest types used to restrict the scan
     * @return the set of fields matching the ingest type filter
     * @throws TableNotFoundException
     *             if no table exists
     */
    public Set<String> getIndexOnlyFields(Set<String> ingestTypeFilter) throws TableNotFoundException {
        
        Multimap<String,String> indexOnlyFields = this.allFieldMetadataHelper.getIndexOnlyFields();
        
        Set<String> fields = new HashSet<>();
        if (ingestTypeFilter == null || ingestTypeFilter.isEmpty()) {
            fields.addAll(indexOnlyFields.values());
        } else {
            for (String datatype : ingestTypeFilter) {
                fields.addAll(indexOnlyFields.get(datatype));
            }
        }
        return Collections.unmodifiableSet(fields);
    }
    
    /**
     * Get a QueryModel from the specified table
     * 
     * @param modelTableName
     *            the query model table
     * @param modelName
     *            the query model name
     * @return the QueryModel
     * @throws TableNotFoundException
     *             if no table exists
     * @throws ExecutionException
     *             it can't, remove this
     */
    public QueryModel getQueryModel(String modelTableName, String modelName) throws TableNotFoundException, ExecutionException {
        return getQueryModel(modelTableName, modelName, this.getIndexOnlyFields(null));
    }
    
    public QueryModel getQueryModel(String modelTableName, String modelName, Collection<String> unevaluatedFields) throws TableNotFoundException {
        return getQueryModel(modelTableName, modelName, unevaluatedFields, null);
    }
    
    /***
     * @param modelName
     * @return
     * @throws TableNotFoundException
     */
    @Cacheable(value = "getQueryModel", key = "{#root.target.auths,#modelTableName,#modelName,#unevaluatedFields,#ingestTypeFilter}",
                    cacheManager = "metadataHelperCacheManager")
    public QueryModel getQueryModel(String modelTableName, String modelName, Collection<String> unevaluatedFields, Set<String> ingestTypeFilter)
                    throws TableNotFoundException {
        log.debug("cache fault for getQueryModel({}, {}, {}, {}, {})", this.auths, modelTableName, modelName, unevaluatedFields, ingestTypeFilter);
        Preconditions.checkNotNull(modelTableName);
        Preconditions.checkNotNull(modelName);
        
        if (log.isTraceEnabled()) {
            log.trace("getQueryModel({}, {}, {}, {})", modelTableName, modelName, unevaluatedFields, ingestTypeFilter);
        }
        
        QueryModel queryModel = new QueryModel();
        
        TraceStopwatch stopWatch = new TraceStopwatch("MetadataHelper -- Building Query Model from instance");
        stopWatch.start();
        
        if (log.isTraceEnabled()) {
            log.trace("using client: {} with auths: {} and model table name: {} looking at model {}} unevaluatedFields {}",
                            accumuloClient.getClass().getCanonicalName(), auths, modelTableName, modelName, unevaluatedFields);
        }
        
        try (Scanner scan = ScannerHelper.createScanner(accumuloClient, modelTableName, auths)) {
            scan.setRange(new Range());
            scan.fetchColumnFamily(new Text(modelName));
            // We need the entire Model so we can do both directions.
            final Set<String> allFields = this.getAllFields(ingestTypeFilter);
            
            for (Entry<Key,Value> entry : scan) {
                try {
                    FieldMapping mapping = ModelKeyParser.parseKey(entry.getKey(), entry.getValue());
                    if (!mapping.isFieldMapping()) {
                        queryModel.setModelFieldAttributes(mapping.getModelFieldName(), mapping.getAttributes());
                    } else if (mapping.getDirection() == Direction.FORWARD) {
                        // If a direct match is found for the field in the database, add a forward mapping entry.
                        if (allFields.contains(mapping.getFieldName())) {
                            queryModel.addTermToModel(mapping.getModelFieldName(), mapping.getFieldName());
                        } else {
                            // If a direct match was not found for the field name, it's possible that a regex pattern was supplied. Attempt to find matches
                            // based off matching against the field name as a pattern.
                            Pattern pattern = Pattern.compile(mapping.getFieldName());
                            Set<String> matches = allFields.stream().filter(field -> pattern.matcher(field).matches()).collect(Collectors.toSet());
                            if (!matches.isEmpty()) {
                                matches.forEach(field -> queryModel.addTermToModel(mapping.getModelFieldName(), field));
                            } else {
                                if (log.isTraceEnabled()) {
                                    log.trace("Ignoring forward mapping of {} for {} because the metadata table has no reference to it", mapping.getFieldName(),
                                                    mapping.getModelFieldName());
                                }
                            }
                        }
                    } else {
                        queryModel.addTermToReverseModel(mapping.getFieldName(), mapping.getModelFieldName());
                    }
                } catch (IllegalArgumentException iae) {
                    log.warn("Ignoring unparseable key {}", entry.getKey());
                }
            }
        }
        
        if (queryModel.getReverseQueryMapping().isEmpty()) {
            if (log.isTraceEnabled()) {
                log.trace("empty query model for {}", this);
            }
            if ("DatawaveMetadata".equals(modelTableName)) {
                log.warn("Query Model {} has no reverse mappings", modelName);
            }
        }
        
        stopWatch.stop();
        
        return queryModel;
    }
    
    /***
     * @param modelTableName
     * @return a list of query model names
     * @throws TableNotFoundException
     */
    @Cacheable(value = "getQueryModelNames", key = "{#root.target.auths,#modelTableName}", cacheManager = "metadataHelperCacheManager")
    public Set<String> getQueryModelNames(String modelTableName) throws TableNotFoundException {
        Preconditions.checkNotNull(modelTableName);
        
        if (log.isTraceEnabled()) {
            log.trace("getQueryModelNames({})", modelTableName);
        }
        
        TraceStopwatch stopWatch = new TraceStopwatch("MetadataHelper -- Getting query model names");
        stopWatch.start();
        
        if (log.isTraceEnabled()) {
            log.trace("using client: {} with auths: {} and model table name: {}", accumuloClient.getClass().getCanonicalName(), auths, modelTableName);
        }
        
        Set<String> modelNames = new HashSet<>();
        try (Scanner scan = ScannerHelper.createScanner(accumuloClient, modelTableName, auths)) {
            scan.setRange(new Range());
            Set<Text> ignoreColfs = new HashSet<>();
            ignoreColfs.addAll(metadataIndexColfs);
            ignoreColfs.addAll(metadataNormalizedColfs);
            ignoreColfs.addAll(metadataTypeColfs);
            ignoreColfs.addAll(metadataCompositeIndexColfs);
            ignoreColfs.addAll(metadataCardinalityColfs);
            ignoreColfs.add(ColumnFamilyConstants.COLF_E);
            ignoreColfs.add(ColumnFamilyConstants.COLF_DESC);
            ignoreColfs.add(ColumnFamilyConstants.COLF_EDGE);
            ignoreColfs.add(ColumnFamilyConstants.COLF_F);
            ignoreColfs.add(ColumnFamilyConstants.COLF_H);
            ignoreColfs.add(ColumnFamilyConstants.COLF_VI);
            ignoreColfs.add(ColumnFamilyConstants.COLF_TF);
            ignoreColfs.add(ColumnFamilyConstants.COLF_VERSION);
            ignoreColfs.add(ColumnFamilyConstants.COLF_EXP);
            
            for (Entry<Key,Value> entry : scan) {
                Text cf = entry.getKey().getColumnFamily();
                Text cq = entry.getKey().getColumnQualifier();
                
                if (!ignoreColfs.contains(cf) && cq.toString().endsWith("\0forward")) {
                    modelNames.add(cf.toString());
                }
                
            }
        }
        
        stopWatch.stop();
        
        return modelNames;
    }
    
    /**
     * Determines whether a field has been reverse indexed by looking for the ri column in the metadata table
     * 
     * @param fieldName
     *            the field
     * @param ingestTypeFilter
     *            the ingest type filter
     * @return true if the field is indexed for the provided filter
     * @throws TableNotFoundException
     *             if the table does not exist
     */
    public boolean isReverseIndexed(String fieldName, Set<String> ingestTypeFilter) throws TableNotFoundException {
        Preconditions.checkNotNull(fieldName);
        Preconditions.checkNotNull(ingestTypeFilter);
        
        Entry<String,Entry<String,Set<String>>> entry = Maps.immutableEntry(metadataTableName, Maps.immutableEntry(fieldName, ingestTypeFilter));
        
        try {
            return this.allFieldMetadataHelper.isIndexed(ColumnFamilyConstants.COLF_RI, entry);
        } catch (InstantiationException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
    
    /**
     * Determines whether a field has been indexed by looking for the i column in the metadata table
     * 
     * @param fieldName
     *            the field
     * @param ingestTypeFilter
     *            the ingest type filter
     * @return true if the field is indexed for the provided ingest types
     * @throws TableNotFoundException
     *             if the table does not exist
     */
    public boolean isIndexed(String fieldName, Set<String> ingestTypeFilter) throws TableNotFoundException {
        Preconditions.checkNotNull(fieldName);
        Preconditions.checkNotNull(ingestTypeFilter);
        
        Entry<String,Entry<String,Set<String>>> entry = Maps.immutableEntry(metadataTableName, Maps.immutableEntry(fieldName, ingestTypeFilter));
        
        try {
            return this.allFieldMetadataHelper.isIndexed(ColumnFamilyConstants.COLF_I, entry);
        } catch (InstantiationException | ExecutionException e) {
            throw new RuntimeException(e);
        }
        
    }
    
    /**
     * Determines whether a field has been tokenized by looking for the tf column in the metadata table
     * 
     * @param fieldName
     *            the field name
     * @param ingestTypeFilter
     *            the ingest type filter
     * @return true if the field is tokenized for the provided ingest types
     * @throws TableNotFoundException
     *             if the table does not exist
     */
    public boolean isTokenized(String fieldName, Set<String> ingestTypeFilter) throws TableNotFoundException {
        Preconditions.checkNotNull(fieldName);
        Preconditions.checkNotNull(ingestTypeFilter);
        
        Entry<String,Entry<String,Set<String>>> entry = Maps.immutableEntry(metadataTableName, Maps.immutableEntry(fieldName, ingestTypeFilter));
        
        try {
            return this.allFieldMetadataHelper.isIndexed(ColumnFamilyConstants.COLF_TF, entry);
        } catch (InstantiationException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
    
    /**
     * Returns a Set of all TextNormalizers in use by any type in Accumulo
     *
     * @param table
     *            the table to scan
     * @return a multimap of facets
     * @throws InstantiationException
     *             it can't, remove this
     * @throws IllegalAccessException
     *             it can't, remove this
     * @throws TableNotFoundException
     *             if no table exists
     */
    @Cacheable(value = "getFacets", key = "{#root.target.auths,#table}", cacheManager = "metadataHelperCacheManager")
    public Multimap<String,String> getFacets(String table) throws InstantiationException, IllegalAccessException, TableNotFoundException {
        log.debug("cache fault for getFacets({}, {})", this.auths, table);
        Multimap<String,String> fieldPivots = HashMultimap.create();
        
        try (Scanner bs = ScannerHelper.createScanner(accumuloClient, table, auths)) {
            bs.setRange(new Range());
            bs.fetchColumnFamily(PV);
            
            for (Entry<Key,Value> entry : bs) {
                Key key = entry.getKey();
                
                if (null != key.getRow()) {
                    String[] parts = StringUtils.split(key.getRow().toString(), "\0");
                    if (parts.length == 2) {
                        fieldPivots.put(parts[0], parts[1]);
                        fieldPivots.put(parts[1], parts[0]);
                        fieldPivots.put(parts[0], parts[0]);
                    }
                } else {
                    log.warn("Row null in ColumnFamilyConstants for key: {}", key);
                }
            }
        }
        
        return fieldPivots;
    }
    
    /**
     * Returns a Set of all counts / cardinalities
     *
     * @return a map of all term counts
     * @throws InstantiationException
     *             it can't, remove this
     * @throws IllegalAccessException
     *             it can't, remove this
     * @throws TableNotFoundException
     *             if no table exists
     */
    @Cacheable(value = "getTermCounts", key = "{#root.target.auths,#root.target.metadataTableName}", cacheManager = "metadataHelperCacheManager")
    public Map<String,Map<String,MetadataCardinalityCounts>> getTermCounts() throws InstantiationException, IllegalAccessException, TableNotFoundException {
        log.debug("cache fault for getTermCounts({}, {})", this.auths, this.metadataTableName);
        Map<String,Map<String,MetadataCardinalityCounts>> allCounts = Maps.newHashMap();
        
        if (log.isTraceEnabled()) {
            log.trace("getTermCounts from table: {}", metadataTableName);
        }
        
        try (Scanner bs = ScannerHelper.createScanner(accumuloClient, metadataTableName, auths)) {
            bs.setRange(new Range());
            bs.fetchColumnFamily(ColumnFamilyConstants.COLF_COUNT);
            
            for (Entry<Key,Value> entry : bs) {
                Key key = entry.getKey();
                
                if (null != key.getRow()) {
                    MetadataCardinalityCounts counts = new MetadataCardinalityCounts(key, entry.getValue());
                    Map<String,MetadataCardinalityCounts> values = allCounts.computeIfAbsent(counts.getField(), k -> Maps.newHashMapWithExpectedSize(5));
                    values.put(counts.getFieldValue(), counts);
                } else {
                    log.warn("Row null in ColumnFamilyConstants for key: {}", key);
                }
            }
        }
        
        return Collections.unmodifiableMap(allCounts);
    }
    
    /**
     * Returns a Set of all Counts using the client's principal's auths. The resulting information cannot be exposed outside the system.
     *
     * @return a map of term counts
     * @throws TableNotFoundException
     *             if no table exists
     * @throws AccumuloException
     *             if something goes wrong with accumulo
     * @throws AccumuloSecurityException
     *             if something goes wrong getting root authorizations
     */
    @Cacheable(value = "getTermCountsWithRootAuths", key = "{#root.target.metadataTableName}", cacheManager = "metadataHelperCacheManager")
    public Map<String,Map<String,MetadataCardinalityCounts>> getTermCountsWithRootAuths()
                    throws InstantiationException, IllegalAccessException, TableNotFoundException, AccumuloSecurityException, AccumuloException {
        log.debug("cache fault for getTermCounts({}, {})", this.auths, this.metadataTableName);
        Map<String,Map<String,MetadataCardinalityCounts>> allCounts = Maps.newHashMap();
        
        if (log.isTraceEnabled())
            log.trace("getTermCounts from table: {}", metadataTableName);
        
        Authorizations rootAuths = accumuloClient.securityOperations().getUserAuthorizations(accumuloClient.whoami());
        
        try (Scanner bs = ScannerHelper.createScanner(accumuloClient, metadataTableName, Collections.singleton(rootAuths))) {
            bs.setRange(new Range());
            bs.fetchColumnFamily(ColumnFamilyConstants.COLF_COUNT);
            
            for (Entry<Key,Value> entry : bs) {
                Key key = entry.getKey();
                
                if (null != key.getRow()) {
                    MetadataCardinalityCounts counts = new MetadataCardinalityCounts(key, entry.getValue());
                    Map<String,MetadataCardinalityCounts> values = allCounts.computeIfAbsent(counts.getField(), k -> Maps.newHashMapWithExpectedSize(5));
                    values.put(counts.getFieldValue(), counts);
                } else {
                    log.warn("Row null in ColumnFamilyConstants for key: {}", key);
                }
            }
        }
        
        return Collections.unmodifiableMap(allCounts);
    }
    
    /**
     * Returns a Set of all TextNormalizers in use by any type in Accumulo
     * 
     * @return a set of all normalizers
     * @throws InstantiationException
     *             it can't, remove this
     * @throws IllegalAccessException
     *             it can't, remove this
     * @throws TableNotFoundException
     *             if the table does not exist
     */
    @Cacheable(value = "getAllNormalized", key = "{#root.target.auths,#root.target.metadataTableName}", cacheManager = "metadataHelperCacheManager")
    public Set<String> getAllNormalized() throws InstantiationException, IllegalAccessException, TableNotFoundException {
        log.debug("cache fault for getAllNormalized({}, {})", this.auths, this.metadataTableName);
        Set<String> normalizedFields = Sets.newHashSetWithExpectedSize(10);
        if (log.isTraceEnabled()) {
            log.trace("getAllNormalized from table: {}", metadataTableName);
        }
        
        try (Scanner bs = ScannerHelper.createScanner(accumuloClient, metadataTableName, auths)) {
            
            bs.setRange(new Range());
            bs.fetchColumnFamily(ColumnFamilyConstants.COLF_N);
            
            for (Entry<Key,Value> entry : bs) {
                Key key = entry.getKey();
                
                if (null != key.getRow()) {
                    normalizedFields.add(key.getRow().toString());
                } else {
                    log.warn("Row null in ColumnFamilyConstants for key: {}", key);
                }
            }
        }
        
        return Collections.unmodifiableSet(normalizedFields);
    }
    
    /**
     * Returns a Set of all Types in use by any type in Accumulo
     *
     * @return all ingest types on the system
     * @throws InstantiationException
     *             for failures to instantiate
     * @throws IllegalAccessException
     *             for failures to access
     * @throws TableNotFoundException
     *             for table not existing
     */
    public Set<Type<?>> getAllDatatypes() throws InstantiationException, IllegalAccessException, TableNotFoundException {
        return this.allFieldMetadataHelper.getAllDatatypes();
    }
    
    /**
     * A map of composite name to the ordered list of it for example, mapping of {@code COLOR -> ['COLOR_WHEELS,0', 'MAKE_COLOR,1' ]}. If called multiple time,
     * it returns the same cached map.
     * 
     * @return An unmodifiable Multimap of composite fields
     * @throws TableNotFoundException
     *             if no table exists
     */
    public Multimap<String,String> getCompositeToFieldMap() throws TableNotFoundException {
        return this.allFieldMetadataHelper.getCompositeToFieldMap();
    }
    
    /**
     * Get the map of composite fields, filtered by ingest type.
     * <p>
     * The delegate method is cached, so multiple calls to this method will return the same map.
     *
     * @param ingestTypeFilter
     *            a filter of ingest types
     * @return the map of composite fields for the provided ingest types
     * @throws TableNotFoundException
     *             if no table exists
     */
    public Multimap<String,String> getCompositeToFieldMap(Set<String> ingestTypeFilter) throws TableNotFoundException {
        return this.allFieldMetadataHelper.getCompositeToFieldMap(ingestTypeFilter);
    }
    
    /**
     * A map of composite name to transition date.
     *
     * @return An unmodifiable Map
     * @throws TableNotFoundException
     *             if no table exists
     */
    public Map<String,Date> getCompositeTransitionDateMap() throws TableNotFoundException {
        return this.allFieldMetadataHelper.getCompositeTransitionDateMap();
    }
    
    public Map<String,Date> getCompositeTransitionDateMap(Set<String> ingestTypeFilter) throws TableNotFoundException {
        return this.allFieldMetadataHelper.getCompositeTransitionDateMap(ingestTypeFilter);
    }
    
    /**
     * A map of whindex field to creation date.
     *
     * @return An unmodifiable Map
     * @throws TableNotFoundException
     *             if no table exists
     */
    public Map<String,Date> getWhindexCreationDateMap() throws TableNotFoundException {
        return this.allFieldMetadataHelper.getWhindexCreationDateMap();
    }
    
    public Map<String,Date> getWhindexCreationDateMap(Set<String> ingestTypeFilter) throws TableNotFoundException {
        return this.allFieldMetadataHelper.getWhindexCreationDateMap(ingestTypeFilter);
    }
    
    /**
     * A map of composite name to field separator.
     *
     * @return An unmodifiable Map
     * @throws TableNotFoundException
     *             if no table found
     */
    public Map<String,String> getCompositeFieldSeparatorMap() throws TableNotFoundException {
        return this.allFieldMetadataHelper.getCompositeFieldSeparatorMap();
    }
    
    public Map<String,String> getCompositeFieldSeparatorMap(Set<String> ingestTypeFilter) throws TableNotFoundException {
        return this.allFieldMetadataHelper.getCompositeFieldSeparatorMap(ingestTypeFilter);
    }
    
    /**
     * Fetch the set of {@link Type}s that are configured for this <code>fieldName</code> as specified in the table pointed to by the
     * <code>metadataTableName</code> parameter.
     *
     * @param fieldName
     *            The name of the field to fetch the {@link Type}s for. If null then all dataTypes are returned.
     * @return the Types configured for the field
     * @throws InstantiationException
     *             for problems instantiating
     * @throws IllegalAccessException
     *             for problems accessing
     * @throws TableNotFoundException
     *             for table not found
     */
    public Set<Type<?>> getDatatypesForField(String fieldName) throws InstantiationException, IllegalAccessException, TableNotFoundException {
        return getDatatypesForField(fieldName, null);
    }
    
    /**
     * Fetches the set of {@link Type}s that are configured for the given field, restricted by ingest type
     *
     * @return the Types associated with a particular field given a set of ingest types
     * @throws InstantiationException
     *             for problems instantiating
     * @throws IllegalAccessException
     *             for problems accessing
     * @throws TableNotFoundException
     *             for table not found
     */
    public Set<Type<?>> getDatatypesForField(String fieldName, Set<String> ingestTypeFilter)
                    throws InstantiationException, IllegalAccessException, TableNotFoundException {
        
        Set<Type<?>> dataTypes = new HashSet<>();
        Multimap<String,Type<?>> mm = this.allFieldMetadataHelper.getFieldsToDatatypes(ingestTypeFilter);
        if (fieldName == null) {
            dataTypes.addAll(mm.values());
        } else {
            Collection<Type<?>> types = mm.asMap().get(fieldName.toUpperCase());
            if (types != null) {
                dataTypes.addAll(types);
            }
        }
        return dataTypes;
    }
    
    public TypeMetadata getTypeMetadata() throws TableNotFoundException {
        return this.allFieldMetadataHelper.getTypeMetadata(null);
    }
    
    public TypeMetadata getTypeMetadata(Set<String> ingestTypeFilter) throws TableNotFoundException {
        return this.allFieldMetadataHelper.getTypeMetadata(ingestTypeFilter);
    }
    
    public CompositeMetadata getCompositeMetadata() throws TableNotFoundException {
        return this.allFieldMetadataHelper.getCompositeMetadata(null);
    }
    
    public CompositeMetadata getCompositeMetadata(Set<String> ingestTypeFilter) throws TableNotFoundException {
        return this.allFieldMetadataHelper.getCompositeMetadata(ingestTypeFilter);
    }
    
    /**
     * Fetch the Set of all fields marked as containing term frequency information, {@link ColumnFamilyConstants#COLF_TF}.
     * <p>
     * These docs are very wrong, update them
     *
     * @return a SetMultimap of raw Key Value pairs
     * @throws TableNotFoundException
     *             if no table exists
     * @throws ExecutionException
     *             it can't, remove this
     */
    @Cacheable(value = "getEdges", key = "{#root.target.fullUserAuths,#root.target.metadataTableName}")
    public SetMultimap<Key,Value> getEdges() throws TableNotFoundException, ExecutionException {
        log.debug("cache fault for getEdges({})", this.auths);
        SetMultimap<Key,Value> edges = HashMultimap.create();
        if (log.isTraceEnabled()) {
            log.trace("getEdges from table: {}", metadataTableName);
        }
        // unlike other entries, the edges colf entries have many auths set. We'll use the fullUserAuths in the scanner instead
        // of the minimal set in this.auths
        try (Scanner scanner = ScannerHelper.createScanner(accumuloClient, metadataTableName, fullUserAuths)) {
            
            scanner.setRange(new Range());
            scanner.fetchColumnFamily(ColumnFamilyConstants.COLF_EDGE);
            
            // First iterator strips the optional attribute2 and attribute3 off the cq, second one
            // combines the protocol buffer data.
            IteratorSetting stripConfig = new IteratorSetting(50, EdgeMetadataCQStrippingIterator.class);
            IteratorSetting combineConfig = new IteratorSetting(51, EdgeMetadataCombiner.class);
            combineConfig.addOption("columns", ColumnFamilyConstants.COLF_EDGE.toString());
            scanner.addScanIterator(stripConfig);
            scanner.addScanIterator(combineConfig);
            
            for (Entry<Key,Value> entry : scanner) {
                edges.put(entry.getKey(), entry.getValue());
            }
        }
        
        return Multimaps.unmodifiableSetMultimap(edges);
    }
    
    /**
     * Fetch the set of {@link Type}s that are configured for this <code>fieldName</code> as specified in the table pointed to by the
     * <code>metadataTableName</code> parameter.
     *
     * @param ingestTypeFilter
     *            Any projection of datatypes to limit the fetch for.
     * @return a mapping of fields to their Types
     * @throws InstantiationException
     *             if there is a problem instantiating
     * @throws IllegalAccessException
     *             if there is a problem accessing
     * @throws TableNotFoundException
     *             if the table does not exist
     */
    public Multimap<String,Type<?>> getFieldsToDatatypes(Set<String> ingestTypeFilter)
                    throws InstantiationException, IllegalAccessException, TableNotFoundException {
        return this.allFieldMetadataHelper.getFieldsToDatatypes(ingestTypeFilter);
    }
    
    /**
     * Scans the metadata table and returns the set of fields that use the supplied normalizer.
     *
     * @param datawaveType
     *            a {@link Type}
     * @return a set of fields associated with the provided Type
     * @throws InstantiationException
     *             it can't, remove this
     * @throws IllegalAccessException
     *             it can't, remove this
     * @throws TableNotFoundException
     *             if no table exists
     */
    public Set<String> getFieldsForDatatype(Class<? extends Type<?>> datawaveType)
                    throws InstantiationException, IllegalAccessException, TableNotFoundException {
        return getFieldsForDatatype(datawaveType, null);
    }
    
    /**
     * Scans the metadata table and returns the set of fields that use the supplied normalizer.
     * <p>
     * This method allows a client to specify data types to filter out. If the set is null, then it assumed the user wants all data types. If the set is empty,
     * then it assumed the user wants no data types. Otherwise, values that occur in the set will be used as a white list of data types.
     *
     * @param datawaveType
     *            a {@link Type}
     * @param ingestTypeFilter
     *            a set of ingest types
     * @return a set of fields associated with the provided Types and ingest types
     * @throws TableNotFoundException
     *             if no table exists
     */
    public Set<String> getFieldsForDatatype(Class<? extends Type<?>> datawaveType, Set<String> ingestTypeFilter) throws TableNotFoundException {
        return this.allFieldMetadataHelper.getFieldsForDatatype(datawaveType, ingestTypeFilter);
    }
    
    /**
     * Pull an instance of the provided normalizer class name from the internal cache.
     *
     * @param datatypeClass
     *            The name of the normalizer class to instantiate.
     * @return An instance of the normalizer class that was requested.
     * @throws InstantiationException
     *             if there is a problem instantiating
     * @throws IllegalAccessException
     *             if there is a problem accessing
     */
    public Type<?> getDatatypeFromClass(Class<? extends Type<?>> datatypeClass) throws InstantiationException, IllegalAccessException {
        return this.allFieldMetadataHelper.getDatatypeFromClass(datatypeClass);
    }
    
    /**
     * Fetch the Set of all fields marked as containing term frequency information, {@link ColumnFamilyConstants#COLF_TF}.
     *
     * @return the set of term frequency fields given the ingest type filter
     * @throws TableNotFoundException
     *             if the table does not exist
     */
    @Cacheable(value = "getTermFrequencyFields", key = "{#root.target.auths,#root.target.metadataTableName,#p0}", cacheManager = "metadataHelperCacheManager")
    public Set<String> getTermFrequencyFields(Set<String> ingestTypeFilter) throws TableNotFoundException {
        
        Multimap<String,String> termFrequencyFields = loadTermFrequencyFields();
        
        Set<String> fields = new HashSet<>();
        if (ingestTypeFilter == null || ingestTypeFilter.isEmpty()) {
            fields.addAll(termFrequencyFields.values());
        } else {
            for (String datatype : ingestTypeFilter) {
                fields.addAll(termFrequencyFields.get(datatype));
            }
        }
        return Collections.unmodifiableSet(fields);
    }
    
    /**
     * Get the set of indexed fields for the provided ingest type filter. A null or empty filter indicates all indexed fields should be returned.
     *
     * @param ingestTypeFilter
     *            the ingest type filter
     * @return the set of indexed fields given the provided ingest type filter
     * @throws TableNotFoundException
     *             if the table does not exist
     */
    public Set<String> getIndexedFields(Set<String> ingestTypeFilter) throws TableNotFoundException {
        
        Multimap<String,String> indexedFields = this.allFieldMetadataHelper.loadIndexedFields();
        
        Set<String> fields = new HashSet<>();
        if (ingestTypeFilter == null || ingestTypeFilter.isEmpty()) {
            fields.addAll(indexedFields.values());
        } else {
            for (String datatype : ingestTypeFilter) {
                fields.addAll(indexedFields.get(datatype));
            }
        }
        return Collections.unmodifiableSet(fields);
    }
    
    /**
     * Get reverse index fields using the data type filter.
     *
     * @param ingestTypeFilter
     *            the ingest type filter
     * @return the set of reverse indexed fields given the provided ingest type filter
     * @throws TableNotFoundException
     *             if the table does not exist
     */
    public Set<String> getReverseIndexedFields(Set<String> ingestTypeFilter) throws TableNotFoundException {
        
        Multimap<String,String> indexedFields = this.allFieldMetadataHelper.loadReverseIndexedFields();
        
        Set<String> fields = new HashSet<>();
        if (ingestTypeFilter == null || ingestTypeFilter.isEmpty()) {
            fields.addAll(indexedFields.values());
        } else {
            for (String datatype : ingestTypeFilter) {
                fields.addAll(indexedFields.get(datatype));
            }
        }
        return Collections.unmodifiableSet(fields);
    }
    
    /**
     * Get expansion fields using the data type filter.
     * 
     * @param ingestTypeFilter
     *            the ingest type filter
     * @return the set of expansion fields that match the provided ingest type filter
     * @throws TableNotFoundException
     *             if no table exists
     */
    public Set<String> getExpansionFields(Set<String> ingestTypeFilter) throws TableNotFoundException {
        
        Multimap<String,String> expansionFields = this.allFieldMetadataHelper.loadExpansionFields();
        
        Set<String> fields = new HashSet<>();
        if (ingestTypeFilter == null || ingestTypeFilter.isEmpty()) {
            fields.addAll(expansionFields.values());
        } else {
            for (String datatype : ingestTypeFilter) {
                fields.addAll(expansionFields.get(datatype));
            }
        }
        return Collections.unmodifiableSet(fields);
    }
    
    /**
     * Get the content fields which are those to be queried when using the content functions.
     * 
     * @param ingestTypeFilter
     *            the ingest type filter
     * @return the fields used for content functions given the ingest type filter
     * @throws TableNotFoundException
     *             if no table exists
     */
    public Set<String> getContentFields(Set<String> ingestTypeFilter) throws TableNotFoundException {
        
        Multimap<String,String> contentFields = this.allFieldMetadataHelper.loadContentFields();
        
        Set<String> fields = new HashSet<>();
        if (ingestTypeFilter == null || ingestTypeFilter.isEmpty()) {
            fields.addAll(contentFields.values());
        } else {
            for (String datatype : ingestTypeFilter) {
                fields.addAll(contentFields.get(datatype));
            }
        }
        return Collections.unmodifiableSet(fields);
    }
    
    /**
     * Sum the frequency counts for a field between a start and end date (inclusive)
     *
     * @param fieldName
     *            the field
     * @param begin
     *            the start date
     * @param end
     *            the end date
     * @return the total instances of the field in the date range
     * @throws TableNotFoundException
     *             if no table exists
     */
    public long getCardinalityForField(String fieldName, Date begin, Date end) throws TableNotFoundException {
        return getCardinalityForField(fieldName, null, begin, end);
    }
    
    /**
     * Sum the frequency counts for a field in a datatype between a start and end date (inclusive)
     *
     * @param fieldName
     *            the field
     * @param datatype
     *            the ingest type
     * @param begin
     *            the start date
     * @param end
     *            the end date
     * @return the total instances of the field in the date range
     * @throws TableNotFoundException
     *             if no table exists
     */
    public long getCardinalityForField(String fieldName, String datatype, Date begin, Date end) throws TableNotFoundException {
        log.trace("getCardinalityForField from table: {}", metadataTableName);
        Text row = new Text(fieldName.toUpperCase());
        
        // Get all the rows in DatawaveMetadata for the field, only in the 'f' colfam
        long count;
        try (Scanner bs = ScannerHelper.createScanner(accumuloClient, metadataTableName, auths)) {
            
            Key startKey = new Key(row);
            bs.setRange(new Range(startKey, startKey.followingKey(PartialKey.ROW)));
            bs.fetchColumnFamily(ColumnFamilyConstants.COLF_F);
            
            count = 0;
            
            for (Entry<Key,Value> entry : bs) {
                Text colq = entry.getKey().getColumnQualifier();
                
                int index = colq.find(NULL_BYTE);
                if (index == -1) {
                    continue;
                }
                
                // If we were given a non-null datatype
                // Ensure that we process records only on that type
                if (null != datatype) {
                    try {
                        String type = Text.decode(colq.getBytes(), 0, index);
                        if (!type.equals(datatype)) {
                            continue;
                        }
                    } catch (CharacterCodingException e) {
                        log.warn("Could not deserialize colqual: {}", entry.getKey());
                        continue;
                    }
                }
                
                // Parse the date to ensure that we want this record
                String dateStr = "null";
                Date date;
                try {
                    dateStr = Text.decode(colq.getBytes(), index + 1, colq.getLength() - (index + 1));
                    date = DateHelper.parse(dateStr);
                    // Add the provided count if we fall within begin and end,
                    // inclusive
                    if (date.compareTo(begin) >= 0 && date.compareTo(end) <= 0) {
                        count += SummingCombiner.VAR_LEN_ENCODER.decode(entry.getValue().get());
                    }
                } catch (ValueFormatException e) {
                    log.warn("Could not convert the Value to a long: {}", entry.getValue());
                } catch (CharacterCodingException e) {
                    log.warn("Could not deserialize colqual: {}", entry.getKey());
                } catch (DateTimeParseException e) {
                    log.warn("Could not convert date string: {}", dateStr);
                }
            }
        }
        
        return count;
    }
    
    /**
     * Get the ingest types that match this filter
     *
     * @param ingestTypeFilter
     *            the ingest type filter
     * @return the actual ingest types
     * @throws TableNotFoundException
     *             if no table exists
     */
    public Set<String> getDatatypes(Set<String> ingestTypeFilter) throws TableNotFoundException {
        
        Set<String> datatypes = this.allFieldMetadataHelper.loadDatatypes();
        if (ingestTypeFilter != null && !ingestTypeFilter.isEmpty()) {
            datatypes = Sets.newHashSet(Sets.intersection(datatypes, ingestTypeFilter));
        }
        
        return Collections.unmodifiableSet(datatypes);
    }
    
    /**
     * Returns the sum of counts for the given field across all datatypes in the date range
     * 
     * @param fieldName
     *            the field
     * @param begin
     *            the start of the date range
     * @param end
     *            the end of the date range
     * @return the count
     */
    public Long getCountsByFieldForDays(String fieldName, Date begin, Date end) {
        return getCountsByFieldForDays(fieldName, begin, end, UniversalSet.instance());
    }
    
    /**
     * Returns the sum of all counts for the given fields and datatypes from the start date to the end date.
     * 
     * @param fieldName
     *            the field name to filter on
     * @param begin
     *            the begin date
     * @param end
     *            the end date
     * @param dataTypes
     *            the datatypes to filter on
     * @return the total counts
     */
    public Long getCountsByFieldForDays(String fieldName, Date begin, Date end, Set<String> dataTypes) {
        Preconditions.checkNotNull(fieldName);
        Preconditions.checkNotNull(begin);
        Preconditions.checkNotNull(end);
        Preconditions.checkArgument((begin.before(end) || begin.getTime() == end.getTime()));
        Preconditions.checkNotNull(dataTypes);
        
        Date truncatedBegin = DateUtils.truncate(begin, Calendar.DATE);
        Date truncatedEnd = DateUtils.truncate(end, Calendar.DATE);
        
        if (truncatedEnd.getTime() != end.getTime()) {
            // If we don't have the same time for both, we actually truncated the end, and, as such, we want to bump out the date range to include the end.
            truncatedEnd = new Date(truncatedEnd.getTime() + 86400000);
        }
        
        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        cal.setTime(truncatedBegin);
        
        // If the start and end date are the same, return the count for just the start date.
        // TODO - Verify if this is the correct behavior, i.e. treating the end date as inclusive vs. not. It should probably match query date range behavior.
        if (truncatedBegin.getTime() == truncatedEnd.getTime()) {
            String desiredDate = DateHelper.format(cal.getTime());
            return getCountsByFieldInDayWithTypes(fieldName, desiredDate, dataTypes);
        } else {
            // Otherwise, sum up the counts across the given date range.
            long sum = 0L;
            while (cal.getTime().before(truncatedEnd)) {
                Date curDate = cal.getTime();
                String desiredDate = DateHelper.format(curDate);
                
                sum += getCountsByFieldInDayWithTypes(fieldName, desiredDate, dataTypes);
                cal.add(Calendar.DATE, 1);
            }
            return sum;
        }
    }
    
    /**
     * Return the sum across all datatypes of the {@link ColumnFamilyConstants#COLF_F} on the given day.
     *
     * @param fieldName
     *            the field
     * @param date
     *            the day
     * @return the number of times this field appears on the given day across all datatypes
     */
    public Long getCountsByFieldInDay(String fieldName, String date) {
        return getCountsByFieldInDayWithTypes(fieldName, date, UniversalSet.instance());
    }
    
    /**
     * Return the sum across all datatypes of the {@link ColumnFamilyConstants#COLF_F} on the given day in the provided types
     *
     * @param fieldName
     *            the field name
     * @param date
     *            the date
     * @param datatypes
     *            a filter of ingest types
     * @return the count of fields on a particular day given the set of ingest types
     */
    public Long getCountsByFieldInDayWithTypes(String fieldName, String date, final Set<String> datatypes) {
        Preconditions.checkNotNull(fieldName);
        Preconditions.checkNotNull(date);
        Preconditions.checkNotNull(datatypes);
        
        try {
            Map<String,Long> countsByType = getCountsByFieldInDayWithTypes(Maps.immutableEntry(fieldName, date));
            Iterable<Entry<String,Long>> filteredByType = Iterables.filter(countsByType.entrySet(), input -> datatypes.contains(input.getKey()));
            
            long sum = 0;
            for (Entry<String,Long> entry : filteredByType) {
                sum += entry.getValue();
            }
            
            return sum;
        } catch (TableNotFoundException | IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    /**
     * Get the counts for a field and date pair across all ingest types.
     * <p>
     * Note: the method name does not match the underlying operation.
     * <p>
     * Get the map of counts by datatype for the provided field. No filtering is done by ingest type, all types are searched.
     *
     * @param identifier
     *            a pair of field and date
     * @return the map of counts
     * @throws TableNotFoundException
     *             if no table exists
     * @throws IOException
     *             if an IO error occurs
     */
    protected HashMap<String,Long> getCountsByFieldInDayWithTypes(Entry<String,String> identifier) throws TableNotFoundException, IOException {
        String fieldName = identifier.getKey();
        String date = identifier.getValue();
        
        // try to get the counts by field using the original (cached) connector
        HashMap<String,Long> datatypeToCounts = getCountsByFieldInDayWithTypes(fieldName, date, accumuloClient, null);
        
        // if we don't get a hit, try the real connector
        if (datatypeToCounts.isEmpty() && accumuloClient instanceof WrappedAccumuloClient) {
            WrappedAccumuloClient wrappedClient = ((WrappedAccumuloClient) accumuloClient);
            datatypeToCounts = getCountsByFieldInDayWithTypes(fieldName, date, wrappedClient.getReal(), wrappedClient);
        }
        
        return datatypeToCounts;
    }
    
    /**
     * Get the counts for a field and date pair across all ingest typos
     * <p>
     * Note: the method name does not match the underlying operation.
     * <p>
     * Get the map of counts by datatype for the provided field. No filtering is done by ingest type, all types are searched.
     *
     * @param fieldName
     *            the field name
     * @param date
     *            the date
     * @param client
     *            the AccumuloClient
     * @param wrappedClient
     *            the 'real' client, used to update a cache
     * @return a map of counts by datatype
     * @throws TableNotFoundException
     *             if no table exists
     * @throws IOException
     *             if there is an IO problem
     */
    protected HashMap<String,Long> getCountsByFieldInDayWithTypes(String fieldName, String date, AccumuloClient client, WrappedAccumuloClient wrappedClient)
                    throws TableNotFoundException, IOException {
        final HashMap<String,Long> datatypeToCounts = Maps.newHashMap();
        
        BatchWriter writer = null;
        
        // we have to use the real connector since the f column is not cached
        try (Scanner scanner = ScannerHelper.createScanner(client, metadataTableName, auths)) {
            scanner.fetchColumnFamily(ColumnFamilyConstants.COLF_F);
            scanner.setRange(Range.exact(fieldName));
            
            IteratorSetting cqRegex = new IteratorSetting(50, RegExFilter.class);
            RegExFilter.setRegexs(cqRegex, null, null, ".*\u0000" + date, null, false);
            scanner.addScanIterator(cqRegex);
            
            final Text holder = new Text();
            for (Entry<Key,Value> entry : scanner) {
                // if this is the real connector, and wrapped connector is not null, it means
                // that we didn't get a hit in the cache. So, we will update the cache with the
                // entries from the real table
                if (wrappedClient != null && client == wrappedClient.getReal()) {
                    writer = updateCache(entry, writer, wrappedClient);
                }
                
                ByteArrayInputStream bais = new ByteArrayInputStream(entry.getValue().get());
                DataInputStream inputStream = new DataInputStream(bais);
                
                Long sum = WritableUtils.readVLong(inputStream);
                
                entry.getKey().getColumnQualifier(holder);
                int offset = holder.find(NULL_BYTE);
                
                Preconditions.checkArgument(-1 != offset, "Could not find nullbyte separator in column qualifier for: " + entry.getKey());
                
                String datatype = Text.decode(holder.getBytes(), 0, offset);
                
                datatypeToCounts.put(datatype, sum);
            }
        } finally {
            if (writer != null) {
                try {
                    writer.close();
                } catch (MutationsRejectedException e) {
                    log.warn("Error closing batch writer for cached table: " + metadataTableName, e);
                }
            }
        }
        
        return datatypeToCounts;
    }
    
    /**
     * Get counts for each field across the date range.
     * <p>
     * Note: it is highly recommended to use this method instead {@link #getCountsForFieldsInDateRange(Set, Set, Date, Date)}.
     *
     * @param fields
     *            the fields
     * @param begin
     *            the start date
     * @param end
     *            the end date
     * @return a map of field counts
     */
    public Map<String,Long> getCountsForFieldsInDateRange(Set<String> fields, Date begin, Date end) {
        return getCountsForFieldsInDateRange(fields, Collections.emptySet(), begin, end);
    }
    
    /**
     * Get counts for each field across the date range. Optionally filter by datatypes if provided.
     *
     * @param fields
     *            the fields
     * @param datatypes
     *            the datatypes
     * @param begin
     *            the start date
     * @param end
     *            the end date
     * @return a map of field counts
     */
    public Map<String,Long> getCountsForFieldsInDateRange(Set<String> fields, Set<String> datatypes, Date begin, Date end) {
        Date truncatedBegin = DateUtils.truncate(begin, Calendar.DATE);
        Date truncatedEnd = DateUtils.truncate(end, Calendar.DATE);
        String startDate = DateHelper.format(truncatedBegin);
        String endDate = DateHelper.format(truncatedEnd);
        return getCountsForFieldsInDateRange(fields, datatypes, startDate, endDate);
    }
    
    /**
     * Get counts for each field across the date range. Optionally filter by datatypes if provided.
     * 
     * @param fields
     *            the fields
     * @param datatypes
     *            the datatypes
     * @param beginDate
     *            the start date
     * @param endDate
     *            the end date
     * @return a map of field counts
     */
    public Map<String,Long> getCountsForFieldsInDateRange(Set<String> fields, Set<String> datatypes, String beginDate, String endDate) {
        
        SortedSet<String> sortedDatatypes = new TreeSet<>(datatypes);
        Map<String,Long> fieldCounts = new HashMap<>();
        Set<Range> ranges = createFieldCountRanges(fields, sortedDatatypes, beginDate, endDate);
        
        if (ranges.isEmpty()) {
            return fieldCounts;
        }
        
        AccumuloClient client = accumuloClient;
        if (client instanceof WrappedAccumuloClient) {
            client = ((WrappedAccumuloClient) client).getReal();
        }
        
        try (BatchScanner bs = ScannerHelper.createBatchScanner(client, getMetadataTableName(), getAuths(), fields.size())) {
            
            bs.setRanges(ranges);
            bs.fetchColumnFamily(ColumnFamilyConstants.COLF_F);
            
            IteratorSetting setting = new IteratorSetting(50, "MetadataFrequencySeekingIterator", MetadataFColumnSeekingFilter.class);
            setting.addOption(MetadataFColumnSeekingFilter.DATATYPES_OPT, Joiner.on(',').join(sortedDatatypes));
            setting.addOption(MetadataFColumnSeekingFilter.START_DATE, beginDate);
            setting.addOption(MetadataFColumnSeekingFilter.END_DATE, endDate);
            bs.addScanIterator(setting);
            
            for (Entry<Key,Value> entry : bs) {
                
                String field = entry.getKey().getRow().toString();
                Long count = readLongFromValue(entry.getValue());
                
                if (fieldCounts.containsKey(field)) {
                    Long existingCount = fieldCounts.get(field);
                    existingCount += count;
                    fieldCounts.put(field, existingCount);
                } else {
                    fieldCounts.put(field, count);
                }
            }
            
        } catch (TableNotFoundException | IOException e) {
            throw new RuntimeException(e);
        }
        return fieldCounts;
    }
    
    /**
     * Build ranges for the {@link #getCountsForFieldsInDateRange(Set, Set, String, String)} method.
     * <p>
     * The {@link MetadataFColumnSeekingFilter} can handle a field range, but providing datatypes enables more precise ranges.
     *
     * @param fields
     *            the fields
     * @param datatypes
     *            the datatypes
     * @param beginDate
     *            the start date
     * @param endDate
     *            the end date
     * @return a set of ranges for the provided fields, bounded by date and optionally datatypes
     */
    private Set<Range> createFieldCountRanges(Set<String> fields, SortedSet<String> datatypes, String beginDate, String endDate) {
        Set<Range> ranges = new HashSet<>();
        for (String field : fields) {
            if (datatypes.isEmpty()) {
                // punt the hard work to the MetadataFColumnSeekingFilter
                ranges.add(Range.exact(field, "f"));
            } else {
                // more precise range, the MetadataFColumnSeekingFilter will handle seeing between the first and
                // last datatypes as necessary
                Key start = new Key(field, "f", datatypes.first() + '\u0000' + beginDate);
                Key end = new Key(field, "f", datatypes.last() + '\u0000' + endDate + '\u0000');
                ranges.add(new Range(start, true, end, false));
            }
        }
        return ranges;
    }
    
    /**
     * Deserialize a Value that contains a Long
     * 
     * @param value
     *            an accumulo Value
     * @return a long
     * @throws IOException
     *             if there is a deserialization problem
     */
    private Long readLongFromValue(Value value) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(value.get())) {
            try (DataInputStream inputStream = new DataInputStream(bais)) {
                return WritableUtils.readVLong(inputStream);
            }
        }
    }
    
    /**
     * Get the earliest occurrence of a field across all datatypes
     * 
     * @param fieldName
     *            the field
     * @return the earliest date that this field occurs
     */
    public Date getEarliestOccurrenceOfField(String fieldName) {
        return getEarliestOccurrenceOfFieldWithType(fieldName, null);
    }
    
    /**
     * Get the earliest occurrence of a field for the given datatype
     * 
     * @param fieldName
     *            the field
     * @param dataType
     *            the datatype
     * @return the earliest date that a field occurred for the given datatype
     */
    public Date getEarliestOccurrenceOfFieldWithType(String fieldName, final String dataType) {
        // try to get the date using the original (cached) connector
        Date date = getEarliestOccurrenceOfFieldWithType(fieldName, dataType, accumuloClient, null);
        
        // if we don't get a hit, try the real connector
        if (date == null && accumuloClient instanceof WrappedAccumuloClient) {
            WrappedAccumuloClient wrappedClient = ((WrappedAccumuloClient) accumuloClient);
            date = getEarliestOccurrenceOfFieldWithType(fieldName, dataType, wrappedClient.getReal(), wrappedClient);
        }
        
        return date;
    }
    
    /**
     * Get the earliest occurrence of a field given a datatype
     * 
     * @param fieldName
     *            the field
     * @param dataType
     *            the datatype
     * @param client
     *            an AccumuloClient
     * @param wrappedClient
     *            a wrapped AccumuloClient
     * @return the earliest date the field is found, or null otherwise
     */
    protected Date getEarliestOccurrenceOfFieldWithType(String fieldName, final String dataType, AccumuloClient client, WrappedAccumuloClient wrappedClient) {
        String dateString = null;
        BatchWriter writer = null;
        
        try (Scanner scanner = ScannerHelper.createScanner(client, metadataTableName, auths)) {
            scanner.fetchColumnFamily(ColumnFamilyConstants.COLF_F);
            scanner.setRange(Range.exact(fieldName));
            
            // if a type was specified, add a regex filter for it
            if (dataType != null) {
                IteratorSetting cqRegex = new IteratorSetting(50, RegExFilter.class);
                RegExFilter.setRegexs(cqRegex, null, null, dataType + "\u0000.*", null, false);
                scanner.addScanIterator(cqRegex);
            }
            
            final Text holder = new Text();
            for (Entry<Key,Value> entry : scanner) {
                // if this is the real connector, and wrapped connector is not null, it means
                // that we didn't get a hit in the cache. So, we will update the cache with the
                // entries from the real table
                if (wrappedClient != null && client == wrappedClient.getReal()) {
                    writer = updateCache(entry, writer, wrappedClient);
                }
                
                entry.getKey().getColumnQualifier(holder);
                int startPos = holder.find(NULL_BYTE) + 1;
                
                if (0 == startPos) {
                    log.trace("Could not find nullbyte separator in column qualifier for: {}", entry.getKey());
                } else if ((holder.getLength() - startPos) <= 0) {
                    log.trace("Could not find date to parse in column qualifier for: {}", entry.getKey());
                } else {
                    try {
                        dateString = Text.decode(holder.getBytes(), startPos, holder.getLength() - startPos);
                        break;
                    } catch (CharacterCodingException e) {
                        log.trace("Unable to decode date string for: {}", entry.getKey().getColumnQualifier());
                    }
                }
            }
        } catch (TableNotFoundException e) {
            log.warn("Error creating scanner against table: {}", metadataTableName, e);
        } finally {
            if (writer != null) {
                try {
                    writer.close();
                } catch (MutationsRejectedException e) {
                    log.warn("Error closing batch writer for cached table: {}", metadataTableName, e);
                }
            }
        }
        
        Date date = null;
        if (dateString != null) {
            date = DateHelper.parse(dateString);
        }
        
        return date;
    }
    
    /**
     * Return the field index holes calculated between all "i" and "f" entries. The map consists of field names to datatypes to field index holes.
     * 
     * @param fields
     *            the fields to fetch field index holes for, an empty set will result in all fields being fetched
     * @param datatypes
     *            the datatypes to fetch field index holes for, an empty set will result in all datatypes being fetched
     * @param minThreshold
     *            the minimum percentage threshold required for an index row to be considered NOT a hole on a particular date, expected to be a value between
     *            0.0 (inclusive) to 1.0 (inclusive)
     * @return the field index holes
     */
    public Map<String,Map<String,IndexFieldHole>> getFieldIndexHoles(Set<String> fields, Set<String> datatypes, double minThreshold)
                    throws TableNotFoundException, IOException {
        return allFieldMetadataHelper.getFieldIndexHoles(fields, datatypes, minThreshold);
    }
    
    /**
     * Return the field index holes calculated between all "ri" and "f" entries. The map consists of field names to datatypes to field index holes.
     * 
     * @param fields
     *            the fields to fetch field index holes for, an empty set will result in all fields being fetched
     * @param datatypes
     *            the datatypes to fetch field index holes for, an empty set will result in all datatypes being fetched
     * @param minThreshold
     *            the minimum percentage threshold required for an index row to be considered NOT a hole on a particular date, expected to be a value between
     *            0.0 (inclusive) to 1.0 (inclusive)
     * @return the field index holes
     */
    public Map<String,Map<String,IndexFieldHole>> getReversedFieldIndexHoles(Set<String> fields, Set<String> datatypes, double minThreshold)
                    throws TableNotFoundException, IOException {
        return allFieldMetadataHelper.getReversedFieldIndexHoles(fields, datatypes, minThreshold);
    }
    
    /**
     * Updates the table cache via the mock connector with the given entry and writer. If writer is null, a writer will be created and returned for subsequent
     * use.
     *
     * @param entry
     *            the entry to add
     * @param writer
     *            the batch writer
     * @param wrappedClient
     *            the wrapped client
     * @return a batch writer
     */
    private BatchWriter updateCache(Entry<Key,Value> entry, BatchWriter writer, WrappedAccumuloClient wrappedClient) {
        try {
            if (writer == null) {
                BatchWriterConfig bwConfig = new BatchWriterConfig().setMaxMemory(10L * (1024L * 1024L)).setMaxLatency(100L, TimeUnit.MILLISECONDS)
                                .setMaxWriteThreads(1);
                writer = wrappedClient.getMock().createBatchWriter(metadataTableName, bwConfig);
            }
            
            Key valueKey = entry.getKey();
            
            Mutation m = new Mutation(entry.getKey().getRow());
            m.put(valueKey.getColumnFamily(), valueKey.getColumnQualifier(), new ColumnVisibility(valueKey.getColumnVisibility()), valueKey.getTimestamp(),
                            entry.getValue());
            
            writer.addMutation(m);
        } catch (MutationsRejectedException | TableNotFoundException e) {
            log.trace("Unable to add entry to cache for: {}", entry.getKey());
        }
        
        return writer;
    }
    
    /**
     * Transform an Iterable of MetadataEntry's to just fieldName. This does not de-duplicate field names
     *
     * @param metadataEntries
     *            an Iterable of {@link MetadataEntry}
     * @return an Iterable of Strings
     */
    public static Iterable<String> fieldNames(Iterable<MetadataEntry> metadataEntries) {
        return Iterables.transform(metadataEntries, toFieldName);
    }
    
    /**
     * Transform an Iterable of MetadataEntry's to just fieldName, removing duplicates.
     *
     * @param metadataEntries
     *            an Iterable of {@link MetadataEntry}
     * @return an Iterable of Strings
     */
    public static Set<String> uniqueFieldNames(Iterable<MetadataEntry> metadataEntries) {
        return Sets.newHashSet(fieldNames(metadataEntries));
    }
    
    /**
     * Transform an Iterable of MetadataEntry's to just datatype. This does not de-duplicate datatypes
     *
     * @param metadataEntries
     *            an iterable of metadata entries
     * @return an Iterable of datatypes
     */
    public static Iterable<String> datatypes(Iterable<MetadataEntry> metadataEntries) {
        return Iterables.transform(metadataEntries, toDatatype);
    }
    
    /**
     * Transform an Iterable of MetadataEntry's to just datatype, removing duplicates.
     *
     * @param metadataEntries
     *            an iterable of metadata entries
     * @return the set of unique datatypes
     */
    public static Set<String> uniqueDatatypes(Iterable<MetadataEntry> metadataEntries) {
        return Sets.newHashSet(datatypes(metadataEntries));
    }
    
    /**
     * Fetches the first entry from each row in the table. This equates to the set of all fields that have occurred in the database. Returns a multimap of
     * datatype to field
     *
     * @return the multimap of datatype to fields
     * @throws TableNotFoundException
     *             if no table exists
     */
    protected Multimap<String,String> loadAllFields() throws TableNotFoundException {
        Multimap<String,String> fields = HashMultimap.create();
        
        try (Scanner bs = ScannerHelper.createScanner(accumuloClient, metadataTableName, auths)) {
            if (log.isTraceEnabled()) {
                log.trace("loadAllFields from table: {}", metadataTableName);
            }
            
            bs.setRange(new Range());
            
            // We don't want to fetch all columns because that could include model
            // field names
            bs.fetchColumnFamily(ColumnFamilyConstants.COLF_T);
            bs.fetchColumnFamily(ColumnFamilyConstants.COLF_I);
            bs.fetchColumnFamily(ColumnFamilyConstants.COLF_E);
            bs.fetchColumnFamily(ColumnFamilyConstants.COLF_RI);
            bs.fetchColumnFamily(ColumnFamilyConstants.COLF_TF);
            bs.fetchColumnFamily(ColumnFamilyConstants.COLF_CI);
            
            for (Entry<Key,Value> entry : bs) {
                Key k = entry.getKey();
                String fieldname = k.getRow().toString();
                String datatype = getDatatype(k);
                fields.put(datatype, fieldname);
            }
        }
        return Multimaps.unmodifiableMultimap(fields);
    }
    
    /**
     * Fetches results from metadata table and calculates the set of fieldNames which are indexed but do not appear as an attribute on the Event Returns a
     * multimap of datatype to field
     * 
     * @throws TableNotFoundException
     *             if no table exists
     */
    protected Multimap<String,String> loadIndexOnlyFields() throws TableNotFoundException {
        return this.allFieldMetadataHelper.getIndexOnlyFields();
    }
    
    /**
     * Fetch the Set of all fields marked as containing term frequency information, {@link ColumnFamilyConstants#COLF_TF}. Returns a multimap of datatype to
     * field
     *
     * @return a multimap of datatype to term frequency fields
     * @throws TableNotFoundException
     *             if no table exists
     */
    protected Multimap<String,String> loadTermFrequencyFields() throws TableNotFoundException {
        Multimap<String,String> fields = HashMultimap.create();
        if (log.isTraceEnabled()) {
            log.trace("loadTermFrequencyFields from table: {}", metadataTableName);
        }
        
        // Scanner to the provided metadata table
        try (Scanner bs = ScannerHelper.createScanner(accumuloClient, metadataTableName, auths)) {
            
            bs.setRange(new Range());
            bs.fetchColumnFamily(ColumnFamilyConstants.COLF_TF);
            
            for (Entry<Key,Value> entry : bs) {
                fields.put(getDatatype(entry.getKey()), entry.getKey().getRow().toString());
            }
        }
        
        return Multimaps.unmodifiableMultimap(fields);
    }
    
    private static String getKey(String instanceID, String metadataTableName) {
        StringBuilder builder = new StringBuilder();
        builder.append(instanceID).append('\0');
        builder.append(metadataTableName).append('\0');
        return builder.toString();
    }
    
    private static String getKey(MetadataHelper helper) {
        return getKey(helper.accumuloClient.instanceOperations().getInstanceId().canonical(), helper.metadataTableName);
    }
    
    @Override
    public String toString() {
        return getKey(this);
    }
    
    /**
     * TODO remove this method
     *
     * @param client
     *            an accumulo client
     * @param tableName
     *            a table name
     * @param auths
     *            Authorizations
     * @throws TableNotFoundException
     *             if no table exists
     * @throws InvalidProtocolBufferException
     *             can't be thrown, remove
     */
    @Deprecated(forRemoval = true, since = "4.0.0")
    public static void basicIterator(AccumuloClient client, String tableName, Collection<Authorizations> auths)
                    throws TableNotFoundException, InvalidProtocolBufferException {
        if (log.isTraceEnabled()) {
            log.trace("--- basicIterator --- {}", tableName);
        }
        
        try (Scanner scanner = client.createScanner(tableName, auths.iterator().next())) {
            Range range = new Range();
            scanner.setRange(range);
            for (Entry<Key,Value> entry : scanner) {
                Key k = entry.getKey();
                if (log.isTraceEnabled()) {
                    log.trace("Key: {}", k);
                }
            }
        }
    }
    
    public String getMetadataTableName() {
        return metadataTableName;
    }
    
    public int getTypeCacheSize() {
        return allFieldMetadataHelper.getTypeCacheSize();
    }
    
    public void setTypeCacheSize(int typeCacheSize) {
        allFieldMetadataHelper.setTypeCacheSize(typeCacheSize);
    }
    
    public int getTypeCacheExpirationInMinutes() {
        return allFieldMetadataHelper.getTypeCacheExpirationInMinutes();
    }
    
    public void setTypeCacheExpirationInMinutes(int typeCacheExpirationInMinutes) {
        allFieldMetadataHelper.setTypeCacheExpirationInMinutes(typeCacheExpirationInMinutes);
    }
}
