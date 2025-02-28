package datawave.query.util;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.RegExFilter;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;

import datawave.data.ColumnFamilyConstants;
import datawave.data.type.Type;
import datawave.data.type.TypeFactory;
import datawave.query.composite.CompositeMetadata;
import datawave.query.composite.CompositeMetadataHelper;
import datawave.query.model.IndexFieldHole;
import datawave.security.util.AuthorizationsMinimizer;
import datawave.security.util.ScannerHelper;
import datawave.util.time.DateHelper;

@EnableCaching
@Component("allFieldMetadataHelper")
@Scope("prototype")
public class AllFieldMetadataHelper {
    private static final Logger log = LoggerFactory.getLogger(AllFieldMetadataHelper.class);
    
    public static final String NULL_BYTE = "\0";
    
    protected static final Function<MetadataEntry,String> toFieldName = new MetadataEntryToFieldName();
    protected static final Function<MetadataEntry,String> toDatatype = new MetadataEntryToDatatype();
    
    protected final Metadata metadata = new Metadata();
    
    protected final List<Text> metadataIndexColfs = Arrays.asList(ColumnFamilyConstants.COLF_I, ColumnFamilyConstants.COLF_RI);
    protected final List<Text> metadataNormalizedColfs = List.of(ColumnFamilyConstants.COLF_N);
    protected final List<Text> metadataTypeColfs = List.of(ColumnFamilyConstants.COLF_T);
    protected final List<Text> metadataCompositeIndexColfs = List.of(ColumnFamilyConstants.COLF_CI);
    
    protected final AccumuloClient accumuloClient;
    protected final String metadataTableName;
    protected final Set<Authorizations> auths;
    protected final Set<Authorizations> fullUserAuths;
    
    protected final TypeMetadataHelper typeMetadataHelper;
    protected final CompositeMetadataHelper compositeMetadataHelper;
    
    private int typeCacheSize = -1;
    private int typeCacheExpirationInMinutes = -1;
    protected TypeFactory typeFactory = null;
    
    /**
     * Initializes the instance with a provided update interval.
     *
     * @param client
     *            A client connection to Accumulo
     * @param metadataTableName
     *            The name of the DatawaveMetadata table
     * @param auths
     *            Any {@link Authorizations} to use
     */
    public AllFieldMetadataHelper(TypeMetadataHelper typeMetadataHelper, CompositeMetadataHelper compositeMetadataHelper, AccumuloClient client,
                    String metadataTableName, Set<Authorizations> auths, Set<Authorizations> fullUserAuths) {
        Preconditions.checkNotNull(typeMetadataHelper, "A TypeMetadataHelper is required by AllFieldMetadataHelper");
        this.typeMetadataHelper = typeMetadataHelper;
        
        Preconditions.checkNotNull(compositeMetadataHelper, "A CompositeMetadataHelper is required by AllFieldMetadataHelper");
        this.compositeMetadataHelper = compositeMetadataHelper;
        
        Preconditions.checkNotNull(client, "A valid AccumuloClient is required by AllFieldMetadataHelper");
        this.accumuloClient = client;
        
        Preconditions.checkNotNull(metadataTableName, "The name of the metadata table is required by AllFieldMetadataHelper");
        this.metadataTableName = metadataTableName;
        
        Preconditions.checkNotNull(auths, "Authorizations are required by AllFieldMetadataHelper");
        this.auths = auths;
        
        Preconditions.checkNotNull(fullUserAuths, "The full set of user authorizations is required by AllFieldMetadataHelper");
        this.fullUserAuths = fullUserAuths;
        
        log.trace("Constructor  connector: {} and metadata table name: {}", accumuloClient.getClass().getCanonicalName(), metadataTableName);
    }
    
    /**
     * Get the datatype from a key's column qualifier
     * 
     * @param k
     *            the key
     * @return the datatype
     */
    protected String getDatatype(Key k) {
        String datatype = k.getColumnQualifier().toString();
        int index = datatype.indexOf('\0');
        if (index >= 0) {
            datatype = datatype.substring(0, index);
        }
        return datatype;
    }
    
    /**
     * Get the field name from a composite key
     * 
     * @param k
     *            the key
     * @return the field name
     */
    protected String getCompositeFieldName(Key k) {
        String compositeFieldName = k.getColumnQualifier().toString();
        int index = compositeFieldName.indexOf('\0');
        if (index >= 0) {
            compositeFieldName = compositeFieldName.substring(index + 1);
            index = compositeFieldName.indexOf(',');
            if (index != -1) {
                compositeFieldName = compositeFieldName.substring(0, index);
            }
        }
        return compositeFieldName;
    }
    
    /**
     * Get the authorizations used by this helper
     * 
     * @return the authorizations
     */
    public Set<Authorizations> getAuths() {
        return auths;
    }
    
    /**
     * Get the full user authorizations used by this helper
     * 
     * @return the full user authorizations
     */
    public Set<Authorizations> getFullUserAuths() {
        return fullUserAuths;
    }
    
    /**
     * Get the metadata table name
     * 
     * @return the metadata table name
     */
    public String getMetadataTableName() {
        return metadataTableName;
    }
    
    /**
     * Get the {@link TypeMetadataHelper}
     * 
     * @return the TypeMetadataHelper
     */
    public TypeMetadataHelper getTypeMetadataHelper() {
        return typeMetadataHelper;
    }
    
    /**
     * Method that determines whether a column exists in the metadata table for the given key.
     * 
     * @param colf
     *            the column family
     * @param key
     *            the key, an Entry of table name, field, and ingest types
     * @return true if the key exists in the metadata table
     * @throws TableNotFoundException
     *             if no table exists
     * @throws InstantiationException
     *             not thrown, remove
     * @throws ExecutionException
     *             not thrown, remove
     */
    @Cacheable(value = "isIndexed", key = "{#root.target.auths,#root.target.metadataTableName,#colf,#key}", cacheManager = "metadataHelperCacheManager",
                    sync = true)
    // using cache with higher maximumSize
    public Boolean isIndexed(Text colf, Entry<String,Entry<String,Set<String>>> key) throws TableNotFoundException, InstantiationException, ExecutionException {
        log.debug("cache fault for isIndexed({}, {}, {}, {})", this.auths, this.metadataTableName, colf, key);
        Preconditions.checkNotNull(key);
        
        final String tableName = key.getKey();
        final String fieldName = key.getValue().getKey();
        final Set<String> datatype = key.getValue().getValue();
        
        Preconditions.checkNotNull(fieldName);
        
        // FieldNames are upper case by convention
        String upCaseFieldName = fieldName.toUpperCase();
        
        // Scanner to the provided metadata table
        try (Scanner scanner = ScannerHelper.createScanner(accumuloClient, tableName, auths)) {
            
            Range range = new Range(upCaseFieldName);
            scanner.setRange(range);
            scanner.fetchColumnFamily(colf);
            
            for (Entry<Key,Value> entry : scanner) {
                
                if (entry.getKey().getColumnQualifier() == null) {
                    log.warn("ColumnQualifier null in ColumnFamilyConstants for key: {}", entry.getKey());
                    continue;
                }
                
                // Get the column qualifier from the key. It contains the ingest type
                // and datatype class
                String colq = entry.getKey().getColumnQualifier().toString();
                
                // there should not be a null byte and Normalizer class in the 'i' entry for version3+
                int idx = colq.indexOf(NULL_BYTE);
                if (idx != -1) {
                    colq = colq.substring(0, idx);
                }
                
                // If types are specified and this type is not in the list,
                // skip it.
                if (datatype == null || datatype.isEmpty() || datatype.contains(colq)) {
                    return true;
                }
            }
        }
        return false;
    }
    
    /**
     * Returns a Set of all Types in use by any type in Accumulo
     * 
     * @return the set of all {@link Type}s
     * @throws InstantiationException
     *             if a Type cannot be created
     * @throws IllegalAccessException
     *             if a Type cannot be created
     * @throws TableNotFoundException
     *             if no table exists
     */
    @Cacheable(value = "getAllDatatypes", key = "{#root.target.auths,#root.target.metadataTableName}", cacheManager = "metadataHelperCacheManager")
    public Set<Type<?>> getAllDatatypes() throws InstantiationException, IllegalAccessException, TableNotFoundException {
        log.debug("cache fault for getAllDatatypes({}, {})", this.auths, this.metadataTableName);
        
        if (log.isTraceEnabled()) {
            log.trace("getAllDatatypes from table: {}", metadataTableName);
        }
        
        Set<Type<?>> datatypes = Sets.newHashSetWithExpectedSize(10);
        
        try (Scanner bs = ScannerHelper.createScanner(accumuloClient, metadataTableName, auths)) {
            bs.setRange(new Range());
            bs.fetchColumnFamily(ColumnFamilyConstants.COLF_T);
            
            for (Entry<Key,Value> entry : bs) {
                Key key = entry.getKey();
                
                if (key.getColumnQualifier() == null) {
                    log.warn("ColumnQualifier null in EventMetadata for key: {}", key);
                }
                
                // Get the column qualifier from the key. It contains the
                // datatype and normalizer class
                String colq = key.getColumnQualifier().toString();
                int idx = colq.indexOf(NULL_BYTE);
                if (idx != -1) {
                    try {
                        @SuppressWarnings("unchecked")
                        Class<? extends Type<?>> clazz = (Class<? extends Type<?>>) Class.forName(colq.substring(idx + 1));
                        
                        datatypes.add(getDatatypeFromClass(clazz));
                    } catch (ClassNotFoundException e) {
                        log.error("Unable to find normalizer on class path: {}", colq.substring(idx + 1), e);
                    }
                } else {
                    log.warn("ColumnFamilyConstants entry did not contain a null byte in the column qualifier: {}", key);
                }
            }
        }
        
        return Collections.unmodifiableSet(datatypes);
    }
    
    /**
     * A map of composite name to the ordered list of it for example, mapping of {@code COLOR -> ['COLOR_WHEELS', 'MAKE_COLOR' ]}. If called multiple time, it
     * returns the same cached map.
     * 
     * @return An unmodifiable Multimap
     * @throws TableNotFoundException
     *             if no table exists
     */
    @Cacheable(value = "getCompositeToFieldMap", key = "{#root.target.auths,#root.target.metadataTableName}", cacheManager = "metadataHelperCacheManager")
    public Multimap<String,String> getCompositeToFieldMap() throws TableNotFoundException {
        log.debug("cache fault for getCompositeToFieldMap({}, {})", this.auths, this.metadataTableName);
        return this.getCompositeToFieldMap(null);
    }
    
    /**
     * A map of composite name to the ordered list of it for example, mapping of {@code COLOR -> ['COLOR_WHEELS', 'MAKE_COLOR' ]}. If called multiple time, it
     * returns the same cached map.
     *
     * @param ingestTypeFilter
     *            the set of ingest types used to filter the scan
     * @return the multimap of field to composite fields
     * @throws TableNotFoundException
     *             if no table exists
     */
    @Cacheable(value = "getCompositeToFieldMap", key = "{#root.target.auths,#root.target.metadataTableName,#ingestTypeFilter}",
                    cacheManager = "metadataHelperCacheManager")
    public Multimap<String,String> getCompositeToFieldMap(Set<String> ingestTypeFilter) throws TableNotFoundException {
        log.debug("cache fault for getCompositeToFieldMap({}, {}, {})", this.auths, this.metadataTableName, ingestTypeFilter);
        
        ArrayListMultimap<String,String> compositeToFieldMap = ArrayListMultimap.create();
        
        try (Scanner bs = ScannerHelper.createScanner(accumuloClient, metadataTableName, auths)) {
            bs.setRange(new Range());
            bs.fetchColumnFamily(ColumnFamilyConstants.COLF_CI);
            
            for (Entry<Key,Value> entry : bs) {
                String fieldName = entry.getKey().getRow().toString();
                
                if (entry.getKey().getColumnQualifier() == null) {
                    log.warn("ColumnQualifier null in EventMetadata for key: {}", entry.getKey());
                    continue;
                }
                
                String colq = entry.getKey().getColumnQualifier().toString();
                int idx = colq.indexOf(NULL_BYTE);
                
                String type = colq.substring(0, idx);
                
                // If types are specified and this type is not in the list,
                // skip it.
                if (null != ingestTypeFilter && !ingestTypeFilter.isEmpty() && !ingestTypeFilter.contains(type)) {
                    continue;
                }
                
                if (idx != -1) {
                    String[] componentFields = colq.substring(idx + 1).split(",");
                    compositeToFieldMap.putAll(fieldName, Arrays.asList(componentFields));
                } else {
                    log.warn("EventMetadata entry did not contain a null byte in the column qualifier: {}", entry.getKey());
                }
            }
        }
        
        return Multimaps.unmodifiableMultimap(compositeToFieldMap);
    }
    
    /**
     * A map of composite name to transition date.
     *
     * @return An unmodifiable Map
     * @throws TableNotFoundException
     *             if no table exists
     */
    @Cacheable(value = "getCompositeTransitionDateMap", key = "{#root.target.auths,#root.target.metadataTableName}",
                    cacheManager = "metadataHelperCacheManager")
    public Map<String,Date> getCompositeTransitionDateMap() throws TableNotFoundException {
        log.debug("cache fault for getCompositeTransitionDateMap({}, {})", this.auths, this.metadataTableName);
        return this.getCompositeTransitionDateMap(null);
    }
    
    /**
     * A map of composite name to transition date.
     *
     * @param ingestTypeFilter
     *            the set of ingest types used to filter the scan
     * @return An unmodifiable Map
     * @throws TableNotFoundException
     *             if no table exists
     */
    
    @Cacheable(value = "getCompositeTransitionDateMap", key = "{#root.target.auths,#root.target.metadataTableName,#ingestTypeFilter}",
                    cacheManager = "metadataHelperCacheManager")
    public Map<String,Date> getCompositeTransitionDateMap(Set<String> ingestTypeFilter) throws TableNotFoundException {
        log.debug("cache fault for getCompositeTransitionDateMap({}, {}, {})", this.auths, this.metadataTableName, ingestTypeFilter);
        
        Map<String,Date> tdMap = new HashMap<>();
        
        SimpleDateFormat dateFormat = new SimpleDateFormat(CompositeMetadataHelper.transitionDateFormat);
        
        try (Scanner bs = ScannerHelper.createScanner(accumuloClient, metadataTableName, auths)) {
            bs.setRange(new Range());
            bs.fetchColumnFamily(ColumnFamilyConstants.COLF_CITD);
            
            for (Entry<Key,Value> entry : bs) {
                
                if (entry.getKey().getColumnQualifier() == null) {
                    log.warn("ColumnQualifier null in EventMetadata for key: {}", entry.getKey());
                    continue;
                }
                
                String colq = entry.getKey().getColumnQualifier().toString();
                int idx = colq.indexOf(NULL_BYTE);
                
                String type = colq.substring(0, idx);
                
                // If types are specified and this type is not in the list,
                // skip it.
                if (null != ingestTypeFilter && !ingestTypeFilter.isEmpty() && !ingestTypeFilter.contains(type)) {
                    continue;
                }
                
                if (idx != -1) {
                    try {
                        String fieldName = entry.getKey().getRow().toString();
                        Date transitionDate = dateFormat.parse(colq.substring(idx + 1));
                        tdMap.put(fieldName, transitionDate);
                    } catch (ParseException e) {
                        log.trace("Unable to parse composite field transition date", e);
                    }
                } else {
                    log.warn("EventMetadata entry did not contain a null byte in the column qualifier: {}", entry.getKey());
                }
            }
        }
        
        return Collections.unmodifiableMap(tdMap);
    }
    
    /**
     * A map of whindex field to creation date.
     *
     * @return An unmodifiable Map
     * @throws TableNotFoundException
     *             if no table exists
     */
    @Cacheable(value = "getWhindexCreationDateMap", key = "{#root.target.auths,#root.target.metadataTableName}", cacheManager = "metadataHelperCacheManager")
    public Map<String,Date> getWhindexCreationDateMap() throws TableNotFoundException {
        log.debug("cache fault for getWhindexCreationDateMap({}, {})", this.auths, this.metadataTableName);
        return this.getWhindexCreationDateMap(null);
    }
    
    /**
     * A map of whindex field to creation date.
     *
     * @param ingestTypeFilter
     *            set of ingest types used to filter the scan
     * @return An unmodifiable Map
     * @throws TableNotFoundException
     *             if no table exists
     */
    @Cacheable(value = "getWhindexCreationDateMap", key = "{#root.target.auths,#root.target.metadataTableName,#ingestTypeFilter}",
                    cacheManager = "metadataHelperCacheManager")
    public Map<String,Date> getWhindexCreationDateMap(Set<String> ingestTypeFilter) throws TableNotFoundException {
        log.debug("cache fault for getWhindexCreationDateMap({}, {}, {})", this.auths, this.metadataTableName, ingestTypeFilter);
        
        Map<String,Date> tdMap = new HashMap<>();
        
        // Note: Intentionally using the same transition date format as the composite fields.
        SimpleDateFormat dateFormat = new SimpleDateFormat(CompositeMetadataHelper.transitionDateFormat);
        
        try (Scanner bs = ScannerHelper.createScanner(accumuloClient, metadataTableName, auths)) {
            bs.setRange(new Range());
            bs.fetchColumnFamily(ColumnFamilyConstants.COLF_WCD);
            
            for (Entry<Key,Value> entry : bs) {
                
                if (entry.getKey().getColumnQualifier() == null) {
                    log.warn("ColumnQualifier null in EventMetadata for key: {}", entry.getKey());
                    continue;
                }
                
                String colq = entry.getKey().getColumnQualifier().toString();
                int idx = colq.indexOf(NULL_BYTE);
                
                if (idx != -1) {
                    String type = colq.substring(0, idx);
                    
                    // If types are specified and this type is not in the list,
                    // skip it.
                    if (null != ingestTypeFilter && !ingestTypeFilter.isEmpty() && !ingestTypeFilter.contains(type)) {
                        continue;
                    }
                    
                    try {
                        String fieldName = entry.getKey().getRow().toString();
                        Date transitionDate = dateFormat.parse(colq.substring(idx + 1));
                        tdMap.put(fieldName, transitionDate);
                    } catch (ParseException e) {
                        log.trace("Unable to parse whindex field creation date", e);
                    }
                } else {
                    log.warn("EventMetadata entry did not contain a null byte in the column qualifier: {}", entry.getKey());
                }
            }
        }
        
        return Collections.unmodifiableMap(tdMap);
    }
    
    /**
     * A map of composite name to field separator.
     *
     * @return An unmodifiable Map
     * @throws TableNotFoundException
     *             if no table exists
     */
    @Cacheable(value = "getCompositeFieldSeparatorMap", key = "{#root.target.auths,#root.target.metadataTableName}",
                    cacheManager = "metadataHelperCacheManager")
    public Map<String,String> getCompositeFieldSeparatorMap() throws TableNotFoundException {
        log.debug("cache fault for getCompositeFieldSeparatorMap({}, {})", this.auths, this.metadataTableName);
        return this.getCompositeFieldSeparatorMap(null);
    }
    
    /**
     * A map of composite name to field separator.
     *
     * @param ingestTypeFilter
     *            set of ingest types used to filter the scan
     * @return An unmodifiable Map
     * @throws TableNotFoundException
     *             if no table exists
     */
    @Cacheable(value = "getCompositeFieldSeparatorMap", key = "{#root.target.auths,#root.target.metadataTableName,#ingestTypeFilter}",
                    cacheManager = "metadataHelperCacheManager")
    public Map<String,String> getCompositeFieldSeparatorMap(Set<String> ingestTypeFilter) throws TableNotFoundException {
        log.debug("cache fault for getCompositeFieldSeparatorMap({}, {}, {})", this.auths, this.metadataTableName, ingestTypeFilter);
        
        Map<String,String> sepMap = new HashMap<>();
        
        try (Scanner bs = ScannerHelper.createScanner(accumuloClient, metadataTableName, auths)) {
            bs.setRange(new Range());
            bs.fetchColumnFamily(ColumnFamilyConstants.COLF_CISEP);
            
            for (Entry<Key,Value> entry : bs) {
                
                if (entry.getKey().getColumnQualifier() == null) {
                    log.warn("ColumnQualifier null in EventMetadata for key: {}", entry.getKey());
                    continue;
                }
                
                String colq = entry.getKey().getColumnQualifier().toString();
                int idx = colq.indexOf(NULL_BYTE);
                
                String type = colq.substring(0, idx);
                
                // If types are specified and this type is not in the list,
                // skip it.
                if (null != ingestTypeFilter && !ingestTypeFilter.isEmpty() && !ingestTypeFilter.contains(type)) {
                    continue;
                }
                
                if (idx != -1) {
                    String fieldName = entry.getKey().getRow().toString();
                    String separator = colq.substring(idx + 1);
                    sepMap.put(fieldName, separator);
                } else {
                    log.warn("EventMetadata entry did not contain a null byte in the column qualifier: {}", entry.getKey());
                }
            }
        }
        
        return Collections.unmodifiableMap(sepMap);
    }
    
    /**
     * Get the {@link TypeMetadata} for all ingest types
     *
     * @return the TypeMetadata
     * @throws TableNotFoundException
     *             if no table exists
     */
    public TypeMetadata getTypeMetadata() throws TableNotFoundException {
        return this.typeMetadataHelper.getTypeMetadata(null);
    }
    
    /**
     * Get the {@link TypeMetadata} for a particular set of ingest types
     * 
     * @param ingestTypeFilter
     *            the set of ingest types used to filter the scan
     * @return the {@link TypeMetadata} for a particular set of ingest types
     * @throws TableNotFoundException
     *             if no table exists
     */
    public TypeMetadata getTypeMetadata(Set<String> ingestTypeFilter) throws TableNotFoundException {
        return this.typeMetadataHelper.getTypeMetadata(ingestTypeFilter);
    }
    
    /**
     * Get the {@link CompositeMetadata} for all ingest types
     *
     * @return the CompositeMetadata
     * @throws TableNotFoundException
     *             if no table exists
     */
    public CompositeMetadata getCompositeMetadata() throws TableNotFoundException {
        return this.compositeMetadataHelper.getCompositeMetadata(null);
    }
    
    /**
     * Get the {@link CompositeMetadata} for the specified ingest types
     * 
     * @param ingestTypeFilter
     *            the set of ingest types used to filter the scan
     * @return the CompositeMetadata
     * @throws TableNotFoundException
     *             if no table exists
     */
    public CompositeMetadata getCompositeMetadata(Set<String> ingestTypeFilter) throws TableNotFoundException {
        return this.compositeMetadataHelper.getCompositeMetadata(ingestTypeFilter);
    }
    
    /**
     * Fetch the set of {@link Type}s that are configured for this <code>fieldName</code> as specified in the table pointed to by the
     * <code>metadataTableName</code> parameter.
     * 
     * @param ingestTypeFilter
     *            Any projection of datatypes to limit the fetch for.
     * @return a multimap of fields to Types
     * @throws InstantiationException
     *             not thrown, remove
     * @throws IllegalAccessException
     *             not thrown, remove
     * @throws TableNotFoundException
     *             if no table exists
     */
    @Cacheable(value = "getFieldsToDatatypes", key = "{#root.target.auths,#root.target.metadataTableName,#ingestTypeFilter}",
                    cacheManager = "metadataHelperCacheManager")
    public Multimap<String,Type<?>> getFieldsToDatatypes(Set<String> ingestTypeFilter)
                    throws InstantiationException, IllegalAccessException, TableNotFoundException {
        log.debug("cache fault for getFieldsToDatatypes({}, {}, {})", this.auths, this.metadataTableName, ingestTypeFilter);
        TypeMetadata typeMetadata = this.typeMetadataHelper.getTypeMetadata(ingestTypeFilter);
        Multimap<String,Type<?>> typeMap = HashMultimap.create();
        for (Entry<String,String> entry : typeMetadata.fold().entries()) {
            String value = entry.getValue();
            try {
                @SuppressWarnings("unchecked")
                Class<? extends Type<?>> clazz = (Class<? extends Type<?>>) Class.forName(value);
                typeMap.put(entry.getKey(), getDatatypeFromClass(clazz));
            } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
                log.error("Unable to find datatype on class path: {}", value, e);
            }
            
        }
        return typeMap;
    }
    
    /**
     * Scans the metadata table and returns the set of fields that use the supplied normalizer.
     * 
     * @param datawaveType
     *            the datawave {@link Type}
     * @return the set of fields associated with the provided Type
     * @throws InstantiationException
     *             not thrown, remove
     * @throws IllegalAccessException
     *             not thrown, remove
     * @throws TableNotFoundException
     *             if no table exists
     */
    @Cacheable(value = "getFieldsForDatatype", key = "{#root.target.auths,#root.target.metadataTableName,#datawaveType}",
                    cacheManager = "metadataHelperCacheManager")
    public Set<String> getFieldsForDatatype(Class<? extends Type<?>> datawaveType)
                    throws InstantiationException, IllegalAccessException, TableNotFoundException {
        log.debug("cache fault for getFieldsForDatatype({})", datawaveType);
        return getFieldsForDatatype(datawaveType, null);
    }
    
    /**
     * Scans the metadata table and returns the set of fields that use the supplied normalizer.
     * <p>
     * This method allows a client to specify data types to filter out. If the set is null, then it assumed the user wants all data types. If the set is empty,
     * then it assumed the user wants no data types. Otherwise, values that occur in the set will be used as a white list of data types.
     * 
     * @param datawaveType
     *            the datawave {@link Type}
     * @param ingestTypeFilter
     *            the set of ingest types used to filter the scan
     * @return the set of fields associated with the provided Type
     * @throws TableNotFoundException
     *             if no table exists
     */
    @Cacheable(value = "getFieldsForDatatype", key = "{#root.target.auths,#root.target.metadataTableName,#datawaveType,#ingestTypeFilter}",
                    cacheManager = "metadataHelperCacheManager")
    public Set<String> getFieldsForDatatype(Class<? extends Type<?>> datawaveType, Set<String> ingestTypeFilter) throws TableNotFoundException {
        log.debug("cache fault for getFieldsForDatatype({}, {})", datawaveType, ingestTypeFilter);
        TypeMetadata typeMetadata = this.typeMetadataHelper.getTypeMetadata(ingestTypeFilter);
        String datawaveTypeClassName = datawaveType.getName();
        
        // datatype class name to field name <--field name to datatype class name
        Multimap<String,String> inverted = Multimaps.invertFrom(typeMetadata.fold(ingestTypeFilter), HashMultimap.create());
        
        return Sets.newHashSet(inverted.get(datawaveTypeClassName));
    }
    
    /**
     * Pull an instance of the provided normalizer class name from the internal cache.
     * 
     * @param datatypeClass
     *            The name of the normalizer class to instantiate.
     * @return An instance of the normalizer class that was requested.
     * @throws InstantiationException
     *             if the class cannot be instantiated
     * @throws IllegalAccessException
     *             if the class is not accessible
     */
    protected Type<?> getDatatypeFromClass(Class<? extends Type<?>> datatypeClass) throws InstantiationException, IllegalAccessException {
        return getTypeFactory().createType(datatypeClass.getName());
    }
    
    /**
     * Fetch the Set of all fields marked as containing term frequency information, {@link ColumnFamilyConstants#COLF_TF}.
     *
     * @param ingestTypeFilter
     *            the set of ingest types used to limit the scan
     * @return the set of term frequency fields
     * @throws TableNotFoundException
     *             if no table exists
     * @throws ExecutionException
     *             not thrown, remove
     */
    public Set<String> getTermFrequencyFields(Set<String> ingestTypeFilter) throws TableNotFoundException, ExecutionException {
        
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
     * Get expansion fields using the data type filter.
     * 
     * @param ingestTypeFilter
     *            a set of ingest types used to filter the scan
     * @return the set of fields marked as expansion
     * @throws TableNotFoundException
     *             if no table exists
     */
    public Set<String> getExpansionFields(Set<String> ingestTypeFilter) throws TableNotFoundException {
        
        Multimap<String,String> expansionFields = loadExpansionFields();
        
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
     *            the set of ingest types used to filter the scan
     * @return the set of content fields
     * @throws TableNotFoundException
     *             if no table exists
     */
    public Set<String> getContentFields(Set<String> ingestTypeFilter) throws TableNotFoundException {
        
        Multimap<String,String> contentFields = loadContentFields();
        
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
     * Get the set of ingest types that exist in the database
     * 
     * @param ingestTypeFilter
     *            a set of ingest types
     * @return the set of ingest types that exist
     * @throws TableNotFoundException
     *             if no table exists
     */
    public Set<String> getDatatypes(Set<String> ingestTypeFilter) throws TableNotFoundException {
        
        Set<String> datatypes = loadDatatypes();
        if (ingestTypeFilter != null && !ingestTypeFilter.isEmpty()) {
            datatypes = Sets.newHashSet(Sets.intersection(datatypes, ingestTypeFilter));
        }
        
        return Collections.unmodifiableSet(datatypes);
    }
    
    /**
     * Get the counts of a field for a particular day
     *
     * @param identifier
     *            and entry of field name and date
     * @return a map of datatypes and counts
     * @throws TableNotFoundException
     *             if no table exists
     * @throws IOException
     *             if there is a problem deserializing the Values
     */
    protected HashMap<String,Long> getCountsByFieldInDayWithTypes(Entry<String,String> identifier) throws TableNotFoundException, IOException {
        String fieldName = identifier.getKey();
        String date = identifier.getValue();
        
        final HashMap<String,Long> datatypeToCounts;
        try (Scanner scanner = ScannerHelper.createScanner(accumuloClient, metadataTableName, auths)) {
            scanner.fetchColumnFamily(ColumnFamilyConstants.COLF_F);
            scanner.setRange(Range.exact(fieldName));
            
            IteratorSetting cqRegex = new IteratorSetting(50, RegExFilter.class);
            RegExFilter.setRegexs(cqRegex, null, null, ".*\u0000" + date, null, false);
            scanner.addScanIterator(cqRegex);
            
            final Text holder = new Text();
            datatypeToCounts = Maps.newHashMap();
            for (Entry<Key,Value> countEntry : scanner) {
                ByteArrayInputStream bais = new ByteArrayInputStream(countEntry.getValue().get());
                DataInputStream inputStream = new DataInputStream(bais);
                
                Long sum = WritableUtils.readVLong(inputStream);
                
                countEntry.getKey().getColumnQualifier(holder);
                int offset = holder.find(NULL_BYTE);
                
                Preconditions.checkArgument(-1 != offset, "Could not find nullbyte separator in column qualifier for: " + countEntry.getKey());
                
                String datatype = Text.decode(holder.getBytes(), 0, offset);
                
                datatypeToCounts.put(datatype, sum);
            }
        }
        
        return datatypeToCounts;
    }
    
    /**
     * Transform an Iterable of MetadataEntry's to just fieldName. This does not de-duplicate field names
     * 
     * @param from
     *            the metadata entries
     * @return the field names
     */
    public static Iterable<String> fieldNames(Iterable<MetadataEntry> from) {
        return Iterables.transform(from, toFieldName);
    }
    
    /**
     * Transform an Iterable of MetadataEntry's to just fieldName, removing duplicates.
     * 
     * @param from
     *            the metadata entries
     * @return the unique field name
     */
    public static Set<String> uniqueFieldNames(Iterable<MetadataEntry> from) {
        return Sets.newHashSet(fieldNames(from));
    }
    
    /**
     * Transform an Iterable of MetadataEntry's to just datatype. This does not de-duplicate datatypes
     * 
     * @param from
     *            the metadata entries
     * @return the datatypes
     */
    public static Iterable<String> datatypes(Iterable<MetadataEntry> from) {
        return Iterables.transform(from, toDatatype);
    }
    
    /**
     * Transform an Iterable of MetadataEntry's to just datatype, removing duplicates.
     * 
     * @param from
     *            the metadata entries
     * @return the unique datatypes
     */
    public static Set<String> uniqueDatatypes(Iterable<MetadataEntry> from) {
        return Sets.newHashSet(datatypes(from));
    }
    
    /**
     * Fetches the first entry from each row in the {@link #metadataTableName} table. This equates to the set of all fields that have occurred in the database.
     * Returns a multimap of datatype to field
     * 
     * @throws TableNotFoundException
     *             if no table exists
     */
    @Cacheable(value = "loadAllFields", key = "{#root.target.auths,#root.target.metadataTableName}", cacheManager = "metadataHelperCacheManager")
    public Multimap<String,String> loadAllFields() throws TableNotFoundException {
        log.debug("cache fault for loadAllFields({}, {})", this.auths, this.metadataTableName);
        if (log.isTraceEnabled()) {
            log.trace("Using these minimized auths: {}", AuthorizationsMinimizer.minimize(this.auths).iterator().next());
            log.trace("loadAllFields from table: {}", metadataTableName);
        }
        
        Multimap<String,String> fields = HashMultimap.create();
        
        try (Scanner bs = ScannerHelper.createScanner(accumuloClient, metadataTableName, auths)) {
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
                String fieldName = k.getRow().toString();
                String datatype = getDatatype(k);
                fields.put(datatype, fieldName);
            }
        }
        return Multimaps.unmodifiableMultimap(fields);
    }
    
    /**
     * Fetches results from {@link #metadataTableName} and calculates the set of fieldNames which are indexed but do not appear as an attribute on the Event
     * Returns a multimap of datatype to field
     * 
     * @throws TableNotFoundException
     *             if no table exists
     */
    @Cacheable(value = "getIndexOnlyFields", key = "{#root.target.auths,#root.target.metadataTableName}", cacheManager = "metadataHelperCacheManager")
    public Multimap<String,String> getIndexOnlyFields() throws TableNotFoundException {
        log.debug("cache fault for getIndexOnlyFields({}, {})", this.auths, this.metadataTableName);
        
        if (log.isTraceEnabled()) {
            log.trace("loadIndexOnlyFields from table: {}", metadataTableName);
        }
        
        Multimap<String,String> fields = HashMultimap.create();
        
        final Map<String,Multimap<Text,Text>> metadata = new HashMap<>();
        
        try (Scanner bs = ScannerHelper.createScanner(accumuloClient, metadataTableName, auths)) {
            // Fetch the 'e' and 'i' columns
            bs.fetchColumnFamily(ColumnFamilyConstants.COLF_E);
            bs.fetchColumnFamily(ColumnFamilyConstants.COLF_I);
            bs.fetchColumnFamily(ColumnFamilyConstants.COLF_CI);
            
            // For all keys in the DatawaveMetadata table
            bs.setRange(new Range());
            
            Iterator<Entry<Key,Value>> iterator = bs.iterator();
            Set<String> compositeFields = Sets.newHashSet();
            // Collect the results and put them into a Multimap
            while (iterator.hasNext()) {
                Entry<Key,Value> entry = iterator.next();
                Key k = entry.getKey();
                Text fieldName = k.getRow();
                Text fieldType = k.getColumnFamily();
                String dataType = getDatatype(k);
                if (fieldType.equals(ColumnFamilyConstants.COLF_CI)) {
                    compositeFields.add(getCompositeFieldName(k));
                }
                
                Multimap<Text,Text> md = metadata.get(dataType);
                if (md == null) {
                    md = HashMultimap.create();
                    metadata.put(dataType, md);
                    
                }
                md.put(fieldName, fieldType);
            }
            
            // Find all of the fields which only have the 'i' column
            for (String dataType : metadata.keySet()) {
                for (Text fieldName : metadata.get(dataType).keySet()) {
                    Collection<Text> columns = metadata.get(dataType).get(fieldName);
                    
                    if (1 == columns.size()) {
                        Text c = columns.iterator().next();
                        
                        if (c.equals(ColumnFamilyConstants.COLF_I)) {
                            if (compositeFields.contains(fieldName.toString()) == false) {
                                fields.put(dataType, fieldName.toString());
                            }
                        }
                    }
                }
            }
        }
        
        return Multimaps.unmodifiableMultimap(fields);
    }
    
    /**
     * Fetch the Set of all fields marked as containing term frequency information, {@link ColumnFamilyConstants#COLF_TF}. Returns a multimap of datatype to
     * field
     * 
     * @return the multimap of datatypes to term frequency fields
     * @throws TableNotFoundException
     *             if no table exists
     */
    @Cacheable(value = "loadTermFrequencyFields", key = "{#root.target.auths,#root.target.metadataTableName}", cacheManager = "metadataHelperCacheManager")
    public Multimap<String,String> loadTermFrequencyFields() throws TableNotFoundException {
        log.debug("cache fault for loadTermFrequencyFields({}, {})", this.auths, this.metadataTableName);
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
    
    /**
     * Fetch the Set of all fields marked as being indexed, {@link ColumnFamilyConstants#COLF_I}. Returns a multimap of datatype to field
     * 
     * @return a multimap of datatypes to indexed fields
     * @throws TableNotFoundException
     *             if no table exists
     */
    @Cacheable(value = "loadIndexedFields", key = "{#root.target.auths,#root.target.metadataTableName}", cacheManager = "metadataHelperCacheManager")
    public Multimap<String,String> loadIndexedFields() throws TableNotFoundException {
        log.debug("cache fault for loadIndexedFields({}, {})", this.auths, this.metadataTableName);
        
        if (log.isTraceEnabled()) {
            log.trace("loadIndexedFields from table: {}", metadataTableName);
        }
        
        Multimap<String,String> fields = HashMultimap.create();
        
        try (Scanner bs = ScannerHelper.createScanner(accumuloClient, metadataTableName, auths)) {
            bs.setRange(new Range());
            bs.fetchColumnFamily(ColumnFamilyConstants.COLF_I);
            
            for (Entry<Key,Value> entry : bs) {
                fields.put(getDatatype(entry.getKey()), entry.getKey().getRow().toString());
            }
        }
        
        return Multimaps.unmodifiableMultimap(fields);
    }
    
    /**
     * Fetch the Set of all fields marked as being reverse indexed, {@link ColumnFamilyConstants#COLF_RI}. Returns a multimap of datatype to field
     *
     * @return the multimap of datatypes to reverse indexed fields
     * @throws TableNotFoundException
     *             if no table exists
     */
    @Cacheable(value = "loadReverseIndexedFields", key = "{#root.target.auths,#root.target.metadataTableName}", cacheManager = "metadataHelperCacheManager")
    public Multimap<String,String> loadReverseIndexedFields() throws TableNotFoundException {
        log.debug("cache fault for loadReverseIndexedFields({}, {})", this.auths, this.metadataTableName);
        
        if (log.isTraceEnabled()) {
            log.trace("loadReverseIndexedFields from table: {}", metadataTableName);
        }
        
        Multimap<String,String> fields = HashMultimap.create();
        
        try (Scanner bs = ScannerHelper.createScanner(accumuloClient, metadataTableName, auths)) {
            bs.setRange(new Range());
            bs.fetchColumnFamily(ColumnFamilyConstants.COLF_RI);
            
            for (Entry<Key,Value> entry : bs) {
                fields.put(getDatatype(entry.getKey()), entry.getKey().getRow().toString());
            }
        }
        return Multimaps.unmodifiableMultimap(fields);
    }
    
    /**
     * Fetch the Set of all fields marked as being indexed, {@link ColumnFamilyConstants#COLF_I}. Returns a multimap of datatype to field
     * 
     * @return the multimap of datatype to indexed fields
     * @throws TableNotFoundException
     *             if no table exists
     */
    @Cacheable(value = "loadIndexedFields", key = "{#root.target.fullUserAuths,#root.target.metadataTableName}", cacheManager = "metadataHelperCacheManager")
    public Multimap<String,String> loadAllIndexedFields() throws TableNotFoundException {
        log.debug("cache fault for loadIndexedFields({}, {})", this.auths, this.metadataTableName);
        
        if (log.isTraceEnabled()) {
            log.trace("loadIndexedFields from table: {}", metadataTableName);
        }
        
        Multimap<String,String> fields = HashMultimap.create();
        
        try (Scanner bs = ScannerHelper.createScanner(accumuloClient, metadataTableName, fullUserAuths)) {
            bs.setRange(new Range());
            bs.fetchColumnFamily(ColumnFamilyConstants.COLF_I);
            
            for (Entry<Key,Value> entry : bs) {
                fields.put(getDatatype(entry.getKey()), entry.getKey().getRow().toString());
            }
        }
        return Multimaps.unmodifiableMultimap(fields);
    }
    
    /**
     * Fetch the Set of all fields marked as being expansion fields, {@link ColumnFamilyConstants#COLF_EXP}. Returns a multimap of datatype to field
     * 
     * @return the multimap of datatype to expansion field
     * @throws TableNotFoundException
     *             if no table exists
     */
    @Cacheable(value = "loadExpansionFields", key = "{#root.target.auths,#root.target.metadataTableName}", cacheManager = "metadataHelperCacheManager")
    public Multimap<String,String> loadExpansionFields() throws TableNotFoundException {
        log.debug("cache fault for loadExpansionFields({}, {})", this.auths, this.metadataTableName);
        
        if (log.isTraceEnabled()) {
            log.trace("loadExpansionFields from table: {}", metadataTableName);
        }
        
        Multimap<String,String> fields = HashMultimap.create();
        
        try (Scanner bs = ScannerHelper.createScanner(accumuloClient, metadataTableName, auths)) {
            bs.setRange(new Range());
            bs.fetchColumnFamily(ColumnFamilyConstants.COLF_EXP);
            
            for (Entry<Key,Value> entry : bs) {
                fields.put(getDatatype(entry.getKey()), entry.getKey().getRow().toString());
            }
        }
        return Multimaps.unmodifiableMultimap(fields);
    }
    
    /**
     * Fetch the set of all fields marked as being content fields, {@link ColumnFamilyConstants#COLF_CONTENT}. Returns a multimap of datatype to field
     * 
     * @return the multimap of datatype to content fields
     * @throws TableNotFoundException
     *             if no table exists
     */
    @Cacheable(value = "loadContentFields", key = "{#root.target.auths,#root.target.metadataTableName}", cacheManager = "metadataHelperCacheManager")
    public Multimap<String,String> loadContentFields() throws TableNotFoundException {
        log.debug("cache fault for loadContentFields({}, {})", this.auths, this.metadataTableName);
        
        if (log.isTraceEnabled()) {
            log.trace("loadContentFields from table: {}", metadataTableName);
        }
        
        Multimap<String,String> fields = HashMultimap.create();
        
        try (Scanner bs = ScannerHelper.createScanner(accumuloClient, metadataTableName, auths)) {
            bs.setRange(new Range());
            bs.fetchColumnFamily(ColumnFamilyConstants.COLF_CONTENT);
            
            for (Entry<Key,Value> entry : bs) {
                fields.put(getDatatype(entry.getKey()), entry.getKey().getRow().toString());
            }
        }
        return Multimaps.unmodifiableMultimap(fields);
    }
    
    /**
     * Fetch the Set of all datatypes that appear in the DatawaveMetadata table.
     * <p>
     * By scanning for all {@link ColumnFamilyConstants#COLF_E}, we will find all of the datatypes currently ingested by virtue that a datatype must have at
     * least one field that appears in an event.
     *
     * @return the set of all datatypes
     * @throws TableNotFoundException
     *             if no table exists
     */
    @Cacheable(value = "loadDatatypes", key = "{#root.target.auths,#root.target.metadataTableName}", cacheManager = "metadataHelperCacheManager")
    public Set<String> loadDatatypes() throws TableNotFoundException {
        log.debug("cache fault for loadDatatypes({}, {})", this.auths, this.metadataTableName);
        
        if (log.isTraceEnabled()) {
            log.trace("loadDatatypes from table: {}", metadataTableName);
        }
        
        HashSet<String> datatypes = new HashSet<>();
        final Text holder = new Text();
        
        try (Scanner bs = ScannerHelper.createScanner(accumuloClient, metadataTableName, auths)) {
            bs.setRange(new Range());
            bs.fetchColumnFamily(ColumnFamilyConstants.COLF_E);
            
            for (Entry<Key,Value> entry : bs) {
                entry.getKey().getColumnQualifier(holder);
                datatypes.add(holder.toString());
            }
        }
        return Collections.unmodifiableSet(datatypes);
    }
    
    /**
     * Fetches results from {@link #metadataTableName} and calculates the set of field index holes that exists for all indexed entries. The map consists of
     * field names to datatypes to field index holes.
     * 
     * @param fields
     *            the fields to fetch field index holes for, an empty set will result in all fields being fetched
     * @param datatypes
     *            the datatypes to fetch field index holes for, an empty set will result in all datatypes being fetched
     * @param minThreshold
     *            the minimum percentage threshold required for an index row to be considered NOT a hole on a particular date, this should be a value in the
     *            range 0.0 to 1.0
     * @return a map of field names and datatype pairs to field index holes
     */
    public Map<String,Map<String,IndexFieldHole>> getFieldIndexHoles(Set<String> fields, Set<String> datatypes, double minThreshold)
                    throws TableNotFoundException, IOException {
        return getFieldIndexHoles(ColumnFamilyConstants.COLF_I, fields, datatypes, minThreshold);
    }
    
    /**
     * Fetches results from {@link #metadataTableName} and calculates the set of field index holes that exists for all reversed indexed entries. The map
     * consists of field names to datatypes to field index holes.
     * 
     * @param fields
     *            the fields to fetch field index holes for, an empty set will result in all fields being fetched
     * @param datatypes
     *            the datatypes to fetch field index holes for, an empty set will result in all datatypes being fetched
     * @param minThreshold
     *            the minimum percentage threshold required for an index row to be considered NOT a hole on a particular date, this should be a value in the
     *            range 0.0 to 1.0
     * @return a map of field names and datatype pairs to field index holes
     */
    public Map<String,Map<String,IndexFieldHole>> getReversedFieldIndexHoles(Set<String> fields, Set<String> datatypes, double minThreshold)
                    throws TableNotFoundException, IOException {
        return getFieldIndexHoles(ColumnFamilyConstants.COLF_RI, fields, datatypes, minThreshold);
    }
    
    /**
     * Get the field index holes for the provided fields and datatypes
     *
     * @param targetColumnFamily
     *            the target column family
     * @param fields
     *            a set of fields for which to get holes (can be empty to denote all)
     * @param datatypes
     *            a set of datatypes (can be empty to denote all)
     * @param minThreshold
     *            the minimum threshold
     * @return a map of index holes by datatype
     * @throws TableNotFoundException
     *             if no table exists
     * @throws IOException
     *             if a value fails to deserialize
     */
    private Map<String,Map<String,IndexFieldHole>> getFieldIndexHoles(Text targetColumnFamily, Set<String> fields, Set<String> datatypes, double minThreshold)
                    throws TableNotFoundException, IOException {
        // create local copies to avoid side effects
        fields = new HashSet<>(fields);
        datatypes = new HashSet<>(datatypes);
        
        // Handle null fields if given.
        if (fields == null) {
            fields = Collections.emptySet();
        } else {
            // Ensure null is not present as an entry.
            fields.remove(null);
        }
        
        // Handle null datatypes if given.
        if (datatypes == null) {
            datatypes = Collections.emptySet();
        } else {
            // Ensure null is not present as an entry.
            datatypes.remove(null);
        }
        
        // remove fields that are not indexed at all by the specified datatypes
        Multimap<String,String> indexedFieldMap = (targetColumnFamily == ColumnFamilyConstants.COLF_I ? loadIndexedFields() : loadReverseIndexedFields());
        Set<String> indexedFields = new HashSet<>();
        if (datatypes.isEmpty()) {
            indexedFields.addAll(indexedFieldMap.values());
        } else {
            indexedFields = new HashSet<>();
            for (String datatype : datatypes) {
                indexedFields.addAll(indexedFieldMap.get(datatype));
            }
        }
        
        // if the initial fields list is empty, then we want all possible holes
        if (fields.isEmpty()) {
            fields = indexedFields;
        } else {
            fields.retainAll(indexedFields);
            
            // if we have removed all fields, then there are no fields for which we can generate holes
            if (fields.isEmpty()) {
                return new HashMap<>();
            }
        }
        
        // Ensure the minThreshold is a percentage in the range 0%-100%.
        if (minThreshold > 1.0d) {
            minThreshold = 1.0d;
        } else if (minThreshold < 0.0d) {
            minThreshold = 0.0d;
        }
        
        Map<String,Map<String,IndexFieldHole>> indexHoles;
        try (Scanner bs = ScannerHelper.createScanner(accumuloClient, metadataTableName, auths)) {
            
            // Fetch the frequency column and the specified index column.
            bs.fetchColumnFamily(ColumnFamilyConstants.COLF_F);
            bs.fetchColumnFamily(targetColumnFamily);
            
            // Determine which range to use.
            Range range;
            if (fields.isEmpty()) {
                // If no fields are specified, scan over all entries in the table.
                range = new Range();
            } else if (fields.size() == 1) {
                // If just one field is specified, limit the range to where the row is the field.
                range = new Range(new Text(fields.iterator().next()));
            } else {
                // If more than one field is specified, sort the fields and limit the range from the lowest to highest field (lexicographically).
                SortedSet<String> sortedFields = new TreeSet<>(fields);
                range = new Range(new Text(sortedFields.first()), new Text(sortedFields.last()));
            }
            bs.setRange(range);
            
            FieldIndexHoleFinder finder = new FieldIndexHoleFinder(bs, minThreshold, fields, datatypes);
            indexHoles = finder.findHoles();
        }
        return indexHoles;
    }
    
    public int getTypeCacheSize() {
        return typeCacheSize;
    }
    
    public void setTypeCacheSize(int typeCacheSize) {
        this.typeCacheSize = typeCacheSize;
    }
    
    public int getTypeCacheExpirationInMinutes() {
        return typeCacheExpirationInMinutes;
    }
    
    public void setTypeCacheExpirationInMinutes(int typeCacheExpirationInMinutes) {
        this.typeCacheExpirationInMinutes = typeCacheExpirationInMinutes;
    }
    
    private enum BOUNDARY_TYPE {
        NONE, // denoting a non-boundary marker
        IMPLIED, // denoting a boundary implied by the old style index entry (using timestamp for date)
        MANUAL_INDEXED, // denoting a boundary that was manually injected including the date in the key and a true value
        MANUAL_UNINDEXED // denoting a boundary that was manually injected including the date in the key and a false value
    }
    
    private static class FieldCount {
        private long count = 0;
        private Date date = null;
        private BOUNDARY_TYPE boundaryType = BOUNDARY_TYPE.NONE;
        
        public void increment(long value) {
            this.count += value;
        }
        
        public void setBoundaryValue(BOUNDARY_TYPE boundaryType) {
            this.boundaryType = boundaryType;
        }
        
        public boolean isBoundary() {
            return this.boundaryType != null && this.boundaryType != BOUNDARY_TYPE.NONE;
        }
        
        public boolean isIndexBoundary() {
            return this.boundaryType == BOUNDARY_TYPE.IMPLIED || this.boundaryType == BOUNDARY_TYPE.MANUAL_INDEXED;
        }
        
        public boolean isUnindexedBoundary() {
            return this.boundaryType == BOUNDARY_TYPE.MANUAL_UNINDEXED;
        }
        
        public BOUNDARY_TYPE getBoundaryType() {
            return this.boundaryType;
        }
        
        public void setDate(Date date) {
            this.date = date;
        }
        
        public Date getDate() {
            return date;
        }
        
        public long getCount() {
            return this.count;
        }
        
        @Override
        public String toString() {
            return ToStringBuilder.reflectionToString(this);
        }
    }
    
    /**
     * Utility class for finding field index holes.
     */
    private static class FieldIndexHoleFinder {
        
        private final Scanner scanner;
        private final double minThreshold;
        private final Set<String> fields;
        private final Set<String> datatypes;
        private final boolean filterFields;
        private final boolean filterDatatypes;
        
        // Contains datatypes to dates and counts for entries seen in "f" rows for the current field name.
        private final Map<String,SortedMap<Date,FieldCount>> frequencyMap = new HashMap<>();
        
        // Contains datatypes to dates and counts for entries seen in the target "i" or "ri" index rows for the current field name.
        private final Map<String,SortedMap<Date,FieldCount>> indexMap = new HashMap<>();
        
        // Points to the target map object that we add entries to. This changes when we see a different column family compared to the previous row when scanning
        // over entries. We must initially start adding entries to the frequency map.
        private Map<String,SortedMap<Date,FieldCount>> targetMap = frequencyMap;
        
        // Map of field names to maps of datatypes to date ranges encompassing field index holes.
        Map<String,Multimap<String,Pair<Date,Date>>> fieldIndexHoles = new HashMap<>();
        
        FieldIndexHoleFinder(Scanner scanner, double minThreshold, Set<String> fields, Set<String> datatypes) {
            this.scanner = scanner;
            this.minThreshold = minThreshold;
            this.fields = Collections.unmodifiableSet(fields);
            this.datatypes = Collections.unmodifiableSet(datatypes);
            // Actively filter out entries based on the field if we have more than one field specified. If we have an empty set, we are searching for field
            // index holes for all fields. If we have just one field, the range for the scanner will already be limited to just the field.
            this.filterFields = fields.size() > 1;
            // Actively filter out entries based on the datatypes if we have any datatypes specified. If we have an empty set, we are searching for field index
            // holes for all datatypes.
            this.filterDatatypes = !datatypes.isEmpty();
        }
        
        /**
         * Find and return all field index holes for the scanner in this {@link FieldIndexHoleFinder}.
         * 
         * @return the field index holes
         * @throws IOException
         *             if a value fails to deserialize
         */
        Map<String,Map<String,IndexFieldHole>> findHoles() throws IOException {
            String prevFieldName = null;
            Text prevColumnFamily = null;
            
            String currFieldName;
            String currDatatype;
            Text currColumnFamily;
            Date currDate;
            long currCount;
            BOUNDARY_TYPE currBoundaryType;
            
            for (Map.Entry<Key,Value> entry : scanner) {
                
                // Parse the current row.
                Key key = entry.getKey();
                currFieldName = key.getRow().toString();
                currColumnFamily = key.getColumnFamily();
                
                // avoid carrying over anything
                currDatatype = null;
                currDate = null;
                currBoundaryType = BOUNDARY_TYPE.NONE;
                
                String cq = key.getColumnQualifier().toString();
                int offset = cq.indexOf(NULL_BYTE);
                if (offset < 0) {
                    currDatatype = cq;
                    
                    // Check if the current field and datatype are part of the fields and datatypes we want to retrieve field index holes for.
                    if (!isPartOfTarget(currFieldName, currDatatype)) {
                        continue;
                    }
                    
                    // we can treat this like an index marker but the ts of the entry denotes the boundary
                    currDate = getPreviousDay(key.getTimestamp());
                    log.warn("Found an index entry missing the date, treating as an index marker at " + currDate + " : " + key);
                    currBoundaryType = BOUNDARY_TYPE.IMPLIED;
                    currCount = 0;
                } else {
                    currDatatype = cq.substring(0, offset);
                    
                    // Check if the current field and datatype are part of the fields and datatypes we want to retrieve field index holes for.
                    if (!isPartOfTarget(currFieldName, currDatatype)) {
                        continue;
                    }
                    
                    String cqRemainder = cq.substring((offset + 1));
                    // check for a marker of <dt>\0<date>\0true/false vs just <dt>\0<date>
                    // where the boolean denotes that we can assume the field is indexed/no on and before this date
                    offset = cqRemainder.indexOf(NULL_BYTE);
                    if (offset >= 0) {
                        currBoundaryType = Boolean.valueOf(cqRemainder.substring(offset + 1)) ? BOUNDARY_TYPE.MANUAL_INDEXED : BOUNDARY_TYPE.MANUAL_UNINDEXED;
                        currDate = DateHelper.parse(cqRemainder.substring(0, offset));
                        currCount = 0;
                    } else {
                        currBoundaryType = null;
                        try {
                            currDate = DateHelper.parse(cqRemainder);
                            ByteArrayInputStream byteStream = new ByteArrayInputStream(entry.getValue().get());
                            DataInputStream inputStream = new DataInputStream(byteStream);
                            currCount = WritableUtils.readVLong(inputStream);
                        } catch (DateTimeParseException e) {
                            // probably the really old type classname format instead of a date.
                            // we can treat this like an index marker but the ts of the entry denotes the boundary
                            currDate = getPreviousDay(key.getTimestamp());
                            log.warn("Found an index entry missing the date, treating as an index marker at " + currDate + " : " + key);
                            currBoundaryType = BOUNDARY_TYPE.IMPLIED;
                            currCount = 0;
                        }
                    }
                }
                
                // If this is the very first entry we've looked at, update our tracking variables
                if (prevFieldName == null) {
                    prevFieldName = currFieldName;
                    // assume we are starting with the COLF_F entries
                    prevColumnFamily = ColumnFamilyConstants.COLF_F;
                    // Set the target map to the frequency map.
                    this.targetMap = frequencyMap;
                }
                
                // The column family is different. We have two possible scenarios:
                // - The previous column family was 'f'. The current row is an index row for to the current field.
                // - The previous column family was the target index column family. The current row is an 'f' row for a new field.
                //
                // In both cases, record the last entry, and begin collecting date ranges for the next batch of related rows.
                if (!prevColumnFamily.equals(currColumnFamily)) {
                    // The column family is "f". We have collected the date ranges for all datatypes for the previous field name. Get the field index holes for
                    // the
                    // previously collected data.
                    if (currColumnFamily.equals(ColumnFamilyConstants.COLF_F)) {
                        // Find and add all field index holes for the current frequency and index entries.
                        findFieldIndexHoles(prevFieldName);
                        // Clear the entry maps.
                        clearEntryMaps();
                        // Set the target map to the frequency map.
                        this.targetMap = frequencyMap;
                    } else {
                        // The current column family is the target index column family. Set the target map to the index map.
                        this.targetMap = indexMap;
                    }
                    
                    // Add the current entry to the target entry map.
                    addToTargetMap(currDatatype, currDate, currCount, currBoundaryType);
                } else {
                    // The column family is the same. We have two possible scenarios:
                    // - A row with a field that is different to the previous field.
                    // - A row with the same field.
                    
                    // We have encountered a new field name and the previous fieldName-datatype combination did not have any corresponding index row entries.
                    if (!currFieldName.equals(prevFieldName)) {
                        // Find and add all field index holes for the current frequency and index entries.
                        findFieldIndexHoles(prevFieldName);
                        // Clear the entry maps.
                        clearEntryMaps();
                        // Add the current entry to the target entry map.
                        addToTargetMap(currDatatype, currDate, currCount, currBoundaryType);
                    } else {
                        // The current row has the same field. Add the current entry to the target map.
                        addToTargetMap(currDatatype, currDate, currCount, currBoundaryType);
                    }
                }
                
                // Set the values for our prev entry to the current entry.
                prevFieldName = currFieldName;
                prevColumnFamily = currColumnFamily;
            }
            
            // After there are no more rows, ensure that we find any field index holes that exist in the last batch of entries.
            findFieldIndexHoles(prevFieldName);
            
            // Return the field index holes as an immutable structure.
            return getImmutableFieldIndexHoles();
        }
        
        private Date getPreviousDay(long ts) {
            Calendar c = Calendar.getInstance();
            c.setTimeInMillis(ts);
            c.set(Calendar.HOUR_OF_DAY, 0);
            c.set(Calendar.SECOND, 0);
            c.set(Calendar.MINUTE, 0);
            c.set(Calendar.MILLISECOND, 0);
            c.add(Calendar.DATE, -1);
            return c.getTime();
        }
        
        private Date getNextDay(long ts) {
            Calendar c = Calendar.getInstance();
            c.setTimeInMillis(ts);
            c.set(Calendar.HOUR_OF_DAY, 0);
            c.set(Calendar.SECOND, 0);
            c.set(Calendar.MINUTE, 0);
            c.set(Calendar.MILLISECOND, 0);
            c.add(Calendar.DATE, +1);
            return c.getTime();
        }
        
        /**
         * Return whether the given field and datatype represent a pairing that should be evaluated for field index holes.
         */
        private boolean isPartOfTarget(String field, String datatype) {
            return (!filterFields || fields.contains(field)) && (!filterDatatypes || datatypes.contains(datatype));
        }
        
        /**
         * Add the current date and count to the current target map for the current datatype.
         */
        private void addToTargetMap(String datatype, Date date, long count, BOUNDARY_TYPE boundaryType) {
            FieldCount fieldCount = getFieldCount(targetMap, datatype, date);
            fieldCount.increment(count);
            if (boundaryType != null) {
                fieldCount.setBoundaryValue(boundaryType);
                
                // If this is an implied boundary type, then the date of field count is actually the following day
                // (see calls to getPreviousDay above)
                if (boundaryType == BOUNDARY_TYPE.IMPLIED) {
                    fieldCount.setDate(getNextDay(date.getTime()));
                }
            }
            
            // we need to ensure we have a frequency entry if a boundary so that we will catch this when finding holes
            getFieldCount(frequencyMap, datatype, date);
        }
        
        /**
         * Return the field count entry from the specified map. A new entry is added to the map if missing
         * 
         * @param datatype
         * @param date
         * @return The field count. Never null
         */
        private FieldCount getFieldCount(Map<String,SortedMap<Date,FieldCount>> map, String datatype, Date date) {
            SortedMap<Date,FieldCount> datesToCounts = map.computeIfAbsent(datatype, (k) -> new TreeMap<>());
            FieldCount fieldCount = datesToCounts.get(date);
            if (fieldCount == null) {
                fieldCount = new FieldCount();
                fieldCount.setDate(date);
                datesToCounts.put(date, fieldCount);
            }
            return fieldCount;
        }
        
        /**
         * Clear the maps {@link #frequencyMap} and {@link #indexMap}.
         */
        private void clearEntryMaps() {
            this.frequencyMap.clear();
            this.indexMap.clear();
        }
        
        /**
         * Find all field index holes for given field name, and store them in {@link #fieldIndexHoles}.
         * 
         * @param fieldName
         *            the field name
         */
        private void findFieldIndexHoles(String fieldName) {
            Multimap<String,Pair<Date,Date>> indexHoles = fieldIndexHoles.computeIfAbsent(fieldName, k -> HashMultimap.create());
            // Compare the entries for each datatype to identify any and all field index holes.
            for (String datatype : frequencyMap.keySet()) {
                // At least one corresponding index row was seen. Compare the entries to identify any index holes.
                if (indexMap.containsKey(datatype)) {
                    // Add all index holes found for the entries for the current datatype.
                    Set<Pair<Date,Date>> holes = getIndexHoles(frequencyMap.get(datatype), indexMap.get(datatype));
                    indexHoles.putAll(datatype, holes);
                } else {
                    // No corresponding index rows were seen for any of the frequency rows. Each date is an index hole. Add a date range of the earliest date to
                    // the latest date.
                    SortedMap<Date,FieldCount> entryMap = frequencyMap.get(datatype);
                    indexHoles.put(datatype, Pair.of(entryMap.firstKey(), entryMap.lastKey()));
                }
            }
        }
        
        /**
         * An expression to determine if we can assume a field is index on a particular date given the previous boundary and the date in question. This is true
         * if the previous boundary was an implied boundary (previous index entry type, based on timestamp of key) and the date in question is the date that the
         * implied boundary was found. In this case the boundary is actually assigned the previous date in the map but the date in the FieldCount is the actual
         * date the marker was found on based on the key timestamp.
         * 
         * @param date
         *            the date in question
         * @param prevBoundary
         *            the previous boundary if any
         * @return true if we can assume indexed
         */
        private boolean canAssumeIndexed(Date date, FieldCount prevBoundary) {
            if (prevBoundary != null && prevBoundary.getBoundaryType() == BOUNDARY_TYPE.IMPLIED) {
                return date.equals(prevBoundary.getDate());
            }
            return false;
        }
        
        /**
         * Return a set of all index hole date ranges found for the given maps of frequency and index entries.
         * 
         * @param frequencyMap
         *            the frequency entries
         * @param indexMap
         *            the index entries
         * @return a set of index holes, possibly empty, but never null
         */
        private Set<Pair<Date,Date>> getIndexHoles(SortedMap<Date,FieldCount> frequencyMap, SortedMap<Date,FieldCount> indexMap) {
            Set<Pair<Date,Date>> indexHoles = new HashSet<>();
            Date holeStartDate = null;
            Date prevDate = null;
            FieldCount prevBoundary = null;
            
            for (Date date : frequencyMap.keySet()) {
                // see if we can assume this date is indexed
                boolean canAssumeIndexed = canAssumeIndexed(date, prevBoundary);
                prevBoundary = null;
                
                // There is a corresponding index entry for the current date.
                if (indexMap.containsKey(date) || canAssumeIndexed) {
                    FieldCount indexCount = indexMap.get(date);
                    
                    // if this is a boundary marker, then replace/clear map thus far
                    if (indexCount != null && indexCount.isBoundary()) {
                        // all holes thus far are to be replaced
                        indexHoles.clear();
                        // if not indexed, then start a hole since the beginning
                        if (indexCount.isUnindexedBoundary()) {
                            holeStartDate = frequencyMap.firstKey();
                        } else {
                            // else indexed since the beginning
                            holeStartDate = null;
                            // keep track of the previous indexed boundary
                            prevBoundary = indexCount;
                        }
                    } else if (canAssumeIndexed || meetsMinThreshold(frequencyMap.get(date), indexCount)) {
                        // The count for the current index entry meets the minimum threshold.
                        // The previous entry was part of an index hole. Capture the index hole range.
                        if (holeStartDate != null) {
                            indexHoles.add(Pair.of(holeStartDate, prevDate));
                            holeStartDate = null;
                        }
                    } else {
                        // The count for the current index entry does not meet the minimum threshold, and thus this entry is part of an index hole. Mark the
                        // start of an index hole date range if we have not already found one.
                        if (holeStartDate == null) {
                            holeStartDate = date;
                        }
                    }
                } else {
                    // There is no corresponding index entry for the current date. This is the start of an index hole if we have not previously found one.
                    if (holeStartDate == null) {
                        holeStartDate = date;
                    }
                }
                
                // Track the previous date
                prevDate = date;
            }
            
            // If we have finished looking at all dates, and we have a trailing index hole, capture the last index hole date range.
            if (holeStartDate != null) {
                indexHoles.add(Pair.of(holeStartDate, prevDate));
            }
            
            return indexHoles;
        }
        
        /**
         * Return whether the given index count meets the minimum threshold for the given frequency count.
         * 
         * @param frequencyCount
         *            the frequency count
         * @param indexCount
         *            the index count
         * @return true if the threshold is met, or false otherwise
         */
        private boolean meetsMinThreshold(FieldCount frequencyCount, FieldCount indexCount) {
            if (indexCount.getCount() >= frequencyCount.getCount()) {
                return true;
            }
            
            double percentage = (double) (indexCount.getCount()) / frequencyCount.getCount();
            return percentage >= minThreshold;
        }
        
        /**
         * Return an immutable version of {@link #fieldIndexHoles}, with all empty collections removed.
         * 
         * @return an immutable map.
         */
        private Map<String,Map<String,IndexFieldHole>> getImmutableFieldIndexHoles() {
            ImmutableMap.Builder<String,Map<String,IndexFieldHole>> fieldMapBuilder = new ImmutableMap.Builder<>();
            
            for (String fieldName : this.fieldIndexHoles.keySet()) {
                Multimap<String,Pair<Date,Date>> datatypeMap = this.fieldIndexHoles.get(fieldName);
                if (!datatypeMap.isEmpty()) {
                    ImmutableMap.Builder<String,IndexFieldHole> datatypeMapBuilder = new ImmutableMap.Builder<>();
                    for (String datatype : datatypeMap.keySet()) {
                        IndexFieldHole fieldIndexHole = new IndexFieldHole(fieldName, datatype, datatypeMap.get(datatype));
                        datatypeMapBuilder.put(datatype, fieldIndexHole);
                    }
                    fieldMapBuilder.put(fieldName, datatypeMapBuilder.build());
                }
            }
            
            return fieldMapBuilder.build();
        }
    }
    
    /**
     * Get a key composed of the accumulo instance ID and the metadata table name
     * 
     * @param instanceID
     *            the accumulo instance id
     * @param metadataTableName
     *            the metadata table name
     * @return a key
     */
    private static String getKey(String instanceID, String metadataTableName) {
        StringBuilder builder = new StringBuilder();
        builder.append(instanceID).append('\0');
        builder.append(metadataTableName).append('\0');
        return builder.toString();
    }
    
    /**
     * Get a key
     * 
     * @param helper
     *            an instance of an {@link AllFieldMetadataHelper}
     * @return a key
     */
    private static String getKey(AllFieldMetadataHelper helper) {
        return getKey(helper.accumuloClient.instanceOperations().getInstanceId().canonical(), helper.metadataTableName);
    }
    
    /**
     * ToString
     * 
     * @return a string
     */
    @Override
    public String toString() {
        return getKey(this);
    }
    
    /**
     * Simple 'get or create' method for the TypeFactory
     *
     * @return a TypeFactory.
     */
    protected TypeFactory getTypeFactory() {
        if (typeFactory == null) {
            
            // check for configured size and TTL
            if (typeCacheSize != -1 && typeCacheExpirationInMinutes != -1) {
                typeFactory = new TypeFactory(typeCacheSize, typeCacheExpirationInMinutes);
            } else {
                typeFactory = new TypeFactory();
            }
        }
        return typeFactory;
    }
}
