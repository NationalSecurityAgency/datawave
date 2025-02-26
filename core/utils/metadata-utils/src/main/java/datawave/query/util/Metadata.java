package datawave.query.util;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.iterators.OptionDescriber;

import com.google.common.collect.Sets;

import datawave.util.StringUtils;

public class Metadata implements Serializable, OptionDescriber {
    
    private static final long serialVersionUID = -3666313025240388292L;
    Set<String> datatypes = new HashSet<>(0);
    Set<String> termFrequencyFields = new HashSet<>(0);
    Set<String> allFields = new HashSet<>(0);
    Set<String> indexedFields = new HashSet<>(0);
    Set<String> indexOnlyFields = new HashSet<>(0);
    Set<String> normalizedFields = new HashSet<>(0);
    
    public static final String DATA_TYPES = "metadata.datatypes";
    public static final String TERM_FREQ_FIELDS = "metadata.termfrequency.fields";
    public static final String ALL_FIELDS = "metadata.all.fields";
    public static final String INDEXED_FIELDS = "metadata.indexed.fields";
    public static final String INDEX_ONLY_FIELDS = "metadata.indexonly.fields";
    public static final String NORMALIZED_FIELDS = "metadata.normalized.fields";
    public static final char PARAM_VALUE_SEP = ',';
    public static final String PARAM_VALUE_SEP_STR = new String(new char[] {PARAM_VALUE_SEP});
    
    public Metadata() {}
    
    /**
     * @param other
     */
    public Metadata(Metadata other) {
        this(Sets.newHashSet(other.datatypes), Sets.newHashSet(other.termFrequencyFields), Sets.newHashSet(other.allFields),
                        Sets.newHashSet(other.indexedFields), Sets.newHashSet(other.indexOnlyFields));
    }
    
    /**
     * @param helper
     * @throws ExecutionException
     * @throws TableNotFoundException
     */
    public Metadata(MetadataHelper helper, Set<String> datatypeFilter) throws ExecutionException, TableNotFoundException {
        this(Sets.newHashSet(helper.getDatatypes(datatypeFilter)), Sets.newHashSet(helper.getTermFrequencyFields(datatypeFilter)),
                        Sets.newHashSet(helper.getAllFields(datatypeFilter)), Sets.newHashSet(helper.getIndexedFields(datatypeFilter)),
                        Sets.newHashSet(helper.getIndexOnlyFields(datatypeFilter)));
    }
    
    protected Metadata(Set<String> datatypes, Set<String> termFrequencyFields, Set<String> allFields, Set<String> indexedFields, Set<String> indexOnlyFields) {
        this.datatypes = datatypes;
        this.termFrequencyFields = termFrequencyFields;
        this.allFields = allFields;
        this.indexedFields = indexedFields;
        this.indexOnlyFields = indexOnlyFields;
    }
    
    /**
     * Get the {@link Set} of all fields contained in the database.
     */
    public Set<String> getAllFields() {
        return Collections.unmodifiableSet(this.allFields);
    }
    
    /**
     * Get the {@link Set} of all indexed fields contained in the database.
     * 
     * @return
     */
    public Set<String> getIndexedFields() {
        return Collections.unmodifiableSet(this.indexedFields);
    }
    
    public Set<String> getNormalizedFields() {
        return Collections.unmodifiableSet(this.normalizedFields);
    }
    
    /**
     * Get the {@link Set} of index-only fields.
     */
    public Set<String> getIndexOnlyFields() {
        return Collections.unmodifiableSet(this.indexOnlyFields);
    }
    
    /**
     * Get the Set of all fields marked as containing term frequency information, {@link datawave.data.ColumnFamilyConstants#COLF_TF}.
     */
    public Set<String> getTermFrequencyFields() {
        return Collections.unmodifiableSet(this.termFrequencyFields);
    }
    
    /**
     * Get the data types
     */
    public Set<String> getDatatypes() {
        return Collections.unmodifiableSet(this.datatypes);
    }
    
    @Override
    public IteratorOptions describeOptions() {
        Map<String,String> options = new HashMap<>();
        options.put(DATA_TYPES, "list of data types separated by '" + PARAM_VALUE_SEP + "'");
        options.put(TERM_FREQ_FIELDS, "list of term frequency fields separated by '" + PARAM_VALUE_SEP + "'");
        options.put(INDEXED_FIELDS, "list of indexed fields separated by '" + PARAM_VALUE_SEP + "'");
        options.put(INDEX_ONLY_FIELDS, "list of index only (unevaluated) fields separated by '" + PARAM_VALUE_SEP + "'");
        options.put(ALL_FIELDS, "list of all fields separated by '" + PARAM_VALUE_SEP + "'");
        options.put(NORMALIZED_FIELDS, "list of fields that are normalized separated by '" + PARAM_VALUE_SEP + "'");
        return new IteratorOptions(getClass().getSimpleName(), "contains the information derived from the metadata table", options, null);
    }
    
    @Override
    public boolean validateOptions(Map<String,String> options) {
        // all options are optional
        return true;
    }
    
    public void init(Map<String,String> options) {
        this.datatypes = parse(options.get(DATA_TYPES));
        this.termFrequencyFields = parse(options.get(TERM_FREQ_FIELDS));
        this.indexedFields = parse(options.get(INDEXED_FIELDS));
        this.indexOnlyFields = parse(options.get(INDEX_ONLY_FIELDS));
        this.allFields = parse(options.get(ALL_FIELDS));
        this.normalizedFields = parse(options.get(NORMALIZED_FIELDS));
    }
    
    public Map<String,String> getOptions() {
        Map<String,String> options = new HashMap<>();
        if (!this.datatypes.isEmpty()) {
            options.put(DATA_TYPES, toString(this.datatypes));
        }
        if (!this.termFrequencyFields.isEmpty()) {
            options.put(TERM_FREQ_FIELDS, toString(this.termFrequencyFields));
        }
        if (!this.indexOnlyFields.isEmpty()) {
            options.put(INDEX_ONLY_FIELDS, toString(this.indexOnlyFields));
        }
        if (!this.allFields.isEmpty()) {
            options.put(ALL_FIELDS, toString(this.allFields));
        }
        if (!this.indexedFields.isEmpty()) {
            options.put(INDEXED_FIELDS, toString(this.indexedFields));
        }
        if (!this.normalizedFields.isEmpty()) {
            options.put(NORMALIZED_FIELDS, toString(this.normalizedFields));
        }
        return options;
    }
    
    private static String toString(Collection<String> values) {
        StringBuilder buffer = new StringBuilder();
        if (values != null) {
            String separator = "";
            for (String value : values) {
                buffer.append(separator).append(value);
                separator = PARAM_VALUE_SEP_STR;
            }
        }
        return buffer.toString();
    }
    
    private static Set<String> parse(String value) {
        if (value == null) {
            return new HashSet<>(0);
        } else {
            return new HashSet<>(Arrays.asList(StringUtils.split(value, PARAM_VALUE_SEP)));
        }
    }
    
}
