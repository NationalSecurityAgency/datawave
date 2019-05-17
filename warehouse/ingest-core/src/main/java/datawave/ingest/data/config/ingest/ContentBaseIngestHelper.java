package datawave.ingest.data.config.ingest;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import datawave.ingest.data.config.FieldConfigHelper;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Class for pulling content indexing specific fields from the XML config files and validating that all required parameters are set.
 * 
 * 
 * 
 */
public abstract class ContentBaseIngestHelper extends AbstractContentIngestHelper {
    
    private static final Logger log = Logger.getLogger(ContentBaseIngestHelper.class);
    
    private final Set<String> contentIndexWhitelist = new HashSet<>();
    private final Set<String> contentReverseIndexWhitelist = new HashSet<>();
    
    private final Set<String> contentIndexBlacklist = new HashSet<>();
    private final Set<String> contentReverseIndexBlacklist = new HashSet<>();
    
    private final Set<String> indexListEtriesFields = new HashSet<>();
    private final Set<String> reverseIndexListEtriesFields = new HashSet<>();
    
    /**
     * If we are using a tokenization blacklist but want to include-only certain fields from specific subtypes we can declare that here. For example say we only
     * want one field tokenized from a subtype. We'd need to enumerate all of the unwanted fields in the blacklist, and that could be a lot of fields to
     * configure. Instead we can do a whitelist on a per subtype basis.
     *
     * ONLY USE IN CONJUNCTION WITH A TOKENIZATION BLACKLIST
     *
     * If the TYPE=; is empty, then no fields will be tokenized
     */
    private Map<String,Set<String>> subtypeFieldTokenizationWhitelistMap = new HashMap<>();
    public static final String SUBTYPE_TOKENIZATION_WHITELIST_MAP = ".data.category.index.tokenize.whitelist.subtype.map";
    
    /**
     * Fields that we want to perform content indexing and reverse indexing (i.e. token and then index the tokens)
     */
    public static final String TOKEN_INDEX_WHITELIST = ".data.category.index.tokenize.whitelist";
    public static final String TOKEN_REV_INDEX_WHITELIST = ".data.category.index.reverse.tokenize.whitelist";
    
    public static final String TOKEN_INDEX_BLACKLIST = ".data.category.index.tokenize.blacklist";
    public static final String TOKEN_REV_INDEX_BLACKLIST = ".data.category.index.reverse.tokenize.blacklist";
    
    public static final String TOKEN_FIELDNAME_DESIGNATOR = ".data.category.token.fieldname.designator";
    public static final String TOKEN_FIELDNAME_DESIGNATOR_ENABLED = ".data.category.token.fieldname.designator.enabled";
    
    public static final String INDEX_LIST_FIELDS = ".data.category.index.list.fields";
    public static final String REV_INDEX_LIST_FIELDS = ".data.category.index.reverse.list.fields";
    public static final String LIST_FIELDNAME_DESIGNATOR = ".data.category.list.fieldname.designator";
    public static final String LIST_DELIMITERS = ".data.category.list.delimiters";
    
    /**
     * option to save raw data in the document column family.
     */
    public static final String SAVE_RAW_DATA_AS_DOCUMENT = ".data.saveRawDataOption";
    public static final String RAW_DOCUMENT_VIEW_NAME = ".data.rawDocumentViewName";
    
    private String tokenFieldNameDesignator = "_TOKEN";
    
    private String listFieldNameDesignator = "_ENTRY";
    private String listDelimiter = ",";
    
    private boolean saveRawDataOption = false;
    private String rawDocumentViewName = "RAW"; // default value
    
    private static Collection<String> trimConfigStrings(Configuration config, String configName) {
        String c = config.get(configName);
        Collection<String> s = new HashSet<>();
        
        if (c != null) {
            String[] names = datawave.util.StringUtils.split(c, ',');
            for (String n : names) {
                n = n.trim();
                if (n.equals(""))
                    continue;
                
                s.add(n);
            }
        }
        
        return s;
    }
    
    @Override
    public void setup(Configuration config) throws IllegalArgumentException {
        super.setup(config);
        
        boolean tokenFieldNameDesignatorEnabled = config.getBoolean(getType().typeName() + TOKEN_FIELDNAME_DESIGNATOR_ENABLED, true);
        
        if (tokenFieldNameDesignatorEnabled) {
            tokenFieldNameDesignator = config.get(getType().typeName() + TOKEN_FIELDNAME_DESIGNATOR, tokenFieldNameDesignator);
        } else {
            tokenFieldNameDesignator = "";
        }
        
        contentIndexWhitelist.addAll(trimConfigStrings(config, getType().typeName() + TOKEN_INDEX_WHITELIST));
        contentReverseIndexWhitelist.addAll(trimConfigStrings(config, getType().typeName() + TOKEN_REV_INDEX_WHITELIST));
        contentIndexBlacklist.addAll(trimConfigStrings(config, getType().typeName() + TOKEN_INDEX_BLACKLIST));
        contentReverseIndexBlacklist.addAll(trimConfigStrings(config, getType().typeName() + TOKEN_REV_INDEX_BLACKLIST));
        
        listFieldNameDesignator = config.get(getType().typeName() + LIST_FIELDNAME_DESIGNATOR, listFieldNameDesignator);
        listDelimiter = config.get(getType().typeName() + LIST_DELIMITERS, listDelimiter);
        indexListEtriesFields.addAll(trimConfigStrings(config, getType().typeName() + INDEX_LIST_FIELDS));
        reverseIndexListEtriesFields.addAll(trimConfigStrings(config, getType().typeName() + REV_INDEX_LIST_FIELDS));
        
        String subtypeMapArg = config.get(getType().typeName() + SUBTYPE_TOKENIZATION_WHITELIST_MAP);
        if (StringUtils.isNotEmpty(subtypeMapArg)) {
            this.subtypeFieldTokenizationWhitelistMap = buildSubtypeTokenizationWhitelist(parseMultiLineConfigValue(subtypeMapArg, Pattern.compile(";")));
        }
        
        this.saveRawDataOption = (null != config.get(getType().typeName() + SAVE_RAW_DATA_AS_DOCUMENT)) ? Boolean.parseBoolean(config.get(getType().typeName()
                        + SAVE_RAW_DATA_AS_DOCUMENT)) : saveRawDataOption;
        
        // If we're saving the raw data in the document column, we need a view name.
        // retrieve this view name if it has been specified, else use default if we
        // are saving the raw record in the doc column.
        if (this.saveRawDataOption) {
            this.rawDocumentViewName = (null != config.get(getType().typeName() + RAW_DOCUMENT_VIEW_NAME)) ? config.get(getType().typeName()
                            + RAW_DOCUMENT_VIEW_NAME) : rawDocumentViewName;
            if (log.isTraceEnabled()) {
                log.trace("saveRawDataOption was true");
                log.trace("getType().typeName()+RAW_DOCUMENT_VIEW_NAME: " + getType().typeName() + RAW_DOCUMENT_VIEW_NAME);
                log.trace("config.get(getType().typeName()+RAW_DOCUMENT_VIEW_NAME): " + config.get(getType().typeName() + RAW_DOCUMENT_VIEW_NAME));
            }
        }
    }
    
    @Override
    public boolean isContentIndexField(String field) {
        if (fieldHelper != null) {
            // tokenization == content indexing (yeah, poor choice of terms)
            return fieldHelper.isTokenizedField(field);
        }
        
        return contentIndexBlacklist.isEmpty() ? contentIndexWhitelist.contains(field) : !contentIndexBlacklist.contains(field);
    }
    
    @Override
    public boolean isReverseContentIndexField(String field) {
        if (fieldHelper != null) {
            // tokenization == content indexing (yeah, poor choice of terms)
            return fieldHelper.isReverseTokenizedField(field);
        }
        
        return contentReverseIndexBlacklist.isEmpty() ? contentReverseIndexWhitelist.contains(field) : !contentReverseIndexBlacklist.contains(field);
    }
    
    @Override
    public boolean isIndexListField(String field) {
        return indexListEtriesFields.contains(field);
    }
    
    @Override
    public boolean isReverseIndexListField(String field) {
        return reverseIndexListEtriesFields.contains(field);
    }
    
    @VisibleForTesting
    public FieldConfigHelper getFieldConfigHelper() {
        return fieldHelper;
    }
    
    @Override
    public String getTokenFieldNameDesignator() {
        return tokenFieldNameDesignator;
    }
    
    @Override
    public String getListFieldNameDesignator() {
        return listFieldNameDesignator;
    }
    
    @Override
    public String getListDelimiter() {
        return listDelimiter;
    }
    
    /**
     * Option to save raw data in the document column family.
     * 
     * @return
     */
    @Override
    public boolean getSaveRawDataOption() {
        return this.saveRawDataOption;
    }
    
    @Override
    public String getRawDocumentViewName() {
        return rawDocumentViewName;
    }
    
    public static Set<String> parseMultiLineConfigValue(String s, Pattern splitter) {
        return cleanSet(Sets.newHashSet(splitter.split(s)));
    }
    
    /**
     * Utility method to take a {@code String[]} and return a {@code Set<String>} where the values are trimmed
     * 
     * @param items
     *            String[] of items to trim and turn into a set
     * @return unique Set of strings where whitespace has been trimmed.
     */
    public static Set<String> cleanSet(Collection<String> items) {
        Set<String> itemSet = new HashSet<>();
        for (String item : items) {
            item = item.trim();
            if (!item.isEmpty()) {
                itemSet.add(item);
            }
        }
        return itemSet;
    }
    
    private static final String EQUALS = "=";
    private static final String COMMA = ",";
    
    /**
     *
     * @param items
     *            Set of strings where each item is of the {@code pattern-> SUBTYPE=field1,field2,...,fieldN}
     * @return {@code Map<String,Set<String>} which is a map of SUBTYPE to a set of fields to tokenize
     */
    public static Map<String,Set<String>> buildSubtypeTokenizationWhitelist(Set<String> items) {
        Map<String,Set<String>> subtypeTokenizationMap = Maps.newHashMap();
        for (String item : items) {
            
            // each item is SUBTYPE=field1,field2,...,fieldN
            // split it up into SUBTYPE and fields
            String[] parts = item.split(EQUALS);
            String subtype = parts[0].trim().toLowerCase();
            Set<String> fieldSet = Sets.newHashSet(); // SUBTYPE= We interpret as an empty whitelist
            
            // split the fields on comma and update the field set
            if (parts.length == 2) {
                String[] fields = parts[1].split(COMMA);
                for (String field : fields) {
                    fieldSet.add(field.trim().toUpperCase());
                }
            }
            
            // now push these into the subtype map
            if (subtypeTokenizationMap.containsKey(subtype)) {
                subtypeTokenizationMap.get(subtype).addAll(fieldSet);
            } else {
                subtypeTokenizationMap.put(subtype, fieldSet);
            }
        }
        return subtypeTokenizationMap;
    }
    
    public Map<String,Set<String>> getSubtypeFieldTokenizationWhitelistMap() {
        return subtypeFieldTokenizationWhitelistMap;
    }
}
