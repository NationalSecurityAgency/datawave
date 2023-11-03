package datawave.ingest.data.config.ingest;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;

import datawave.ingest.data.config.FieldConfigHelper;

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

    private final Set<String> indexListEntriesFields = new HashSet<>();
    private final Set<String> reverseIndexListEntriesFields = new HashSet<>();

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
    public static final String LIST_DELIMITERS = ".data.category.list.delimiters";

    /**
     * option to save raw data in the document column family.
     */
    public static final String SAVE_RAW_DATA_AS_DOCUMENT = ".data.saveRawDataOption";
    public static final String RAW_DOCUMENT_VIEW_NAME = ".data.rawDocumentViewName";

    private String tokenFieldNameDesignator = "_TOKEN";

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
                if (n.isEmpty())
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

        listDelimiter = config.get(getType().typeName() + LIST_DELIMITERS, listDelimiter);
        indexListEntriesFields.addAll(trimConfigStrings(config, getType().typeName() + INDEX_LIST_FIELDS));
        reverseIndexListEntriesFields.addAll(trimConfigStrings(config, getType().typeName() + REV_INDEX_LIST_FIELDS));

        this.saveRawDataOption = (null != config.get(getType().typeName() + SAVE_RAW_DATA_AS_DOCUMENT))
                        ? Boolean.parseBoolean(config.get(getType().typeName() + SAVE_RAW_DATA_AS_DOCUMENT))
                        : saveRawDataOption;
        // If we're saving the raw data in the document column, we need a view name.
        // retrieve this view name if it has been specified, else use default if we
        // are saving the raw record in the doc column.
        if (this.saveRawDataOption) {
            this.rawDocumentViewName = (null != config.get(getType().typeName() + RAW_DOCUMENT_VIEW_NAME))
                            ? config.get(getType().typeName() + RAW_DOCUMENT_VIEW_NAME)
                            : rawDocumentViewName;
            if (log.isTraceEnabled()) {
                log.trace("saveRawDataOption was true");
                log.trace("getType().typeName()+RAW_DOCUMENT_VIEW_NAME: " + getType().typeName() + RAW_DOCUMENT_VIEW_NAME);
                log.trace("config.get(getType().typeName()+RAW_DOCUMENT_VIEW_NAME): " + config.get(getType().typeName() + RAW_DOCUMENT_VIEW_NAME));
            }
        }
    }

    @Override
    public boolean isContentIndexField(String field) {
        if (fieldConfigHelper != null) {
            // tokenization == content indexing (yeah, poor choice of terms)
            return fieldConfigHelper.isTokenizedField(field);
        }

        return contentIndexBlacklist.isEmpty() ? contentIndexWhitelist.contains(field) : !contentIndexBlacklist.contains(field);
    }

    @Override
    public boolean isReverseContentIndexField(String field) {
        if (fieldConfigHelper != null) {
            // tokenization == content indexing (yeah, poor choice of terms)
            return fieldConfigHelper.isReverseTokenizedField(field);
        }

        return contentReverseIndexBlacklist.isEmpty() ? contentReverseIndexWhitelist.contains(field) : !contentReverseIndexBlacklist.contains(field);
    }

    @Override
    public boolean isIndexListField(String field) {
        return indexListEntriesFields.contains(field);
    }

    @Override
    public boolean isReverseIndexListField(String field) {
        return reverseIndexListEntriesFields.contains(field);
    }

    @VisibleForTesting
    public FieldConfigHelper getFieldConfigHelper() {
        return fieldConfigHelper;
    }

    @Override
    public String getTokenFieldNameDesignator() {
        return tokenFieldNameDesignator;
    }

    @Override
    public String getListDelimiter() {
        return listDelimiter;
    }

    /**
     * Option to save raw data in the document column family.
     *
     * @return flag of save raw data option
     */
    @Override
    public boolean getSaveRawDataOption() {
        return this.saveRawDataOption;
    }

    @Override
    public String getRawDocumentViewName() {
        return rawDocumentViewName;
    }

}
