package datawave.ingest.data.config.ingest;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;

import datawave.ingest.data.Type;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.NormalizedFieldAndValue;
import datawave.util.StringUtils;

/**
 * This class supports aliasing fieldname, and will ensure field names are normalized appropriately. Only [A-Z0-9_.] is permitted in the end.
 *
 *
 */
public class FieldNameAliaserNormalizer {
    /**
     * Configuration parameter to specify changes in spellings of field names. Parameter should contain comma separated strings of the format "OLD:NEW". This
     * parameter supports multiple datatypes, so a valid values would be something like:
     *
     * product.data.category.field.aliases="PRODUCTID:PRODUCT_ID,SUPPLIERNAME:SUPPLIER_NAME"
     *
     */
    public static final String FIELD_ALIASES = ".data.category.field.aliases";

    public static final String INDEX_ALIASES_ENABLED = ".data.category.index.aliases.enabled";
    public static final String INDEX_ALIASES = ".data.category.index.aliases";

    private Map<String,String> _fieldNameAliases = null;
    protected Map<String,String> _canonicalFieldNameAliases = null;
    protected Map<Pattern,String> _compiledFieldPatterns = null;
    private Map<String,HashSet<String>> _indexNameAliases = null;

    public void setAliases(Map<String,String> aliases) {
        _fieldNameAliases = aliases;
    }

    public void setIndexAliases(Map<String,HashSet<String>> aliases) {
        _indexNameAliases = aliases;
    }

    public Map<String,String> getAliases() {
        return _fieldNameAliases;
    }

    public Map<String,HashSet<String>> getIndexAliases() {
        return _indexNameAliases;
    }

    public HashSet<String> getIndexAliases(String fieldName) {
        return _indexNameAliases.get(fieldName);
    }

    public void setup(Type type, Configuration config) {
        // Process the field aliases
        _fieldNameAliases = new HashMap<>();
        String[] a = config.getStrings(type.typeName() + FIELD_ALIASES, (String[]) null);
        if (null != a) {
            for (String s : a) {
                String[] parts = StringUtils.split(s, ':');
                if (parts.length == 2)
                    _fieldNameAliases.put(parts[0], parts[1]);
                else
                    throw new IllegalArgumentException("Missing alias for " + parts[0]);
            }
        }
        _indexNameAliases = new HashMap<>();
        Boolean indexAliasesAllowed = config.getBoolean(type.typeName() + INDEX_ALIASES_ENABLED, false);
        if (indexAliasesAllowed) {
            String indexAliasesConfig = config.get(type.typeName() + INDEX_ALIASES, null);
            if (null != indexAliasesConfig) {
                for (String indexAliasStr : StringUtils.split(indexAliasesConfig, ';')) {
                    if (!indexAliasStr.isEmpty()) {
                        String[] parts = StringUtils.split(indexAliasStr, ':');
                        if (parts.length == 2) {
                            HashSet<String> aliases = new HashSet<>();
                            for (String alias : StringUtils.split(parts[1], ',')) {
                                aliases.add(canonicalizeFieldName(alias, FIELD.NAME));
                            }
                            _indexNameAliases.put(parts[0], aliases);
                        } else {
                            throw new IllegalArgumentException("Improperly formatted index alias");
                        }
                    }
                }
            }
        }
    }

    public NormalizedContentInterface normalizeAndAlias(NormalizedContentInterface field) {
        String fieldName = normalizeAndAlias(field.getIndexedFieldName());
        if (!fieldName.equals(field.getIndexedFieldName())) {
            field = new NormalizedFieldAndValue(field);
            field.setFieldName(fieldName);
        }
        return field;
    }

    public String normalizeAndAlias(String fieldName) {
        if (_canonicalFieldNameAliases == null)
            canonicalizeFieldNames();

        String canonicalFieldName = canonicalizeFieldName(fieldName, FIELD.NAME);
        String alias = _canonicalFieldNameAliases.get(canonicalFieldName);

        if (alias == null) {
            if (_compiledFieldPatterns == null)
                compilePatterns();
            for (Entry<Pattern,String> pattern : _compiledFieldPatterns.entrySet()) {
                Matcher matcher = pattern.getKey().matcher(canonicalFieldName);
                if (matcher.matches()) {
                    String replacement = matcher.group(1);
                    alias = pattern.getValue().replace("*", replacement);
                    break;
                }
            }
        }

        if (alias == null) {
            alias = canonicalFieldName;
        }

        return alias;
    }

    protected void canonicalizeFieldNames() {
        Map<String,String> patterns = new HashMap<>();
        for (Entry<String,String> vFields : _fieldNameAliases.entrySet()) {
            if (vFields.getKey().indexOf('*') < 0) {
                patterns.put(canonicalizeFieldName(vFields.getKey(), FIELD.NAME), canonicalizeFieldName(vFields.getValue(), FIELD.NAME));
            }
        }
        _canonicalFieldNameAliases = patterns;
    }

    protected void compilePatterns() {
        Map<Pattern,String> patterns = new HashMap<>();
        for (Entry<String,String> vFields : _fieldNameAliases.entrySet()) {
            if (vFields.getKey().indexOf('*') >= 0) {
                patterns.put(Pattern.compile(canonicalizeFieldName(vFields.getKey(), FIELD.PATTERN)),
                                canonicalizeFieldName(vFields.getValue(), FIELD.PATTERN_ALIAS));
            }
        }
        _compiledFieldPatterns = patterns;
    }

    protected enum FIELD {
        NAME, PATTERN, PATTERN_ALIAS
    }

    protected String canonicalizeFieldName(String fieldName, FIELD type) {
        if (fieldName == null)
            return null;
        // now ensure the name is normalized appropriately
        StringBuilder builder = new StringBuilder(fieldName.length());
        for (int i = 0; i < fieldName.length(); i++) {
            char c = fieldName.charAt(i);
            if (c >= 'A' && c <= 'Z') {
                // noop
            } else if (c >= '0' && c <= '9') {
                // noop
            } else if (c >= 'a' && c <= 'z') {
                c -= 0x20;
            } else if (c == '.') {
                // noop
            } else if (type != FIELD.NAME && c == '*') {
                if (type == FIELD.PATTERN) {
                    builder.append("(.*");
                    c = ')';
                }
                // else type == FIELD.PATTERN_ALIAS so keep the '*' (i.e. noop)
            } else {
                c = '_';
            }
            builder.append(c);
        }

        return builder.toString();
    }

}
