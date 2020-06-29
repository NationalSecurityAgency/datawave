package datawave.iterators.filter;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.charset.UnsupportedCharsetException;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Identical in functionality to {@link org.apache.accumulo.core.iterators.user.RegExFilter}, but with support for matching against the visibility, and with
 * case-insensitivity.
 */
public class EntryRegexFilter extends Filter {
    
    public static final String ROW_REGEX = "rowRegex";
    public static final String COLUMN_FAMILY_REGEX = "columnFamilyRegex";
    public static final String COLUMN_QUALIFIER_REGEX = "columnQualifierRegex";
    public static final String VISIBILITY_REGEX = "visibilityRegex";
    public static final String VALUE_REGEX = "valueRegex";
    public static final String OR_MATCHES = "orMatches";
    public static final String MATCH_SUBSTRINGS = "matchSubstrings";
    public static final String CASE_INSENSITIVE = "caseInsensitive";
    public static final String ENCODING = "encoding";
    public static final String DEFAULT_ENCODING = StandardCharsets.UTF_8.name();
    
    private Matcher rowMatcher;
    private Matcher columnFamilyMatcher;
    private Matcher columnQualifierMatcher;
    private Matcher visibilityMatcher;
    private Matcher valueMatcher;
    private boolean orMatches = false;
    private boolean matchSubstrings = false;
    private boolean caseInsensitive = false;
    private Charset encoding = Charset.forName(DEFAULT_ENCODING);
    
    /**
     * Helper class to configure a {@link org.apache.accumulo.core.client.IteratorSetting} with the options available for this filter.
     */
    public static class OptionsConfigurator {
        
        private final IteratorSetting setting;
        
        public OptionsConfigurator(IteratorSetting setting) {
            this.setting = setting;
        }
        
        /**
         * Set the pattern to match against the row, column family, column qualifier, column visibility, and value.
         * 
         * @param regex
         *            the pattern to match
         * @return this {@link EntryRegexFilter.OptionsConfigurator}
         */
        public OptionsConfigurator allRegex(String regex) {
            rowRegex(regex);
            columnFamilyRegex(regex);
            columnQualifierRegex(regex);
            visibilityRegex(regex);
            valueRegex(regex);
            return this;
        }
        
        /**
         * Set the pattern to match against the row.
         * 
         * @param regex
         *            the pattern to match
         * @return this {@link EntryRegexFilter.OptionsConfigurator}
         */
        public OptionsConfigurator rowRegex(String regex) {
            setting.addOption(ROW_REGEX, regex);
            return this;
        }
        
        /**
         * Set the pattern to match against the column family.
         * 
         * @param regex
         *            the pattern to match
         * @return this {@link EntryRegexFilter.OptionsConfigurator}
         */
        public OptionsConfigurator columnFamilyRegex(String regex) {
            setting.addOption(COLUMN_FAMILY_REGEX, regex);
            return this;
        }
        
        /**
         * Set the pattern to match against the column qualifier.
         * 
         * @param regex
         *            the pattern to match
         * @return this {@link EntryRegexFilter.OptionsConfigurator}
         */
        public OptionsConfigurator columnQualifierRegex(String regex) {
            setting.addOption(COLUMN_QUALIFIER_REGEX, regex);
            return this;
        }
        
        /**
         * Set the pattern to match against the column visibility.
         * 
         * @param regex
         *            the pattern to match
         * @return this {@link EntryRegexFilter.OptionsConfigurator}
         */
        public OptionsConfigurator visibilityRegex(String regex) {
            setting.addOption(VISIBILITY_REGEX, regex);
            return this;
        }
        
        /**
         * Set the pattern to match against the value.
         * 
         * @param regex
         *            the pattern to match
         * @return this {@link EntryRegexFilter.OptionsConfigurator}
         */
        public OptionsConfigurator valueRegex(String regex) {
            setting.addOption(VALUE_REGEX, regex);
            return this;
        }
        
        /**
         * Consider an entry a match if any configured regexes matches against it, rather than only if all configured regexes match against it. False by
         * default.
         * 
         * @return this {@link EntryRegexFilter.OptionsConfigurator}
         */
        public OptionsConfigurator orMatches() {
            setting.addOption(OR_MATCHES, Boolean.TRUE.toString());
            return this;
        }
        
        /**
         * Allow substring matching for the regexes. False by default.
         * 
         * @return this {@link EntryRegexFilter.OptionsConfigurator}
         */
        public OptionsConfigurator matchSubstrings() {
            setting.addOption(MATCH_SUBSTRINGS, Boolean.TRUE.toString());
            return this;
        }
        
        /**
         * Allow case-insensitive matching for the regexes. False by default.
         * 
         * @return this {@link EntryRegexFilter.OptionsConfigurator}
         */
        public OptionsConfigurator caseInsensitive() {
            setting.addOption(CASE_INSENSITIVE, Boolean.TRUE.toString());
            return this;
        }
        
        /**
         * Set the encoding string to use when interpreting characters. UTF-8 by default.
         * 
         * @param encoding
         *            the encoding string to use
         * @return this {@link EntryRegexFilter.OptionsConfigurator}
         */
        public OptionsConfigurator encoding(String encoding) {
            setting.addOption(ENCODING, encoding);
            return this;
        }
    }
    
    public static OptionsConfigurator configureOptions(IteratorSetting setting) {
        return new OptionsConfigurator(setting);
    }
    
    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        EntryRegexFilter result = (EntryRegexFilter) super.deepCopy(env);
        result.rowMatcher = copyMatcher(rowMatcher);
        result.columnFamilyMatcher = copyMatcher(columnFamilyMatcher);
        result.columnQualifierMatcher = copyMatcher(columnQualifierMatcher);
        result.visibilityMatcher = copyMatcher(visibilityMatcher);
        result.valueMatcher = copyMatcher(valueMatcher);
        result.orMatches = orMatches;
        result.matchSubstrings = matchSubstrings;
        result.caseInsensitive = caseInsensitive;
        return result;
    }
    
    @Override
    public boolean accept(Key key, Value value) {
        if (orMatches) {
            return ((matches(rowMatcher, rowMatcher == null ? null : key.getRowData()))
                            || (matches(columnFamilyMatcher, columnFamilyMatcher == null ? null : key.getColumnFamilyData()))
                            || (matches(columnQualifierMatcher, columnQualifierMatcher == null ? null : key.getColumnQualifierData()))
                            || (matches(visibilityMatcher, visibilityMatcher == null ? null : key.getColumnVisibilityData())) || (matches(valueMatcher,
                                value.get(), value.get().length)));
        }
        return ((matches(rowMatcher, rowMatcher == null ? null : key.getRowData()))
                        && (matches(columnFamilyMatcher, columnFamilyMatcher == null ? null : key.getColumnFamilyData()))
                        && (matches(columnQualifierMatcher, columnQualifierMatcher == null ? null : key.getColumnQualifierData()))
                        && (matches(visibilityMatcher, visibilityMatcher == null ? null : key.getColumnVisibilityData())) && (matches(valueMatcher,
                            value.get(), value.get().length)));
    }
    
    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        super.init(source, options, env);
        
        if (options.containsKey(CASE_INSENSITIVE)) {
            caseInsensitive = Boolean.parseBoolean(options.get(CASE_INSENSITIVE));
        } else {
            caseInsensitive = false;
        }
        
        if (options.containsKey(ROW_REGEX)) {
            rowMatcher = compile(options.get(ROW_REGEX));
        } else {
            rowMatcher = null;
        }
        
        if (options.containsKey(COLUMN_FAMILY_REGEX)) {
            columnFamilyMatcher = compile(options.get(COLUMN_FAMILY_REGEX));
        } else {
            columnFamilyMatcher = null;
        }
        
        if (options.containsKey(COLUMN_QUALIFIER_REGEX)) {
            columnQualifierMatcher = compile(options.get(COLUMN_QUALIFIER_REGEX));
        } else {
            columnQualifierMatcher = null;
        }
        
        if (options.containsKey(VISIBILITY_REGEX)) {
            visibilityMatcher = compile(options.get(VISIBILITY_REGEX));
        } else {
            visibilityMatcher = null;
        }
        
        if (options.containsKey(VALUE_REGEX)) {
            valueMatcher = compile(options.get(VALUE_REGEX));
        } else {
            valueMatcher = null;
        }
        
        if (options.containsKey(OR_MATCHES)) {
            orMatches = Boolean.parseBoolean(options.get(OR_MATCHES));
        } else {
            orMatches = false;
        }
        
        if (options.containsKey(MATCH_SUBSTRINGS)) {
            matchSubstrings = Boolean.parseBoolean(options.get(MATCH_SUBSTRINGS));
        } else {
            matchSubstrings = false;
        }
        
        if (options.containsKey(ENCODING)) {
            encoding = Charset.forName(options.get(ENCODING));
        } else {
            encoding = Charset.forName(DEFAULT_ENCODING);
        }
    }
    
    @Override
    public IteratorOptions describeOptions() {
        IteratorOptions options = super.describeOptions();
        options.setName("regex");
        options.setDescription("The RegExFilter/Iterator allows you to filter for" + " key/value pairs based on regular expressions");
        options.addNamedOption(EntryRegexFilter.ROW_REGEX, "regular expression on row");
        options.addNamedOption(EntryRegexFilter.COLUMN_FAMILY_REGEX, "regular expression on column family");
        options.addNamedOption(EntryRegexFilter.COLUMN_QUALIFIER_REGEX, "regular expression on column qualifier");
        options.addNamedOption(EntryRegexFilter.VISIBILITY_REGEX, "regular expression on column visibility");
        options.addNamedOption(EntryRegexFilter.VALUE_REGEX, "regular expression on value");
        options.addNamedOption(EntryRegexFilter.OR_MATCHES, "use OR instead of AND when multiple regexes given");
        options.addNamedOption(EntryRegexFilter.MATCH_SUBSTRINGS, "match on substrings");
        options.addNamedOption(EntryRegexFilter.CASE_INSENSITIVE, "match with case-insensitivity");
        options.addNamedOption(EntryRegexFilter.ENCODING, "character encoding of byte array value (default is " + DEFAULT_ENCODING + ")");
        return options;
    }
    
    @Override
    public boolean validateOptions(Map<String,String> options) {
        if (!super.validateOptions(options)) {
            return false;
        }
        
        try {
            if (options.containsKey(ROW_REGEX)) {
                compile(options.get(ROW_REGEX));
            }
            if (options.containsKey(COLUMN_FAMILY_REGEX)) {
                compile(options.get(COLUMN_FAMILY_REGEX));
            }
            if (options.containsKey(COLUMN_QUALIFIER_REGEX)) {
                compile(options.get(COLUMN_QUALIFIER_REGEX));
            }
            if (options.containsKey(VISIBILITY_REGEX)) {
                compile(options.get(VISIBILITY_REGEX));
            }
            if (options.containsKey(VALUE_REGEX)) {
                compile(options.get(VALUE_REGEX));
            }
        } catch (Exception e) {
            throw new IllegalArgumentException("Unable to compile regex", e);
        }
        
        if (options.containsKey(ENCODING)) {
            try {
                String encodingOpt = options.get(ENCODING);
                this.encoding = Charset.forName(encodingOpt.isEmpty() ? DEFAULT_ENCODING : encodingOpt);
            } catch (UnsupportedCharsetException e) {
                throw new IllegalArgumentException("invalid encoding " + ENCODING + ":" + this.encoding, e);
            }
        }
        
        return true;
    }
    
    // Return a matcher for the pattern compiled from the specified regex, applying case-insensitivity if configured.
    private Matcher compile(String regex) {
        if (!caseInsensitive) {
            return Pattern.compile(regex).matcher("");
        } else {
            return Pattern.compile(regex, Pattern.CASE_INSENSITIVE).matcher("");
        }
    }
    
    // Return a copy of the specified matcher.
    private Matcher copyMatcher(Matcher matcher) {
        return matcher != null ? matcher.pattern().matcher("") : null;
    }
    
    // Return true if the specified matcher matches the specified byte sequence.
    private boolean matches(Matcher matcher, ByteSequence bs) {
        if (matcher != null) {
            matcher.reset(new String(bs.getBackingArray(), bs.offset(), bs.length(), encoding));
            return matchSubstrings ? matcher.find() : matcher.matches();
        }
        return !orMatches;
    }
    
    // Return true if the specified matcher matches the specified data.
    private boolean matches(Matcher matcher, byte[] data, int len) {
        if (matcher != null) {
            matcher.reset(new String(data, 0, len, encoding));
            return matchSubstrings ? matcher.find() : matcher.matches();
        }
        return !orMatches;
    }
}
