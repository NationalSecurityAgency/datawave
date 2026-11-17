package datawave.ingest.data.config;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.regex.Matcher;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import com.google.common.collect.ImmutableSet;

import datawave.data.type.LcNoDiacriticsType;
import datawave.ingest.data.config.ingest.BaseIngestHelper;

/** Helper class to read XML based Field Configurations */
public final class XMLFieldConfigHelper implements FieldConfigHelper {

    private static final Logger log = LoggerFactory.getLogger(XMLFieldConfigHelper.class);

    /** be explicit and use Apache Xerces-J here instead of relying on java to plug in the proper parser */
    private static final SAXParserFactory parserFactory = SAXParserFactory.newInstance();

    private final FieldInfo noMatchFieldInfo = new FieldInfo(true, false, false, false, false);
    private String noMatchFieldType = null;

    private final String configSource;
    private final Map<String,FieldInfo> knownFields = new HashMap<>();
    /**
     * Memoizes the fully resolved FieldInfo per field name, including pattern matches and no-match results. The cache's bound and overflow policy are described
     * by {@link FieldConfigHelperConstants#FIELD_CONFIG_CACHE}. Not thread-safe: instances are confined to a single thread.
     */
    private final FieldLookupCache<String,FieldInfo> resolvedFields;
    /** the mapping function passed to {@code computeIfAbsent} on every lookup, held so the capturing method reference is allocated once */
    private final Function<String,FieldInfo> resolveFieldInfoFunction = this::resolveFieldInfo;
    /**
     * Single-entry "last field looked up" cache in front of {@link #resolvedFields}. Ingest call sites query the same field name several times in a row (once
     * per {@code is*Field} accessor), so checking these two fields first skips the hash probe on repeat lookups. Relies on the same thread confinement as
     * {@link #resolvedFields}.
     */
    private String previousFieldName;
    private FieldInfo previousFieldInfo;
    private TreeMap<Matcher,String> patterns = new TreeMap<>(new BaseIngestHelper.MatcherComparator());

    private static final String UNEXPECTED_ATTRIBUTE = "Unexpected attribute encountered in: ";

    public static class FieldInfo {
        boolean stored;
        boolean indexed;
        boolean reverseIndexed;
        boolean tokenized;
        boolean reverseTokenized;

        FieldInfo() {}

        FieldInfo(boolean stored, boolean indexed, boolean reverseIndexed, boolean tokenized, boolean reverseTokenized) {
            this.stored = stored;
            this.indexed = indexed;
            this.reverseIndexed = reverseIndexed;
            this.tokenized = tokenized;
            this.reverseTokenized = reverseTokenized;
        }
    }

    /**
     * Attempt to load the field config fieldHelper from the specified file, which is expected to be found on the classpath, with the field lookup cache
     * described for this datatype.
     *
     * @param fieldConfigFile
     *            the field configuration file name
     * @param baseIngestHelper
     *            the ingest helper
     * @param conf
     *            the configuration to read the cache settings from
     * @throws IllegalArgumentException
     *             if the file can't be found or an exception occurs when reading the file.
     * @return null if no a null value was specified for fieldConfigFile - or a populated FieldConfigHelper.
     */
    public static XMLFieldConfigHelper load(String fieldConfigFile, BaseIngestHelper baseIngestHelper, Configuration conf) {
        String typeName = baseIngestHelper.getType().typeName();
        return load(fieldConfigFile, baseIngestHelper, FieldLookupCache.parse(conf, typeName, FieldConfigHelperConstants.FIELD_CONFIG_CACHE));
    }

    /**
     * Attempt to load the field config fieldHelper from the specified file, which is expected to be found on the classpath, with an unbounded field lookup
     * cache.
     *
     * @param fieldConfigFile
     *            the field configuration file name
     * @param baseIngestHelper
     *            the ingest helper
     * @throws IllegalArgumentException
     *             if the file can't be found or an exception occurs when reading the file.
     * @return null if no a null value was specified for fieldConfigFile - or a populated FieldConfigHelper.
     */
    public static XMLFieldConfigHelper load(String fieldConfigFile, BaseIngestHelper baseIngestHelper) {
        return load(fieldConfigFile, baseIngestHelper, new FieldLookupCache<>());
    }

    /**
     * Attempt to load the field config fieldHelper from the specified file, which is expected to be found on the classpath.
     *
     * @param fieldConfigFile
     *            the field configuration file name
     * @param baseIngestHelper
     *            the ingest helper
     * @param resolvedFieldCache
     *            the field lookup cache backing the loaded helper
     * @throws IllegalArgumentException
     *             if the file can't be found or an exception occurs when reading the file.
     * @return null if no a null value was specified for fieldConfigFile - or a populated FieldConfigHelper.
     */
    private static XMLFieldConfigHelper load(String fieldConfigFile, BaseIngestHelper baseIngestHelper, FieldLookupCache<String,FieldInfo> resolvedFieldCache) {
        if (fieldConfigFile == null) {
            return null;
        }

        try (InputStream in = getAsStream(fieldConfigFile)) {
            if (in != null) {
                log.info("Loading field configuration from configuration file: {}", fieldConfigFile);
                return new XMLFieldConfigHelper(in, baseIngestHelper, fieldConfigFile, resolvedFieldCache);
            } else {
                throw new IllegalArgumentException("Field config file '" + fieldConfigFile + "' not found!");
            }
        } catch (Exception e) {
            throw new IllegalArgumentException("Exception reading field config file '" + fieldConfigFile + "': " + e.getMessage(), e);
        }

    }

    /*
     * Opens a configuration path as an InputStream. If no scheme is given (e.g. http://), then the classpath is assumed.
     */
    private static InputStream getAsStream(String fieldConfigPath) {
        URI uri = URI.create(fieldConfigPath);

        if (uri.getScheme() == null) {
            return XMLFieldConfigHelper.class.getClassLoader().getResourceAsStream(fieldConfigPath);
        } else {
            try {
                return uri.toURL().openStream();
            } catch (IOException e) {
                log.error("Could not open config location: {}", fieldConfigPath, e);
                return null;
            }
        }
    }

    public String toString() {
        return "[FieldConfigHelper: " + knownFields.size() + " known fields, " + patterns.size() + " of those are patterns, " + "nomatch, indexed:"
                        + isNoMatchIndexed() + " reverseIndexed:" + isNoMatchReverseIndexed() + " tokenized:" + isNoMatchTokenized() + " reverseTokenized:"
                        + isNoMatchReverseTokenized() + "]";

    }

    public XMLFieldConfigHelper(InputStream in, BaseIngestHelper helper) throws ParserConfigurationException, SAXException, IOException {
        this(in, helper, null);
    }

    public XMLFieldConfigHelper(InputStream in, BaseIngestHelper helper, String source) throws ParserConfigurationException, SAXException, IOException {
        this(in, helper, source, new FieldLookupCache<>());
    }

    XMLFieldConfigHelper(InputStream in, BaseIngestHelper helper, String source, FieldLookupCache<String,FieldInfo> resolvedFields)
                    throws ParserConfigurationException, SAXException, IOException {
        this.configSource = source;
        this.resolvedFields = resolvedFields;

        final FieldConfigHandler handler = new FieldConfigHandler(this, helper);
        SAXParser parser = parserFactory.newSAXParser();
        parser.parse(in, handler);

        log.info("Loaded FieldConfigHelper: {}", this);
    }

    public boolean addKnownField(String fieldName, FieldInfo info) {
        // must track the fields we've seen so we can properly apply default rules.
        return (knownFields.put(fieldName, info) == null);
    }

    @Override
    public String describeSource() {
        return configSource;
    }

    public boolean addKnownFieldPattern(String fieldName, FieldInfo info, Matcher pattern) {
        patterns.put(pattern, fieldName);
        return addKnownField(fieldName, info);
    }

    public void setNoMatchFieldType(String fieldType) {
        this.noMatchFieldType = fieldType;
    }

    @Override
    public boolean isStoredField(String fieldName) {
        return getFieldInfo(fieldName).stored;
    }

    @Override
    public boolean isIndexedField(String fieldName) {
        return getFieldInfo(fieldName).indexed;
    }

    @Override
    public boolean isIndexOnlyField(String fieldName) {
        FieldInfo info = getFieldInfo(fieldName);
        return info.indexed && !info.stored;
    }

    @Override
    public boolean isReverseIndexedField(String fieldName) {
        return getFieldInfo(fieldName).reverseIndexed;
    }

    @Override
    public boolean isTokenizedField(String fieldName) {
        return getFieldInfo(fieldName).tokenized;
    }

    @Override
    public boolean isReverseTokenizedField(String fieldName) {
        return getFieldInfo(fieldName).reverseTokenized;
    }

    private FieldInfo getFieldInfo(String fieldName) {
        if (fieldName.equals(previousFieldName)) {
            return previousFieldInfo;
        }
        FieldInfo info = resolvedFields.computeIfAbsent(fieldName, resolveFieldInfoFunction);
        previousFieldName = fieldName;
        previousFieldInfo = info;
        return info;
    }

    private FieldInfo resolveFieldInfo(String fieldName) {
        FieldInfo info = knownFields.get(fieldName);
        if (info != null) {
            return info;
        }
        if (!patterns.isEmpty()) {
            String pattern = findMatchingPattern(fieldName);
            if (pattern != null) {
                return knownFields.get(pattern);
            }
        }
        return noMatchFieldInfo;
    }

    public boolean isNoMatchStored() {
        return noMatchFieldInfo.stored;
    }

    public void setNoMatchStored(boolean noMatchStored) {
        this.noMatchFieldInfo.stored = noMatchStored;
    }

    public boolean isNoMatchIndexed() {
        return noMatchFieldInfo.indexed;
    }

    public void setNoMatchIndexed(boolean noMatchIndexed) {
        this.noMatchFieldInfo.indexed = noMatchIndexed;
    }

    public boolean isNoMatchReverseIndexed() {
        return noMatchFieldInfo.reverseIndexed;
    }

    public void setNoMatchReverseIndexed(boolean noMatchReverseIndexed) {
        this.noMatchFieldInfo.reverseIndexed = noMatchReverseIndexed;
    }

    public boolean isNoMatchTokenized() {
        return noMatchFieldInfo.tokenized;
    }

    public void setNoMatchTokenized(boolean noMatchTokenized) {
        this.noMatchFieldInfo.tokenized = noMatchTokenized;
    }

    public boolean isNoMatchReverseTokenized() {
        return noMatchFieldInfo.reverseTokenized;
    }

    public void setNoMatchReverseTokenized(boolean noMatchReverseTokenized) {
        this.noMatchFieldInfo.reverseTokenized = noMatchReverseTokenized;
    }

    FieldLookupCache<String,FieldInfo> getResolvedFields() {
        return this.resolvedFields;
    }

    /**
     * Return true if any of the specified patterns matches the field name provided.
     *
     * @param fieldName
     *            the field name
     * @return whether any patterns were found or not
     */
    private String findMatchingPattern(String fieldName) {
        Matcher bestMatch = BaseIngestHelper.getBestMatch(patterns.keySet(), fieldName);
        return (bestMatch == null ? null : patterns.get(bestMatch));
    }

    static final class FieldConfigHandler extends DefaultHandler {
        public static final String STORED = "stored";
        public static final String INDEXED = "indexed";
        public static final String REVERSE_INDEXED = "reverseIndexed";
        public static final String INDEX_TYPE = "indexType";
        public static final String TOKENIZED = "tokenized";
        public static final String REVERSE_TOKENIZED = "reverseTokenized";

        static final Set<String> expectedDefaultAttributes;
        static final Set<String> expectedNoMatchAttributes;

        static {
            Set<String> attr = new HashSet<>();
            attr.add(STORED);
            attr.add(INDEXED);
            attr.add(REVERSE_INDEXED);
            attr.add(TOKENIZED);
            attr.add(REVERSE_TOKENIZED);
            attr.add(INDEX_TYPE);
            expectedDefaultAttributes = ImmutableSet.copyOf(attr);
            expectedNoMatchAttributes = ImmutableSet.copyOf(attr);
        }

        private final XMLFieldConfigHelper fieldHelper;
        private final BaseIngestHelper ingestHelper;

        boolean defaultsComplete = false;

        boolean defaultStored = true;
        boolean defaultIndexed = false;
        boolean defaultReverseIndexed = false;
        boolean defaultTokenized = false;
        boolean defaultReverseTokenized = false;

        String defaultFieldType = LcNoDiacriticsType.class.getCanonicalName();

        FieldConfigHandler(XMLFieldConfigHelper fieldHelper, BaseIngestHelper ingestHelper) {
            this.fieldHelper = fieldHelper;
            this.ingestHelper = ingestHelper;
        }

        @Override
        public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
            if ("default".equals(qName)) {
                startDefault(uri, localName, qName, attributes);
            } else if ("nomatch".equals(qName)) {
                startNoMatch(uri, localName, qName, attributes);
            } else if ("field".equals(qName)) {
                startField(uri, localName, qName, attributes);
            } else if ("fieldPattern".equals(qName)) {
                startFieldPattern(uri, localName, qName, attributes);
            } else if ("fieldConfig".equals(qName)) {
                // structurral tag only, ignore for now, but allow.
            } else {
                throw new IllegalArgumentException("Unexpected element encounteded in: " + uri + ": qName: '" + qName + "' localName: '" + localName + "'");
            }
        }

        void startDefault(String uri, String localName, String qName, Attributes attributes) throws SAXException {
            final int sz = attributes.getLength();
            final Set<String> seenAttr = new HashSet<>(expectedDefaultAttributes);

            for (int i = 0; i < sz; i++) {
                final String qn = attributes.getQName(i);
                final String lv = attributes.getValue(i);

                if (STORED.equals(qn)) {
                    this.defaultStored = Boolean.parseBoolean(lv);
                    seenAttr.remove(STORED);
                } else if (INDEXED.equals(qn)) {
                    this.defaultIndexed = Boolean.parseBoolean(lv);
                    seenAttr.remove(INDEXED);
                } else if (REVERSE_INDEXED.equals(qn)) {
                    this.defaultReverseIndexed = Boolean.parseBoolean(lv);
                    seenAttr.remove(REVERSE_INDEXED);
                } else if (TOKENIZED.equals(qn)) {
                    this.defaultTokenized = Boolean.parseBoolean(lv);
                    seenAttr.remove(TOKENIZED);
                } else if (REVERSE_TOKENIZED.equals(qn)) {
                    this.defaultReverseTokenized = Boolean.parseBoolean(lv);
                    seenAttr.remove(REVERSE_TOKENIZED);
                } else if (INDEX_TYPE.equals(qn)) {
                    this.defaultFieldType = lv;
                    seenAttr.remove(INDEX_TYPE);
                } else {
                    throw new IllegalArgumentException(UNEXPECTED_ATTRIBUTE + uri + " in 'default' tag: '" + qn + "'");
                }
            }

            if (!seenAttr.isEmpty()) {
                throw new IllegalArgumentException("default tag incomplete, '" + seenAttr + "' attributes were missing");
            } else {
                defaultsComplete = true;
            }
        }

        void startNoMatch(String uri, String localName, String qName, Attributes attributes) throws SAXException {
            final int sz = attributes.getLength();
            final Set<String> seenAttr = new HashSet<>(expectedDefaultAttributes);

            for (int i = 0; i < sz; i++) {
                final String qn = attributes.getQName(i);
                final String lv = attributes.getValue(i);

                if (STORED.equals(qn)) {
                    fieldHelper.setNoMatchStored(Boolean.parseBoolean(lv));
                    seenAttr.remove(STORED);
                } else if (INDEXED.equals(qn)) {
                    fieldHelper.setNoMatchIndexed(Boolean.parseBoolean(lv));
                    seenAttr.remove(INDEXED);
                } else if (REVERSE_INDEXED.equals(qn)) {
                    fieldHelper.setNoMatchReverseIndexed(Boolean.parseBoolean(lv));
                    seenAttr.remove(REVERSE_INDEXED);
                } else if (TOKENIZED.equals(qn)) {
                    fieldHelper.setNoMatchTokenized(Boolean.parseBoolean(lv));
                    seenAttr.remove(TOKENIZED);
                } else if (REVERSE_TOKENIZED.equals(qn)) {
                    fieldHelper.setNoMatchReverseTokenized(Boolean.parseBoolean(lv));
                    seenAttr.remove(REVERSE_TOKENIZED);
                } else if (INDEX_TYPE.equals(qn)) {
                    if (this.ingestHelper != null) {
                        this.ingestHelper.updateDatawaveTypes(null, lv);
                    } else {
                        log.warn("No BaseIngestHelper set, ignoring type information for nomatch in configuration file");
                    }
                    seenAttr.remove(INDEX_TYPE);
                } else {
                    throw new IllegalArgumentException(UNEXPECTED_ATTRIBUTE + uri + " in 'nomatch' tag: '" + qn + "'");
                }
            }

            if (!seenAttr.isEmpty()) {
                throw new IllegalArgumentException("nomatch tag incomplete, '" + seenAttr + "' attributes were missing");
            }
        }

        void startField(String uri, String localName, String qName, Attributes attributes) throws SAXException {

            if (!defaultsComplete) {
                throw new IllegalStateException("Can't define a field without defaults - expected default tag before field tag");
            }

            final int sz = attributes.getLength();

            String name = null;
            FieldInfo fieldInfo = new FieldInfo();
            fieldInfo.stored = this.defaultStored;
            fieldInfo.indexed = this.defaultIndexed;
            fieldInfo.reverseIndexed = this.defaultReverseIndexed;
            fieldInfo.tokenized = this.defaultTokenized;
            fieldInfo.reverseTokenized = this.defaultReverseTokenized;
            String fieldType = this.defaultFieldType;

            for (int i = 0; i < sz; i++) {
                final String qn = attributes.getQName(i);
                final String lv = attributes.getValue(i);

                if (STORED.equals(qn)) {
                    fieldInfo.stored = Boolean.parseBoolean(lv);
                } else if (INDEXED.equals(qn)) {
                    fieldInfo.indexed = Boolean.parseBoolean(lv);
                } else if (REVERSE_INDEXED.equals(qn)) {
                    fieldInfo.reverseIndexed = Boolean.parseBoolean(lv);
                } else if (TOKENIZED.equals(qn)) {
                    fieldInfo.tokenized = Boolean.parseBoolean(lv);
                } else if (REVERSE_TOKENIZED.equals(qn)) {
                    fieldInfo.reverseTokenized = Boolean.parseBoolean(lv);
                } else if ("name".equals(qn)) {
                    name = lv;
                } else if (INDEX_TYPE.equals(qn)) {
                    fieldType = lv;
                } else {
                    throw new IllegalArgumentException(UNEXPECTED_ATTRIBUTE + uri + " in 'field' tag: '" + qn + "'");
                }
            }

            if (name == null) {
                throw new IllegalArgumentException("No field called 'name' specified");
            } else if (!this.fieldHelper.addKnownField(name, fieldInfo)) {
                throw new IllegalArgumentException(
                                "Field " + name + " was already seen, check configuration file for duplicate entries (among fieldPattern, field tags)");
            }
            if (fieldType != null) {
                if (this.ingestHelper != null) {
                    this.ingestHelper.updateDatawaveTypes(name, fieldType);
                } else if (fieldType.equals(this.defaultFieldType)) {
                    log.warn("No BaseIngestHelper set, ignoring type information for {} in configuration file", name);
                }
            }
        }

        void startFieldPattern(String uri, String localName, String qName, Attributes attributes) throws SAXException {

            if (!defaultsComplete) {
                throw new IllegalStateException("Can't define a fieldPattern without defaults - expected default tag before field tag");
            }

            final int sz = attributes.getLength();

            String pattern = null;
            FieldInfo fieldInfo = new FieldInfo();
            fieldInfo.stored = this.defaultStored;
            fieldInfo.indexed = this.defaultIndexed;
            fieldInfo.reverseIndexed = this.defaultReverseIndexed;
            fieldInfo.tokenized = this.defaultTokenized;
            fieldInfo.reverseTokenized = this.defaultReverseTokenized;
            String fieldType = this.defaultFieldType;

            for (int i = 0; i < sz; i++) {
                final String qn = attributes.getQName(i);
                final String lv = attributes.getValue(i);

                if (STORED.equals(qn)) {
                    fieldInfo.stored = Boolean.parseBoolean(lv);
                } else if (INDEXED.equals(qn)) {
                    fieldInfo.indexed = Boolean.parseBoolean(lv);
                } else if (REVERSE_INDEXED.equals(qn)) {
                    fieldInfo.reverseIndexed = Boolean.parseBoolean(lv);
                } else if (TOKENIZED.equals(qn)) {
                    fieldInfo.tokenized = Boolean.parseBoolean(lv);
                } else if (REVERSE_TOKENIZED.equals(qn)) {
                    fieldInfo.reverseTokenized = Boolean.parseBoolean(lv);
                } else if ("pattern".equals(qn)) {
                    pattern = lv;
                } else if (INDEX_TYPE.equals(qn)) {
                    fieldType = lv;
                } else {
                    throw new IllegalArgumentException(UNEXPECTED_ATTRIBUTE + uri + " in 'field' tag: '" + qn + "'");
                }
            }

            if (pattern == null) {
                throw new IllegalArgumentException("No field called 'name' specified");
            } else if (!this.fieldHelper.addKnownFieldPattern(pattern, fieldInfo, BaseIngestHelper.compileFieldNamePattern(pattern))) {
                throw new IllegalArgumentException(
                                "Field pattern " + pattern + " is already known, check configuration file for duplicates (among fieldPattern, field tag)");
            }

            if (fieldType != null) {
                if (this.ingestHelper != null) {
                    this.ingestHelper.updateDatawaveTypes(pattern, fieldType);
                } else if (!fieldType.equals(this.defaultFieldType)) {
                    log.warn("No BaseIngestHelper set, ignoring type information for {} in configuration file", pattern);
                }
            }
        }
    }
}
