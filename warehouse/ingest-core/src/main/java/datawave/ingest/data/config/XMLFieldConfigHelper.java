package datawave.ingest.data.config;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.log4j.Logger;
import org.apache.xerces.jaxp.SAXParserFactoryImpl;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import com.google.common.collect.ImmutableSet;

import datawave.data.type.LcNoDiacriticsType;
import datawave.ingest.data.config.ingest.BaseIngestHelper;

/** Helper class to read XML based Field Configurations */
public final class XMLFieldConfigHelper implements FieldConfigHelper {

    private static final Logger log = Logger.getLogger(XMLFieldConfigHelper.class);

    /** be explicit and use Apache Xerces-J here instead of relying on java to plug in the proper parser */
    private static final SAXParserFactory parserFactory = SAXParserFactoryImpl.newInstance();

    private boolean noMatchStored = true;
    private boolean noMatchIndexed = false;
    private boolean noMatchReverseIndexed = false;
    private boolean noMatchTokenized = false;
    private boolean noMatchReverseTokenized = false;
    private String noMatchFieldType = null;

    private final Map<String,FieldInfo> knownFields = new HashMap<>();
    private TreeMap<Matcher,String> patterns = new TreeMap<>(new BaseIngestHelper.MatcherComparator());

    public static class FieldInfo {
        boolean stored;
        boolean indexed;
        boolean reverseIndexed;
        boolean tokenized;
        boolean reverseTokenized;
    }

    /**
     * Attempt to load the field config fieldHelper from the specified file, which is expected to be found on the classpath.
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
        if (fieldConfigFile == null) {
            return null;
        }

        try (InputStream in = getAsStream(fieldConfigFile)) {
            if (in != null) {
                log.info("Loading field configuration from configuration file: " + fieldConfigFile);
                return new XMLFieldConfigHelper(in, baseIngestHelper);
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
                log.error("Could not open config location: " + fieldConfigPath, e);
                return null;
            }
        }
    }

    public String toString() {
        return "[FieldConfigHelper: " + knownFields.size() + " known fields, " + patterns.size() + " of those are patterns, " + "nomatch, indexed:"
                        + noMatchIndexed + " reverseIndexed:" + noMatchReverseIndexed + " tokenized:" + noMatchTokenized + " reverseTokenized:"
                        + noMatchReverseTokenized + "]";

    }

    public XMLFieldConfigHelper(InputStream in, BaseIngestHelper helper) throws ParserConfigurationException, SAXException, IOException {
        final FieldConfigHandler handler = new FieldConfigHandler(this, helper);
        SAXParser parser = parserFactory.newSAXParser();
        parser.parse(in, handler);

        log.info("Loaded FieldConfigHelper: " + this);
    }

    public boolean addKnownField(String fieldName, FieldInfo info) {
        // must track the fields we've seen so we can properly apply default rules.
        return (knownFields.put(fieldName, info) == null);
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
        if (knownFields.containsKey(fieldName)) {
            return this.knownFields.get(fieldName).stored;
        }

        String pattern = findMatchingPattern(fieldName);
        if (pattern != null) {
            return this.knownFields.get(pattern).stored;
        }

        return isNoMatchStored();
    }

    @Override
    public boolean isIndexedField(String fieldName) {
        if (knownFields.containsKey(fieldName)) {
            return this.knownFields.get(fieldName).indexed;
        }

        String pattern = findMatchingPattern(fieldName);
        if (pattern != null) {
            return this.knownFields.get(pattern).indexed;
        }

        return isNoMatchIndexed();
    }

    @Override
    public boolean isIndexOnlyField(String fieldName) {
        return isIndexedField(fieldName) && !isStoredField(fieldName);
    }

    @Override
    public boolean isReverseIndexedField(String fieldName) {
        if (knownFields.containsKey(fieldName)) {
            return this.knownFields.get(fieldName).reverseIndexed;
        }

        String pattern = findMatchingPattern(fieldName);
        if (pattern != null) {
            return this.knownFields.get(pattern).reverseIndexed;
        }

        return isNoMatchReverseIndexed();
    }

    @Override
    public boolean isTokenizedField(String fieldName) {
        if (knownFields.containsKey(fieldName)) {
            return this.knownFields.get(fieldName).tokenized;
        }

        String pattern = findMatchingPattern(fieldName);
        if (pattern != null) {
            return this.knownFields.get(pattern).tokenized;
        }

        return isNoMatchTokenized();
    }

    @Override
    public boolean isReverseTokenizedField(String fieldName) {
        if (knownFields.containsKey(fieldName)) {
            return this.knownFields.get(fieldName).reverseTokenized;
        }

        String pattern = findMatchingPattern(fieldName);
        if (pattern != null) {
            return this.knownFields.get(pattern).reverseTokenized;
        }

        return isNoMatchReverseTokenized();
    }

    public boolean isNoMatchStored() {
        return noMatchStored;
    }

    public void setNoMatchStored(boolean noMatchStored) {
        this.noMatchStored = noMatchStored;
    }

    public boolean isNoMatchIndexed() {
        return noMatchIndexed;
    }

    public void setNoMatchIndexed(boolean noMatchIndexed) {
        this.noMatchIndexed = noMatchIndexed;
    }

    public boolean isNoMatchReverseIndexed() {
        return noMatchReverseIndexed;
    }

    public void setNoMatchReverseIndexed(boolean noMatchReverseIndexed) {
        this.noMatchReverseIndexed = noMatchReverseIndexed;
    }

    public boolean isNoMatchTokenized() {
        return noMatchTokenized;
    }

    public void setNoMatchTokenized(boolean noMatchTokenized) {
        this.noMatchTokenized = noMatchTokenized;
    }

    public boolean isNoMatchReverseTokenized() {
        return noMatchReverseTokenized;
    }

    public void setNoMatchReverseTokenized(boolean noMatchReverseTokenized) {
        this.noMatchReverseTokenized = noMatchReverseTokenized;
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
                    throw new IllegalArgumentException("Unexpected attribute encounteded in: " + uri + " in 'default' tag: '" + qn + "'");
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
                    throw new IllegalArgumentException("Unexpected attribute encounteded in: " + uri + " in 'nomatch' tag: '" + qn + "'");
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
                    throw new IllegalArgumentException("Unexpected attribute encounteded in: " + uri + " in 'field' tag: '" + qn + "'");
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
                    log.warn("No BaseIngestHelper set, ignoring type information for " + name + " in configuration file");
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
                    throw new IllegalArgumentException("Unexpected attribute encounteded in: " + uri + " in 'field' tag: '" + qn + "'");
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
                    log.warn("No BaseIngestHelper set, ignoring type information for " + pattern + " in configuration file");
                }
            }
        }
    }
}
