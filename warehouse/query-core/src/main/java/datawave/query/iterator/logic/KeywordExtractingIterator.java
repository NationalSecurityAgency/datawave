package datawave.query.iterator.logic;

import static datawave.util.keyword.KeywordExtractor.EMPTY_RESULTS;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import datawave.query.Constants;
import datawave.query.table.parser.ContentKeyValueFactory;
import datawave.util.keyword.KeywordExtractor;
import datawave.util.keyword.KeywordResults;
import datawave.util.keyword.VisibleContent;

/** An iterator that will execute the keyword extractor when given 'd' column ranges to scan for specific documents */
public class KeywordExtractingIterator implements SortedKeyValueIterator<Key,Value>, OptionDescriber {
    private static final Logger log = LoggerFactory.getLogger(KeywordExtractingIterator.class);

    private static final Collection<ByteSequence> D_COLUMN_FAMILY_BYTE_SEQUENCE = Collections
                    .singleton(new ArrayByteSequence(Constants.D_COLUMN_FAMILY.getBytes()));

    public static final String VIEW_NAMES = "view.names";
    public static final String DOCUMENT_LANGUAGES = "document.languages";

    public static final String DEFAULT_VIEW_NAMES = "CONTENT";

    private static final Map<String,String> defaultMapOptions;

    static {
        defaultMapOptions = new HashMap<>();
        defaultMapOptions.put(KeywordExtractor.MIN_NGRAMS, "minimum number of words (ngrams) per keyword");
        defaultMapOptions.put(KeywordExtractor.MAX_NGRAMS, "maximum number of words (ngrams) per keyword");
        defaultMapOptions.put(KeywordExtractor.MAX_KEYWORDS, "maximum number of keywords to extract");
        defaultMapOptions.put(KeywordExtractor.MAX_SCORE, "max keyword score allowed (smaller scores are better)");
        defaultMapOptions.put(KeywordExtractor.MAX_CONTENT_CHARS, "max number of input characters to process");
        defaultMapOptions.put(VIEW_NAMES, "a comma separated list of views to extract keywords from, in priority order");
    }

    private static final int MAX_CONTENT_ONE_HUNDRED_MB = 100 * 1024 * 1024;

    /**
     * A list of views we'll attempt to use for content from which to extract keywords, ordered based on preference.
     */
    protected final List<String> preferredViews = new ArrayList<>();

    /**
     * A map of dtUid to language, discovered via query parameters
     */
    protected final Map<String,String> documentLanguageMap = new HashMap<>();

    /** the underlying source */
    protected SortedKeyValueIterator<Key,Value> source;

    /** the options this iterator is configured to use */
    protected Map<String,String> iteratorOptions;

    /** The specified dt/uid column families */
    protected SortedSet<String> columnFamilies;

    /** inclusive or exclusive dt/uid column families */
    protected boolean inclusive;

    /** the underlying D column scan range */
    protected Range scanRange;

    /** max content length to send to the keyword extractor */
    protected int maxContentLength = MAX_CONTENT_ONE_HUNDRED_MB;

    /** the top key */
    protected Key tk;

    /** the top value */
    protected Value tv;

    @Override
    public boolean hasTop() {
        return tk != null;
    }

    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        KeywordExtractingIterator it = new KeywordExtractingIterator();

        it.preferredViews.clear();
        it.preferredViews.addAll(preferredViews);

        it.documentLanguageMap.clear();
        it.documentLanguageMap.putAll(documentLanguageMap);

        it.columnFamilies = new TreeSet<>(columnFamilies);
        it.iteratorOptions = new HashMap<>(iteratorOptions);

        it.inclusive = inclusive;
        it.scanRange = new Range(scanRange);
        it.tk = tk;
        it.tv = tv;

        return it;
    }

    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> iteratorOptions, IteratorEnvironment env) throws IOException {
        this.source = source;
        this.iteratorOptions = iteratorOptions;

        final String viewNames = iteratorOptions.getOrDefault(VIEW_NAMES, DEFAULT_VIEW_NAMES);
        if (viewNames.equals(DEFAULT_VIEW_NAMES)) {
            log.warn("No content view names for keyword extraction were specified in the iterator option {}, using defaults {} instead", VIEW_NAMES,
                            DEFAULT_VIEW_NAMES);
        }

        final String[] nameList = viewNames.split(Constants.COMMA);
        preferredViews.clear();
        preferredViews.addAll(List.of(nameList));

        // add entries passed in via iterator options to the document language map.
        if (iteratorOptions.containsKey(DOCUMENT_LANGUAGES)) {
            documentLanguageMap.clear();
            documentLanguageMap.putAll(deserializeMap(iteratorOptions.get(DOCUMENT_LANGUAGES)));
        }

        if (iteratorOptions.containsKey(KeywordExtractor.MAX_CONTENT_CHARS)) {
            maxContentLength = Integer.parseInt(iteratorOptions.get(KeywordExtractor.MAX_CONTENT_CHARS));
        }
    }

    @Override
    public Key getTopKey() {
        return tk;
    }

    @Override
    public Value getTopValue() {
        return tv;
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        if (log.isDebugEnabled()) {
            log.debug("{} seeking with requested range {}", this, range);
        }

        // capture the column families and the inclusiveness
        this.columnFamilies = columnFamilies != null ? getSortedCFs(columnFamilies) : Collections.emptySortedSet();
        this.inclusive = inclusive;
        // set our d keys scan range
        this.scanRange = range;

        if (log.isDebugEnabled()) {
            log.debug("{} seeking to: {} from requested range {}", this, this.scanRange, range);
        }

        // seek the underlying source
        source.seek(this.scanRange, D_COLUMN_FAMILY_BYTE_SEQUENCE, true);

        // get the next key
        next();
    }

    public void next() throws IOException {
        tk = null;
        tv = null;

        if (log.isTraceEnabled()) {
            log.trace("{} calling next on {}", source.hasTop(), scanRange);
        }

        // find a valid dt/uid (depends on initial column families set in seek call)
        String dtUid = getValidDtUid();

        // if no more d keys, then we are done.
        if (!source.hasTop() || dtUid == null) {
            return;
        }

        // store the content that we'll use for keyword extraction.
        final Map<String,VisibleContent> foundContent = new LinkedHashMap<>();
        Key top = source.getTopKey();

        // while we have d keys for the same document
        while (source.hasTop() && dtUid.equals(getDtUidFromDocumentKey(source.getTopKey()))) {
            top = source.getTopKey();
            Value value = source.getTopValue();
            String currentViewName = getViewName(top);
            String visibility = getVisibility(top);

            for (String name : preferredViews) {
                if (name.endsWith("*")) {
                    String truncatedName = name.substring(0, name.length() - 1);
                    if (currentViewName.startsWith(truncatedName)) {
                        addFoundContent(foundContent, currentViewName, value.get(), visibility);
                    }
                } else {
                    if (currentViewName.equals(name)) {
                        addFoundContent(foundContent, currentViewName, value.get(), visibility);
                    }
                }
            }

            // get the next d key
            source.next();
        }

        // extract keywords from the found content.
        String documentUid = getDocumentIdentifier(top.getRow().toString(), dtUid);
        String language = documentLanguageMap.get(documentUid);
        KeywordExtractor keywordExtractor = new KeywordExtractor(documentUid, preferredViews, foundContent, language, iteratorOptions);
        KeywordResults results = keywordExtractor.extractKeywords();

        if (results != EMPTY_RESULTS) {
            tk = top;
            tv = new Value(KeywordResults.serialize(results));
            return;
        }

        // If we get here, we have not found content for keyword extraction, so return null
        tk = null;
        tv = null;
    }

    private String getValidDtUid() throws IOException {
        String dtUid = null;
        while (source.hasTop() && dtUid == null) {
            Key top = source.getTopKey();
            String thisDtUid = getDtUidFromDocumentKey(top);
            // if this dt and uid are in the accepted column families...
            if (columnFamilies.contains(thisDtUid) == inclusive) {
                // we can use this document
                dtUid = thisDtUid;
            } else {
                seekToNextUid(top.getRow(), thisDtUid);
            }
        }
        return dtUid;
    }

    private void addFoundContent(Map<String,VisibleContent> foundContent, String currentViewName, byte[] encodedContent, String visibility) {
        final byte[] decodedContent = ContentKeyValueFactory.decodeAndDecompressContent(encodedContent);
        final String decodedString = new String(decodedContent, StandardCharsets.UTF_8);
        final String input = decodedString.substring(0, Math.min(decodedString.length(), maxContentLength));
        foundContent.put(currentViewName, new VisibleContent(visibility, input));
    }

    /**
     * Seek to the dt/uid following the one passed in
     *
     * @param row
     *            a row
     * @param dtAndUid
     *            the dt and uid string
     * @throws IOException
     *             for issues with read/write
     */
    private void seekToNextUid(Text row, String dtAndUid) throws IOException {
        Key startKey = new Key(row, Constants.D_COLUMN_FAMILY, new Text(dtAndUid + Constants.ONE_BYTE));
        this.scanRange = new Range(startKey, false, this.scanRange.getEndKey(), this.scanRange.isEndKeyInclusive());
        if (log.isDebugEnabled()) {
            log.debug("{} seeking to next document: {}", this, this.scanRange);
        }

        source.seek(this.scanRange, Collections.singleton(new ArrayByteSequence(Constants.D_COLUMN_FAMILY.getBytes())), true);
    }

    /**
     * Get the view name from the end of the column qualifier of the d key
     *
     * @param key
     *            the d key
     * @return the view name
     */
    protected String getViewName(Key key) {
        String cq = key.getColumnQualifier().toString();
        int index = cq.lastIndexOf(Constants.NULL);
        return cq.substring(index + 1);
    }

    /**
     * Get the visibility string from the key.
     *
     * @param key
     *            the d key
     * @return the view name
     */
    protected String getVisibility(Key key) {
        return key.getColumnVisibility().toString();
    }

    /**
     * get the datatype and uid from a d key
     *
     * @param key
     *            the d key
     * @return the datatype\x00uid
     */
    private static String getDtUidFromDocumentKey(Key key) {
        return getDtUid(key.getColumnQualifier().toString());
    }

    // get the dt/uid from the beginning of a given string
    protected static String getDtUid(String str) {
        int index = str.indexOf(Constants.NULL);
        index = str.indexOf(Constants.NULL, index + 1);
        return index == -1 ? str : str.substring(0, index);
    }

    /**
     * Turn a set of column families into a sorted string set
     *
     * @param columnFamilies
     *            the column families
     * @return a sorted set of column families as Strings
     */
    protected static SortedSet<String> getSortedCFs(Collection<ByteSequence> columnFamilies) {
        return columnFamilies.stream().map(m -> {
            try {
                return Text.decode(m.getBackingArray(), m.offset(), m.length());
            } catch (CharacterCodingException e) {
                throw new RuntimeException(e);
            }
        }).collect(Collectors.toCollection(TreeSet::new));
    }

    protected static String getDocumentIdentifier(String row, String dtUid) {
        return row + "/" + dtUid.replace("\0", "/");
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + ": " + preferredViews;
    }

    @Override
    public IteratorOptions describeOptions() {
        return new IteratorOptions("keywordExtractor", "Extracts keywords from content stored in the Datawave 'd' column", defaultMapOptions, null);
    }

    public boolean validateOptions(Map<String,String> options) {
        boolean valid = (options == null || options.isEmpty());
        if (!valid) {
            try {
                validateIntOption(KeywordExtractor.MIN_NGRAMS, options);
                validateIntOption(KeywordExtractor.MAX_NGRAMS, options);
                validateIntOption(KeywordExtractor.MAX_KEYWORDS, options);
                validateFloatOption(KeywordExtractor.MAX_SCORE, options);
                validateIntOption(KeywordExtractor.MAX_CONTENT_CHARS, options);
                valid = true;
            } catch (Exception e) {
                if (log.isDebugEnabled()) {
                    log.debug("Validate options failed: {}", e.getMessage());
                }
            }
        }
        return valid;
    }

    public static void setOptions(IteratorSetting si, int minNgrams, int maxNgrams, int maxKeywords, float maxScore, int maxContentChars,
                    List<String> viewNames, Map<String,String> documentLanguageMap) {

        if (minNgrams > 0) {
            si.addOption(KeywordExtractor.MIN_NGRAMS, String.valueOf(minNgrams));
        }

        if (maxNgrams > 0) {
            si.addOption(KeywordExtractor.MAX_NGRAMS, String.valueOf(maxNgrams));
        }

        if (maxKeywords > 0) {
            si.addOption(KeywordExtractor.MAX_KEYWORDS, String.valueOf(maxKeywords));
        }

        if (maxScore > 0) {
            si.addOption(KeywordExtractor.MAX_SCORE, String.valueOf(maxScore));
        }

        if (maxContentChars > 0) {
            si.addOption(KeywordExtractor.MAX_CONTENT_CHARS, String.valueOf(maxContentChars));
        }

        si.addOption(VIEW_NAMES, String.join(",", viewNames));
        si.addOption(DOCUMENT_LANGUAGES, serializeMap(documentLanguageMap));
    }

    private static final Gson gson = new Gson();
    private static final TypeToken<Map<String,String>> typeToken = new TypeToken<>() {};

    private static String serializeMap(Map<String,String> map) {
        return gson.toJson(map);
    }

    public static Map<String,String> deserializeMap(String string) {
        return gson.fromJson(string, typeToken.getType());
    }

    @SuppressWarnings("unused")
    private static void validateIntOption(String name, Map<String,String> options) {
        if (options.containsKey(name)) {
            int i = Integer.parseInt(options.get(name));
        }
    }

    @SuppressWarnings({"unused", "SameParameterValue"})
    private static void validateFloatOption(String name, Map<String,String> options) {
        if (options.containsKey(name)) {
            float f = Float.parseFloat(options.get(name));
        }
    }

    @SuppressWarnings({"unused", "SameParameterValue"})
    private static void validateBooleanOption(String name, Map<String,String> options) {
        if (options.containsKey(name)) {
            String value = options.get(name);
            if (value.equalsIgnoreCase("true") || value.equalsIgnoreCase("false")) {
                return;
            }
            throw new IllegalArgumentException(value + " does not equal 'true' or 'false'");
        }
    }

}
