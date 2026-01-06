package datawave.query.transformer.annotation;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import org.apache.accumulo.core.data.Key;
import org.apache.commons.jexl3.parser.ASTEQNode;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ParseException;
import org.apache.log4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import datawave.annotation.data.v1.AnnotationDataAccess;
import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.protobuf.v1.Segment;
import datawave.annotation.protobuf.v1.SegmentBoundary;
import datawave.annotation.protobuf.v1.SegmentValue;
import datawave.marking.MarkingFunctions;
import datawave.microservice.query.Query;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Content;
import datawave.query.attributes.Document;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.transformer.DocumentTransform;
import datawave.query.transformer.annotation.model.AllHits;
import datawave.query.transformer.annotation.model.AllHitsError;

/**
 * This iterator will lookup and search annotations for hits as well as provide context
 */
public class AnnotationHitsTransformer extends DocumentTransform.DefaultDocumentTransform {
    private static final Logger log = Logger.getLogger(AnnotationHitsTransformer.class);

    public static final String ENABLED_PARAMETER = "annotation.all.hits";
    public static final String CONTEXT_SIZE_PARAMETER = "annotation.all.hits.contextSize";
    public static final String MIN_SCORE_PARAMETER = "annotation.all.hits.minScore";
    public static final String KEYWORDS_PARAMETER = "annotation.all.hits.keywords";
    public static final String TIMEUNIT_PARAMETER = "annotation.all.hits.timeunit";
    public static final String KEYWORD_DELIMITER = ";";

    private static final boolean DEFAULT_ENABLED = false;
    private static final int DEFAULT_CONTEXT_SIZE = 3;
    private static final float DEFAULT_MIN_SCORE = 0;
    private static final TimeUnit DEFAULT_TIMEUNIT = TimeUnit.MILLISECONDS;
    private static final SegmentValueByScoreComparator SEGMENT_VALUE_BY_SCORE_COMPARATOR = new SegmentValueByScoreComparator();

    private final ShardQueryConfiguration shardQueryConfig;
    private final AnnotationDataAccess annotationDataAccess;
    private final AllHitsFactory allHitsFactory;
    private final int maxContextBoundary;
    private final Set<String> validTypes;
    private final Set<String> validQueryFields;
    private final String targetField;

    private boolean enabled = DEFAULT_ENABLED;
    private int contextSize = DEFAULT_CONTEXT_SIZE;
    private float minScore = DEFAULT_MIN_SCORE;
    private TimeUnit timeUnit = DEFAULT_TIMEUNIT;

    private Set<String> searchHitTerms;
    private ObjectMapper objectMapper;

    public AnnotationHitsTransformer(ShardQueryConfiguration shardQueryConfig, AnnotationDataAccess annotationDataAccess, AllHitsFactory allHitsFactory,
                    int maxContextBoundary, Set<String> validTypes, Set<String> validQueryFields, String targetField) {
        this.shardQueryConfig = shardQueryConfig;
        this.annotationDataAccess = annotationDataAccess;
        this.allHitsFactory = allHitsFactory;
        this.maxContextBoundary = maxContextBoundary;
        this.validTypes = validTypes;
        this.validQueryFields = validQueryFields;
        this.targetField = targetField;
    }

    @Override
    public void initialize(Query settings, MarkingFunctions markingFunctions) {
        super.initialize(settings, markingFunctions);

        // handle query parameters for configuration overrides
        String enabledStr = settings.findParameter(ENABLED_PARAMETER).getParameterValue();
        if (!enabledStr.isBlank()) {
            enabled = Boolean.parseBoolean(enabledStr);
        }
        // go no further if not enabled, searchHitTerms will be null so the transformer will never do anything
        if (!enabled) {
            return;
        }

        String contextBoundaryStr = settings.findParameter(CONTEXT_SIZE_PARAMETER).getParameterValue();
        if (!contextBoundaryStr.isBlank()) {
            contextSize = Integer.parseInt(contextBoundaryStr);
        }
        if (contextSize > maxContextBoundary) {
            log.warn("contextBoundary requested: " + contextSize + " max configured: " + maxContextBoundary + " Automatically reducing to max");
            contextSize = maxContextBoundary;
            log.info("all hits contextSize: " + contextSize);
        } else if (contextSize < 0) {
            log.warn("contextBoundary requested: " + contextSize + " below min context: 0 Automatically increasing to min");
            contextSize = 0;
        }

        String minScoreStr = settings.findParameter(MIN_SCORE_PARAMETER).getParameterValue();
        if (!minScoreStr.isBlank()) {
            minScore = Float.parseFloat(minScoreStr);
            log.info("all hits minScore: " + minScore);
        }

        String timeUnitStr = settings.findParameter(TIMEUNIT_PARAMETER).getParameterValue();
        if (!timeUnitStr.isBlank()) {
            timeUnit = TimeUnit.valueOf(timeUnitStr);
            log.info("all hits timeUnit: " + timeUnit);
        }

        objectMapper = new ObjectMapper();

        String keywordStr = settings.findParameter(KEYWORDS_PARAMETER).getParameterValue();
        if (!keywordStr.isBlank()) {
            // check for json
            String[] keywords;
            try {
                // decode the string
                String decoded = URLDecoder.decode(keywordStr, StandardCharsets.UTF_8);
                // convert from json
                keywords = objectMapper.readValue(decoded, String[].class);
            } catch (JsonProcessingException e) {
                log.info("keywordStr provided, but not json, falling back to ; delimited parsing for: " + keywordStr);
                // basic parsing
                keywords = keywordStr.split(KEYWORD_DELIMITER);
            }
            searchHitTerms = new HashSet<>();
            searchHitTerms.addAll(Arrays.asList(keywords));
        }

    }

    /**
     * Lookup the annotation for a document and enrich all hits if it exists
     *
     * @param keyDocumentEntry
     *            the function argument
     * @return
     */
    @Nullable
    @Override
    public Entry<Key,Document> apply(@Nullable Entry<Key,Document> keyDocumentEntry) {
        // check null and enabled status
        if (keyDocumentEntry == null || !enabled) {
            return keyDocumentEntry;
        }

        // extract terms to lookup hits on if they haven't been extracted yet
        if (searchHitTerms == null) {
            try {
                searchHitTerms = extractSearchHitTerms(shardQueryConfig, settings);
            } catch (ParseException e) {
                log.debug("no valid search terms detected for query, skipping all hits");
            }
        }

        if (searchHitTerms.isEmpty()) {
            // no search terms, no-op
            return keyDocumentEntry;
        }

        Key key = keyDocumentEntry.getKey();
        Document document = keyDocumentEntry.getValue();
        if (key == null || document == null || !document.isToKeep()) {
            // either missing information that is critical or is transient
            return keyDocumentEntry;
        } else if (document.get(targetField) != null) {
            log.warn("Document: " + key + " already contains field: " + targetField + " skipping");
            return keyDocumentEntry;
        }

        String shard = key.getRow().toString();
        String cf = key.getColumnFamily().toString();
        String[] parts = cf.split("\u0000");

        if (parts.length != 2) {
            // unexpected doc key
            log.warn("Cannot apply all hits to result. Unexpected doc key: " + key);
            return keyDocumentEntry;
        }

        String dataType = parts[0];
        String uid = parts[1];
        List<Annotation> annotations = annotationDataAccess.getAnnotations(shard, dataType, uid);
        for (Annotation annotation : annotations) {
            String annotationType = annotation.getAnnotationType();
            if (validTypes.contains(annotationType)) {
                // this annotation supports allHits
                TreeMap<SegmentBoundary,List<SegmentValue>> sortedSegments = sort(annotation.getSegmentsList());
                List<SegmentHit> orderedHits = search(sortedSegments, contextSize, minScore);
                try {
                    AllHits results = allHitsFactory.create(annotation.getAnnotationId(), orderedHits, sortedSegments, timeUnit);
                    updateDocument(keyDocumentEntry, results);
                } catch (AllHitsException e) {
                    log.warn("failed to process hit(s) on annotation: " + annotation.getAnnotationId() + " for doc: " + dataType + "\\x00" + uid, e);
                    AllHitsError error = new AllHitsError();
                    error.setAnnotationId(annotation.getAnnotationId());
                    error.setErrorMessage(e.getMessage());
                    updateDocument(keyDocumentEntry, error);
                }
            }
        }

        return keyDocumentEntry;
    }

    private void updateDocument(Entry<Key,Document> entry, AllHits allHits) {
        List<AllHits> rollup = getCurrentAllHitsValue(entry);
        rollup.add(allHits);

        // convert pojo to json
        String json = null;
        try {
            json = objectMapper.writeValueAsString(rollup);
        } catch (JsonProcessingException e) {
            log.warn("Failed to write json for all hits for document: " + entry.getKey(), e);
        }

        if (json != null) {
            // update the document
            entry.getValue().replace(targetField, new Content(json, entry.getKey(), true), false);
        }
    }

    /**
     * Deserialize the current targetField value from json to an object
     *
     * @param entry
     *            the entry to extract any previously generated AllHits from
     * @return the extracted non-null list of AllHits, or an empty List
     */
    private List<AllHits> getCurrentAllHitsValue(Entry<Key,Document> entry) {
        Attribute<?> attr = entry.getValue().get(targetField);
        List<AllHits> rollup = new ArrayList<>();

        if (attr == null) {
            return rollup;
        }

        if (!(attr instanceof Content)) {
            // targetField has to not exist before handling annotation hits. Seeing this means something has happened internally to this code that was
            // unexpected
            log.error("Unexpected Attribute in: " + entry.getKey() + " targetField: " + targetField + " is type " + attr.getClass().getCanonicalName()
                            + " with value " + attr.getData() + " will be overwritten");
            return rollup;
        }

        // must be Content
        Content content = (Content) attr;
        try {
            rollup = objectMapper.readValue(content.getContent(), new TypeReference<>() {});
        } catch (JsonProcessingException e) {
            log.error("Failed to process existing AllHits as json: " + content.getContent(), e);
        }

        return rollup;
    }

    /**
     * sort both the segments and values
     *
     * @param segments
     * @return
     */
    private TreeMap<SegmentBoundary,List<SegmentValue>> sort(List<Segment> segments) {
        TreeMap<SegmentBoundary,List<SegmentValue>> orderedSegments = new TreeMap<>(new BoundaryComparator());
        for (Segment segment : segments) {
            // make a copy so they can be sorted
            List<SegmentValue> segmentValues = new ArrayList<>(segment.getValuesList());
            segmentValues.sort(SEGMENT_VALUE_BY_SCORE_COMPARATOR);
            orderedSegments.put(segment.getBoundary(), segmentValues);
        }

        return orderedSegments;
    }

    /**
     * Make a single pass through the sortedSegments finding matches and creating context for each hit
     *
     * @param sortedSegments
     *            the sorted set of all SegmentBoundary to consider for the search. All SegmentValue will appear in ascending order
     * @param contextSize
     *            the window of adjacent terms +/- from the hit term to include in the hit
     * @param minScore
     *            the minimum score a term must have to register a hit
     * @return non-null List of hits ordered by the segmentBoundary they hit on. Hit order guaranteed to be ascending SegmentBoundary, no second order sort is
     *         applied. Hits for the same SegmentBoundary will appear in the order they were found.
     */
    private List<SegmentHit> search(TreeMap<SegmentBoundary,List<SegmentValue>> sortedSegments, int contextSize, float minScore) {
        // keep a list of recent boundaries for context
        // window has to include context + 1 so that on the hit it still has the full window available
        int maxWindow = contextSize + 1;
        ArrayDeque<SegmentBoundary> window = new ArrayDeque<>(maxWindow);

        final Iterator<SegmentBoundary> itr = sortedSegments.navigableKeySet().iterator();
        List<SegmentHit> finishedHits = new ArrayList<>();
        Map<Integer,List<SegmentHit>> partialHits = new HashMap<>();
        int segmentIndex = 0;
        // track the last segment boundary for end conditions
        SegmentBoundary last = null;
        while (itr.hasNext()) {
            SegmentBoundary boundary = itr.next();

            // update context window
            if (window.size() == maxWindow) {
                window.removeFirst();
            }
            window.add(boundary);

            List<SegmentValue> values = sortedSegments.get(boundary);
            for (int i = 0; i < values.size(); i++) {
                SegmentValue segmentValue = values.get(i);
                String normalizedTerm = normalize(segmentValue.getValue());
                if (segmentValue.getScore() >= minScore && searchHitTerms.contains(normalizedTerm)) {
                    // partial hits index is the location in the window where the hit is complete
                    List<SegmentHit> hits = partialHits.computeIfAbsent(segmentIndex + contextSize, x -> new ArrayList<>());
                    hits.add(new SegmentHit(window.getFirst(), boundary, i));
                }
            }

            // check partial hits for the end of their context window
            if (partialHits.containsKey(segmentIndex)) {
                List<SegmentHit> hits = partialHits.get(segmentIndex);
                for (SegmentHit hit : hits) {
                    hit.setContextEnd(boundary);
                }
                partialHits.remove(segmentIndex);
                finishedHits.addAll(hits);
            }

            segmentIndex++;
            last = boundary;
        }

        // close out any remaining hits with the last
        for (List<SegmentHit> hits : partialHits.values()) {
            for (SegmentHit hit : hits) {
                hit.setContextEnd(last);
            }
            finishedHits.addAll(hits);
        }

        // clean up
        partialHits.clear();
        window.clear();

        // finished hits now should include hits and boundaries
        return finishedHits;
    }

    private Set<String> extractSearchHitTerms(ShardQueryConfiguration shardQueryConfig, Query settings) throws ParseException {
        Set<String> searchTerms = new HashSet<>();

        if (shardQueryConfig != null && shardQueryConfig.getQueryTree() != null) {
            ASTJexlScript script = shardQueryConfig.getQueryTree();
            List<ASTEQNode> eqNodes = JexlASTHelper.getEQNodes(script);
            if (!eqNodes.isEmpty()) {
                for (ASTEQNode eqNode : eqNodes) {
                    String identifier = JexlASTHelper.getIdentifier(eqNode);
                    if (identifier != null && validQueryFields.contains(identifier)) {
                        Object literal = JexlASTHelper.getLiteralValue(eqNode);
                        if (literal != null) {
                            // simple normalization for exact string matches only
                            searchTerms.add(normalize(literal.toString()));
                        }
                    }
                }
            }
        }

        return searchTerms;
    }

    private String normalize(String toNormalize) {
        return toNormalize.toLowerCase().trim();
    }

    public static class SegmentHit {
        private final SegmentBoundary contextStart;
        private final SegmentBoundary hitBoundary;
        private final int valueHitIndex;

        private SegmentBoundary contextEnd;

        public SegmentHit(SegmentBoundary contextStart, SegmentBoundary hitBoundary, int valueHitIndex) {
            this.contextStart = contextStart;
            this.hitBoundary = hitBoundary;
            this.valueHitIndex = valueHitIndex;
        }

        public SegmentBoundary getContextStart() {
            return contextStart;
        }

        public SegmentBoundary getHitBoundary() {
            return hitBoundary;
        }

        public int getValueHitIndex() {
            return valueHitIndex;
        }

        public void setContextEnd(SegmentBoundary contextEnd) {
            this.contextEnd = contextEnd;
        }

        public SegmentBoundary getContextEnd() {
            return contextEnd;
        }
    }
}
