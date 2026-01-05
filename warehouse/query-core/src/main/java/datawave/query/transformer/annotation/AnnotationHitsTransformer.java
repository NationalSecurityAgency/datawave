package datawave.query.transformer.annotation;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

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
    public static final String KEYWORD_DELIMITER = ";";

    private static final boolean DEFAULT_ENABLED = false;
    private static final int DEFAULT_CONTEXT_SIZE = 3;
    private static final float DEFAULT_MIN_SCORE = 0;

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
            this.enabled = Boolean.parseBoolean(enabledStr);
        }
        // go no further if not enabled, searchHitTerms will be null so the transformer will never do anything
        if (!this.enabled) {
            return;
        }

        String contextBoundaryStr = settings.findParameter(CONTEXT_SIZE_PARAMETER).getParameterValue();
        if (!contextBoundaryStr.isBlank()) {
            this.contextSize = Integer.parseInt(contextBoundaryStr);
        }
        if (this.contextSize > this.maxContextBoundary) {
            log.warn("contextBoundary requested: " + this.contextSize + " max configured: " + this.maxContextBoundary + " Automatically reducing to max");
            this.contextSize = this.maxContextBoundary;
            log.info("all hits contextSize: " + this.contextSize);
        } else if (this.contextSize < 0) {
            log.warn("contextBoundary requested: " + this.contextSize + " below min context: 0 Automatically increasing to min");
            this.contextSize = 0;
        }

        String minScoreStr = settings.findParameter(MIN_SCORE_PARAMETER).getParameterValue();
        if (!minScoreStr.isBlank()) {
            this.minScore = Float.parseFloat(minScoreStr);
            log.info("all hits minScore: " + this.minScore);
        }

        this.objectMapper = new ObjectMapper();

        String keywordStr = settings.findParameter(KEYWORDS_PARAMETER).getParameterValue();
        if (!keywordStr.isBlank()) {
            // check for json
            String[] keywords;
            try {
                // decode the string
                String decoded = URLDecoder.decode(keywordStr, StandardCharsets.UTF_8);
                // convert from json
                keywords = this.objectMapper.readValue(decoded, String[].class);
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
        // extract terms to lookup hits on if they haven't been extracted yet
        if (this.searchHitTerms == null) {
            try {
                this.searchHitTerms = extractSearchHitTerms(this.shardQueryConfig, this.settings);
            } catch (ParseException e) {
                log.debug("no valid search terms detected for query, skipping all hits");
            }
        }

        if (!this.enabled || this.searchHitTerms.isEmpty()) {
            // no search terms, no-op
            return keyDocumentEntry;
        }

        Key key = keyDocumentEntry.getKey();
        if (key != null) {
            String shard = key.getRow().toString();
            String cf = key.getColumnFamily().toString();
            String[] parts = cf.split("\u0000");
            if (parts.length == 2) {
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
                            AllHits results = allHitsFactory.create(annotation.getAnnotationId(), orderedHits, sortedSegments);
                            updateDocument(keyDocumentEntry, results);
                        } catch (AllHitsException e) {
                            log.warn("failed to process hit(s) on annotation: " + annotation.getAnnotationId() + " for doc: " + dataType + "\\x00" + uid, e);
                            AllHitsError error = new AllHitsError();
                            error.setAnnotationId(annotation.getAnnotationId());
                            error.setErrorMessage(e.getMessage());
                            updateDocument(keyDocumentEntry, error);
                        }
                    } else {
                        // TODO
                    }
                }
            } else {
                // TODO
            }
        } else {
            // TODO
        }

        return keyDocumentEntry;
    }

    private List<AllHits> getCurrentAllHitsValue(Entry<Key,Document> entry) {
        Content attr = (Content) entry.getValue().get(this.targetField);
        List<AllHits> rollup = null;
        if (attr != null) {
            try {
                rollup = this.objectMapper.readValue(attr.getContent(), new TypeReference<>() {});
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }

        if (rollup == null) {
            rollup = new ArrayList<>();
        }

        return rollup;
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
            Collections.sort(segmentValues, new SegmentValueComparator());
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
