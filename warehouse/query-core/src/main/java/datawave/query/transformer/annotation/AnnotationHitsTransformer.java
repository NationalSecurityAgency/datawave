package datawave.query.transformer.annotation;

import static datawave.query.QueryParameters.INCLUDE_GROUPING_CONTEXT;
import static datawave.query.QueryParameters.RETURN_FIELDS;

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
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

import org.apache.accumulo.core.data.Key;
import org.apache.commons.jexl3.parser.ParseException;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;

import datawave.annotation.data.v1.AnnotationDataAccess;
import datawave.annotation.protobuf.v1.Annotation;
import datawave.annotation.protobuf.v1.AnnotationSource;
import datawave.annotation.protobuf.v1.Segment;
import datawave.annotation.protobuf.v1.SegmentBoundary;
import datawave.annotation.protobuf.v1.SegmentValue;
import datawave.data.normalizer.Normalizer;
import datawave.marking.MarkingFunctions;
import datawave.microservice.query.Query;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Content;
import datawave.query.attributes.Document;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.function.RemoveGroupingContext;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.parser.JavaRegexAnalyzer;
import datawave.query.transformer.DocumentTransform;
import datawave.query.transformer.annotation.model.AllHits;
import datawave.query.util.Tuple2;

/**
 * This Transform will lookup and search annotations for hits as well as provide context
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
    private static final int DEFAULT_CONTEXT_SIZE = 10;
    private static final float DEFAULT_MIN_SCORE = 0;
    private static final TimeUnit DEFAULT_TIMEUNIT = TimeUnit.MILLISECONDS;
    private static final SegmentValueByScoreComparator SEGMENT_VALUE_BY_SCORE_COMPARATOR = new SegmentValueByScoreComparator();
    private static final BoundaryComparator BOUNDARY_COMPARATOR = new BoundaryComparator();

    /**
     * Depending on how the query is configured to run by the user, adjustments may need to be made to forcibly enable grouping notation and return fields so
     * that this transformer will have all the data necessary to fully enrichment responses. These changes will be made transparently to the user, and removed
     * before the final Document is returned from the transformer. This is only necessary if enrichmentFieldMap is populated.
     */
    private final ShardQueryConfiguration shardQueryConfig;
    private final AnnotationDataAccess annotationDataAccess;
    private final AllHitsFactory allHitsFactory;
    private final int maxContextBoundary;
    private final Set<String> validTypes;
    private final String targetField;
    private final TermExtractor queryTermExtractor;
    private final Normalizer<String> termNormalizer;
    private final String jexlQueryString;
    /**
     * Used for merging data about an annotation that may have been stored in the event with hits against that annotation. The key represents the field that
     * should be searched in the Document. The value is the name of the field to store in the AllHits dynamicFields when found. For each AllHits object that is
     * generated, check the Document for the presence of these fields and add them to the AllHits response.
     */
    private final Map<String,String> enrichmentFieldMap;

    private boolean enabled = DEFAULT_ENABLED;
    private int contextSize = DEFAULT_CONTEXT_SIZE;
    private float minScore = DEFAULT_MIN_SCORE;
    private TimeUnit timeUnit = DEFAULT_TIMEUNIT;
    private boolean forcedGroupingNotation = false;
    private List<String> forcedReturnFields = new ArrayList<>();

    private Set<Pattern> searchHitTerms;
    private ObjectMapper objectMapper;

    public AnnotationHitsTransformer(ShardQueryConfiguration shardQueryConfig, @Nullable String jexlQueryString, TermExtractor queryTermExtractor,
                    Normalizer<String> termNormalizer, AnnotationDataAccess annotationDataAccess, AllHitsFactory allHitsFactory, int maxContextBoundary,
                    Set<String> validTypes, String targetField, Map<String,String> enrichmentFieldMap) {
        this.shardQueryConfig = shardQueryConfig;
        this.jexlQueryString = jexlQueryString;
        this.queryTermExtractor = queryTermExtractor;
        this.termNormalizer = termNormalizer;
        this.annotationDataAccess = annotationDataAccess;
        this.allHitsFactory = allHitsFactory;
        this.maxContextBoundary = maxContextBoundary;
        this.validTypes = validTypes;
        this.targetField = targetField;
        this.enrichmentFieldMap = enrichmentFieldMap;
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

        if (queryTermExtractor == null || termNormalizer == null) {
            throw new IllegalStateException("queryTermExtractor and termNormalizer must be set");
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

            if (minScore < 0) {
                log.warn("minScore set below 0, adjusting to 0");
                minScore = 0;
            } else if (minScore > 1) {
                log.warn("minScore set above 1, adjusting to 1");
                minScore = 1;
            }
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
            for (String keyword : keywords) {
                searchHitTerms.add(compileNormalized(termNormalizer.normalize(keyword)));
            }
        }

        // test for changes that need to be made to the query to support field enrichment from the event
        if (enrichmentFieldMap != null && !enrichmentFieldMap.isEmpty()) {
            String groupingParameter = settings.findParameter(INCLUDE_GROUPING_CONTEXT).getParameterValue();
            if ((!Boolean.parseBoolean(groupingParameter))) {
                // grouping notation not set, apply it
                shardQueryConfig.setIncludeGroupingContext(true);
                // capture this, so it can be undone after the transform is complete
                forcedGroupingNotation = true;
            }

            // now check that if return.fields are set that the fields required are included
            String returnFieldsParameter = settings.findParameter(RETURN_FIELDS).getParameterValue();
            if (returnFieldsParameter != null && !returnFieldsParameter.isBlank()) {
                // parse the return fields to get the list of missing return fields
                List<String> missingReturnFields = getMissingReturnFields(returnFieldsParameter);

                if (!missingReturnFields.isEmpty()) {
                    Set<String> updatedProjectFields = new HashSet<>(shardQueryConfig.getProjectFields());
                    updatedProjectFields.addAll(missingReturnFields);
                    shardQueryConfig.setProjectFields(updatedProjectFields);
                    // capture this to undo what was forced into the query to satisfy the response after processing
                    forcedReturnFields = missingReturnFields;
                }
            }
        }
    }

    private List<String> getMissingReturnFields(String returnFieldsParameter) {
        String[] returnFields = returnFieldsParameter.split(",");
        // build a list of all the missing return fields that are in the enrichment map
        List<String> missingReturnFields = new ArrayList<>();
        for (String eventField : enrichmentFieldMap.keySet()) {
            boolean found = false;
            for (String returnField : returnFields) {
                if (eventField.equals(returnField)) {
                    found = true;
                    break;
                }
            }

            if (!found) {
                missingReturnFields.add(eventField);
            }
        }
        return missingReturnFields;
    }

    /**
     * All patterns should be compiled case-insensitive and unicode-case even if previously normalized
     *
     * @param normalized
     * @return
     */
    private Pattern compileNormalized(String normalized) {
        return Pattern.compile(normalized, Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE);
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
                searchHitTerms = new HashSet<>();
                for (String normalized : queryTermExtractor.extract(jexlQueryString, termNormalizer)) {
                    searchHitTerms.add(compileNormalized(normalized));
                }
            } catch (ParseException | JavaRegexAnalyzer.JavaRegexParseException e) {
                log.debug("no valid search terms detected for query, skipping all hits", e);
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
                    AllHits results = null;
                    if (!orderedHits.isEmpty()) {
                        results = allHitsFactory.create(annotation.getAnnotationId(), orderedHits, sortedSegments, timeUnit);
                    }
                    enrichAllHitsFromDocument(annotation, results, document);
                    updateDocument(keyDocumentEntry, results);
                } catch (AllHitsException e) {
                    log.warn("failed to process hit(s) on annotation: " + annotation.getAnnotationId() + " for doc: " + dataType + "\\x00" + uid, e);
                    AllHits error = new AllHits();
                    error.setAnnotationId(annotation.getAnnotationId());
                    error.addDynamicProperties("error", e.getMessage());
                    updateDocument(keyDocumentEntry, error);
                } finally {
                    // strip anything we forced into the Document to complete the query
                    keyDocumentEntry = stripGroupingNotation(keyDocumentEntry);
                    keyDocumentEntry = removeForcedFields(keyDocumentEntry);
                }
            }
        }

        return keyDocumentEntry;
    }

    private Entry<Key,Document> removeForcedFields(Entry<Key,Document> entry) {
        if (forcedReturnFields.isEmpty()) {
            return entry;
        }

        Set<Tuple2<String,Attribute<? extends Comparable<?>>>> toRemove = Sets.newHashSet();
        for (Entry<String,Attribute<? extends Comparable<?>>> attribute : entry.getValue().entrySet()) {
            String fieldName = attribute.getKey();
            String baseFieldName = JexlASTHelper.deconstructIdentifier(fieldName);
            if (forcedReturnFields.contains(baseFieldName)) {
                toRemove.add(new Tuple2<>(attribute.getKey(), attribute.getValue()));
            }
        }

        // remove everyone with a grouping context
        for (Tuple2<String,Attribute<? extends Comparable<?>>> goner : toRemove) {
            entry.getValue().removeAll(goner.first());
        }

        return entry;
    }

    private Entry<Key,Document> stripGroupingNotation(Entry<Key,Document> entry) {
        if (!forcedGroupingNotation) {
            return entry;
        }

        RemoveGroupingContext removeGroupingContext = new RemoveGroupingContext();
        return removeGroupingContext.apply(entry);
    }

    private void enrichAllHitsFromDocument(Annotation annotation, AllHits allHits, Document document) {
        if (allHits == null || enrichmentFieldMap.isEmpty()) {
            return;
        }

        // this operation may cause an accumulo lookup
        Optional<AnnotationSource> optionalAnnotationSource = annotationDataAccess.getAnnotationSource(annotation.getAnalyticSourceHash());
        if (optionalAnnotationSource.isEmpty()) {
            log.info("could not enrich from event due to missing annotationSource for annotation:" + annotation.getAnnotationId() + " for doc:"
                            + annotation.getShard() + " " + annotation.getDataType() + " " + annotation.getUid());
            return;
        }

        // this hash will match the beginning of grouping notation generated for related fields in the event
        String annotationSourceHash = optionalAnnotationSource.get().getAnalyticHash();

        // since the document will be in grouping notation, there will not be an exact match on a field, iterate over the Document fields to find those that are
        // candidates
        for (String docField : document.getDictionary().keySet()) {
            String baseFieldName = JexlASTHelper.deconstructIdentifier(docField, false);
            if (!enrichmentFieldMap.containsKey(baseFieldName)) {
                // the base name isn't in the enrichment field map skip to the next one
                continue;
            }

            String[] groups = docField.split("\\.");
            // all annotation fields will be in grouping notation of the form FIELD.annotationHash.segmentHash.valueHash
            if (groups.length != 4) {
                // doesn't match the required notation of the annotation enriched fields
                continue;
            }

            // check the position of the analytic hash with the target value
            if (!groups[1].equals(annotationSourceHash)) {
                // doesn't match the annotation source hash from the annotation, not the target value
                continue;
            }

            // get any pre-existing value in the map
            String[] existingSplits = null;
            String existing = allHits.getDynamicProperties().get(enrichmentFieldMap.get(baseFieldName));
            if (existing != null && !existing.isBlank()) {
                existingSplits = existing.split(";");
            }

            // extract the value for enrichment
            Attribute<?> attr = document.get(docField);
            List<String> values = new ArrayList<>();
            if (existingSplits != null) {
                values.addAll(Arrays.asList(existingSplits));
            }
            boolean updated = false;
            if (attr instanceof Attributes) {
                // multi-valued
                Attributes attrs = (Attributes) attr;
                Set<Attribute<? extends Comparable<?>>> attrSet = attrs.getAttributes();

                for (Attribute<? extends Comparable<?>> value : attrSet) {
                    values.add(String.valueOf(value.getData()));
                }
                updated = true;
            } else if (attr != null) {
                // single value
                values.add(String.valueOf(attr.getData()));
                updated = true;
            }

            if (updated) {
                allHits.addDynamicProperties(enrichmentFieldMap.get(baseFieldName), StringUtils.join(values, ";"));
            }
        }
    }

    private void updateDocument(Entry<Key,Document> entry, @Nullable AllHits allHits) {
        List<AllHits> rollup = getCurrentAllHitsValue(entry);
        if (allHits != null) {
            rollup.add(allHits);
        }

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
        TreeMap<SegmentBoundary,List<SegmentValue>> orderedSegments = new TreeMap<>(BOUNDARY_COMPARATOR);
        for (Segment segment : segments) {
            // make a copy so they can be sorted
            List<SegmentValue> segmentValues = new ArrayList<>(segment.getValuesList());
            segmentValues.sort(SEGMENT_VALUE_BY_SCORE_COMPARATOR);
            orderedSegments.put(segment.getBoundary(), segmentValues);
        }

        return orderedSegments;
    }

    /**
     * Make a single pass through the sortedSegments finding matches and creating context for each hit. A queue will hold the last contextSize SegmentBoundary
     * along with the current SegmentBoundary. When a searchHitTerm is matched a SegmentHit will be created using the first SegmentBoundary in the queue, and
     * the current SegmentBoundary along with the index of the hit in the SegmentValue list. This hit is then added to the partialHits map using the current
     * SegmentBoundary index plus the contextSize. This sets a future point to complete the SegmentHit context window after the hit. Each iteration checks for
     * any partial SegmentHits which may be completing on this SegmentBoundary (start, hit, and end) by looking for anything in the partialHits map which
     * matches the current index. After moving through all SegmentBoundary anything remaining in the partialHits map will not have a full contextSize after the
     * hit since there weren't enough remaining Segments, but should instead have their end SegmentBoundary set to the last SegmentBoundary seen.
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
        // a list of SegmentHits which have their end context set
        List<SegmentHit> finishedHits = new ArrayList<>();
        // a map from the segmentIndex associated SegmentHits will have their end context set
        Map<Integer,List<SegmentHit>> partialHits = new HashMap<>();
        // track which segment index is currently being processed
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
                if (segmentValue.getScore() >= minScore && matchesSearchTerm(segmentValue.getValue())) {
                    // partial hits index is the location in the window where the hit is complete
                    List<SegmentHit> hits = partialHits.computeIfAbsent(segmentIndex + contextSize, ArrayList::new);
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

    private boolean matchesSearchTerm(String term) {
        for (Pattern searchPattern : searchHitTerms) {
            String normalized = termNormalizer.normalize(term);
            Matcher matcher = searchPattern.matcher(normalized);
            if (matcher.matches()) {
                return true;
            }
        }

        return false;
    }

    /**
     * Used to track a keyword hit and its beginning and ending context SegmentBoundary
     */
    public static class SegmentHit {
        private final SegmentBoundary contextStart;
        private final SegmentBoundary hitBoundary;
        private final int valueHitIndex;

        private SegmentBoundary contextEnd;

        /**
         * Create a new partial SegmentHit
         *
         * @param contextStart
         *            the SegmentBoundary which marks the beginning of this hits context
         * @param hitBoundary
         *            the SegmentBoundary containing the hit
         * @param valueHitIndex
         *            the index into the SegmentValue which the hit is from
         */
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

        /**
         * This should be called with the SegmentBoundary which marks the end of the hits context
         *
         * @param contextEnd
         */
        public void setContextEnd(SegmentBoundary contextEnd) {
            this.contextEnd = contextEnd;
        }

        public SegmentBoundary getContextEnd() {
            return contextEnd;
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof SegmentHit)) {
                return false;
            }

            SegmentHit otherHit = (SegmentHit) other;
            // @formatter:off
            return Objects.equals(contextStart.getBoundaryType(), otherHit.contextStart.getBoundaryType()) &&
                    Objects.equals(contextStart.getStart(), otherHit.contextStart.getStart()) &&
                    Objects.equals(contextStart.getEnd(), otherHit.contextStart.getEnd()) &&
                    Objects.equals(contextEnd.getBoundaryType(), otherHit.contextEnd.getBoundaryType()) &&
                    Objects.equals(contextEnd.getStart(), otherHit.contextEnd.getStart()) &&
                    Objects.equals(contextEnd.getEnd(), otherHit.contextEnd.getEnd()) &&
                    Objects.equals(hitBoundary.getBoundaryType(), otherHit.hitBoundary.getBoundaryType()) &&
                    Objects.equals(hitBoundary.getStart(), otherHit.hitBoundary.getStart()) &&
                    Objects.equals(hitBoundary.getEnd(), otherHit.hitBoundary.getEnd()) &&
                    Objects.equals(valueHitIndex, otherHit.valueHitIndex);
            // @formatter:on
        }

        @Override
        public int hashCode() {
            // @formatter:off
            return Objects.hash(contextStart.getBoundaryTypeValue(),
                    contextStart.getStart(),
                    contextStart.getEnd(),
                    hitBoundary.getBoundaryTypeValue(),
                    hitBoundary.getStart(),
                    hitBoundary.getEnd(),
                    contextEnd.getBoundaryTypeValue(),
                    contextEnd.getStart(),
                    contextEnd.getEnd(),
                    valueHitIndex);
            // @formatter:on
        }
    }
}
