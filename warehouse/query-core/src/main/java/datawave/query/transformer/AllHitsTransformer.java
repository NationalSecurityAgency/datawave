package datawave.query.transformer;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import javax.annotation.Nullable;

import org.apache.accumulo.core.data.Key;
import org.apache.commons.jexl3.parser.ASTEQNode;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ParseException;
import org.apache.log4j.Logger;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.core.JsonProcessingException;
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
import datawave.query.jexl.JexlASTHelper;

/**
 * This iterator will lookup and search annotations for hits as well as provide context
 */
public class AllHitsTransformer extends DocumentTransform.DefaultDocumentTransform {
    private static final Logger log = Logger.getLogger(AllHitsTransformer.class);
    private static final AllHits EMPTY_ALL_HITS = new AllHits();

    private final AnnotationDataAccess annotationDataAccess;
    private final int contextBoundary;
    private final Set<String> validTypes;
    private final Set<String> validQueryFields;
    private final String targetField;

    private Set<String> searchHitTerms;
    private ObjectMapper objectMapper;

    public AllHitsTransformer(AnnotationDataAccess annotationDataAccess, int contextBoundary, Set<String> validTypes, Set<String> validQueryFields,
                    String targetField) {
        this.annotationDataAccess = annotationDataAccess;
        this.contextBoundary = contextBoundary;
        this.validTypes = validTypes;
        this.validQueryFields = validQueryFields;
        this.targetField = targetField;
    }

    @Override
    public void initialize(Query settings, MarkingFunctions markingFunctions) {
        super.initialize(settings, markingFunctions);

        // extract terms to lookup hits on
        try {
            this.searchHitTerms = extractSearchHitTerms(settings);
        } catch (ParseException e) {
            log.debug("no valid search terms detected for query, skipping all hits");
        }

        objectMapper = new ObjectMapper();
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
        if (searchHitTerms == null || searchHitTerms.isEmpty()) {
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
                        List<SegmentHit> hits = search(sortedSegments);
                        AllHits results = assembleResults(sortedSegments, hits);
                        updateDocument(keyDocumentEntry, results);
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

    private AllHits assembleResults(TreeMap<SegmentBoundary,List<SegmentValue>> sortedSegments, List<SegmentHit> hits) {
        if (hits.isEmpty()) {
            return EMPTY_ALL_HITS;
        }

        AllHits allHits = new AllHits();
        AllHit allHit = new AllHit();

        // extract hits and convert to pojo
        for (SegmentHit hit : hits) {
            if (allHit.hitBoundary != null && !allHit.hitBoundary.equals(hit.hit)) {
                // create a new allHit
                allHits.hits.add(allHit);
                allHit = new AllHit();
            }

            allHit.hitBoundary = hit.hit;

            // both the start and end are inclusive
            SortedMap<SegmentBoundary,List<SegmentValue>> contextView = sortedSegments.subMap(hit.getContextStart(), true, hit.getContextEnd(), true);
            if (allHit.oneBestContext == null) {
                allHit.oneBestContext = new ArrayList<>();
                Iterator<Entry<SegmentBoundary,List<SegmentValue>>> itr = contextView.entrySet().iterator();
                if (!itr.hasNext()) {
                    // special case, start and end context were the same so no elements were covered in the submap
                    itr = List.of(Map.entry(hit.getHit(), sortedSegments.get(hit.getHit()))).iterator();
                }
                while (itr.hasNext()) {
                    Entry<SegmentBoundary,List<SegmentValue>> contextEntry = itr.next();
                    SegmentBoundary boundary = contextEntry.getKey();
                    // highest score will be last
                    SegmentValue firstValue = contextEntry.getValue().get(contextEntry.getValue().size() - 1);
                    Term t = new Term();
                    t.label = firstValue.getValue();
                    t.confidence = firstValue.getScore();
                    t.timeRange = new TimeRange();
                    t.timeRange.startTime = boundary.getStart();
                    t.timeRange.endTime = boundary.getEnd();
                    allHit.oneBestContext.add(t);

                    // now check if this segment also contains the hit
                    if (hit.getHit() == boundary) {
                        SegmentValue hitValue = contextEntry.getValue().get(hit.getValueHitIndex());
                        TermHit th = new TermHit();
                        th.termLabel = hitValue.getValue();
                        th.confidence = hitValue.getScore();
                        th.timeRange = t.timeRange;
                        if (allHit.termHits == null) {
                            allHit.termHits = new ArrayList<>();
                        }
                        allHit.termHits.add(th);

                        // rollup confidence
                        if (allHit.confidence < th.confidence) {
                            allHit.confidence = th.confidence;
                        }
                    }
                }
            } else {
                // just write the new TermHit
                SegmentBoundary hitBoundary = hit.getHit();
                SegmentValue hitValue = sortedSegments.get(hitBoundary).get(hit.getValueHitIndex());
                TermHit th = new TermHit();
                th.termLabel = hitValue.getValue();
                th.confidence = hitValue.getScore();
                th.timeRange = new TimeRange();
                th.timeRange.startTime = hitBoundary.getStart();
                th.timeRange.endTime = hitBoundary.getEnd();

                allHit.termHits.add(th);

                // rollup confidence
                if (allHit.confidence < th.confidence) {
                    allHit.confidence = th.confidence;
                }
            }
        }

        // add last allHit to allHits
        allHits.hits.add(allHit);

        return allHits;
    }

    private void updateDocument(Entry<Key,Document> entry, AllHits allHits) {
        // convert pojo to json
        String json = null;
        try {
            json = objectMapper.writeValueAsString(allHits);
        } catch (JsonProcessingException e) {
            log.warn("Failed to write json for all hits for document: " + entry.getKey(), e);
        }

        if (json != null) {
            // update the document
            entry.getValue().put(targetField, new Content(json, entry.getKey(), true));
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
     * Make a single pass through the segments finding matches and creating context for each hit
     *
     * @param segments
     */
    private List<SegmentHit> search(TreeMap<SegmentBoundary,List<SegmentValue>> segments) {
        // keep a list of recent boundaries for context
        // window has to include context + 1 so that on the hit it still has the full window available
        int maxWindow = contextBoundary + 1;
        ArrayDeque<SegmentBoundary> window = new ArrayDeque<>(maxWindow);

        final Iterator<SegmentBoundary> itr = segments.navigableKeySet().iterator();
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

            List<SegmentValue> values = segments.get(boundary);
            for (int i = 0; i < values.size(); i++) {
                String normalizedTerm = normalize(values.get(i).getValue());
                if (searchHitTerms.contains(normalizedTerm)) {
                    // partial hits index is the location in the window where the hit is complete
                    List<SegmentHit> hits = partialHits.computeIfAbsent(segmentIndex + contextBoundary, x -> new ArrayList<>());
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

    private Set<String> extractSearchHitTerms(Query settings) throws ParseException {
        Set<String> searchTerms = new HashSet<>();

        String query = settings.getQuery();
        if (query != null) {
            ASTJexlScript script = JexlASTHelper.parseJexlQuery(query);
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

    public static class AllHits {
        @JsonValue
        private List<AllHit> hits = new ArrayList<>();
    }

    public static class AllHit {
        @JsonProperty
        private float confidence;
        @JsonProperty
        private List<Term> oneBestContext;
        @JsonProperty
        private List<TermHit> termHits;
        private SegmentBoundary hitBoundary;
    }

    public static class TermHit {
        @JsonProperty
        private String termLabel;
        @JsonProperty
        private float confidence;
        @JsonProperty
        private TimeRange timeRange;
    }

    public static class Term {
        @JsonProperty
        private String label;
        @JsonProperty
        private float confidence;
        @JsonProperty
        private TimeRange timeRange;
    }

    public static class TimeRange {
        @JsonProperty
        private float startTime;
        @JsonProperty
        private float endTime;
    }

    public static class SegmentValueComparator implements Comparator<SegmentValue> {
        @Override
        public int compare(SegmentValue o1, SegmentValue o2) {
            float scoreDiff = o1.getScore() - o2.getScore();
            if (scoreDiff == 0) {
                // equal scores sort by value
                return o1.getValue().compareTo(o2.getValue());
            } else if (scoreDiff < 0) {
                return -1;
            } else {
                return 1;
            }
        }
    }

    public static class BoundaryComparator implements Comparator<SegmentBoundary> {
        @Override
        public int compare(SegmentBoundary o1, SegmentBoundary o2) {
            if (o1.getBoundaryTypeValue() != o2.getBoundaryTypeValue()) {
                // lower number sorts first
                return o1.getBoundaryTypeValue() - o2.getBoundaryTypeValue();
            }

            // sort by start
            return o1.getStart() - o2.getStart();
        }
    }

    public static class SegmentHit {
        private final SegmentBoundary contextStart;
        private final SegmentBoundary hit;
        private final int valueHitIndex;

        private SegmentBoundary contextEnd;

        private SegmentHit(SegmentBoundary contextStart, SegmentBoundary hit, int valueHitIndex) {
            this.contextStart = contextStart;
            this.hit = hit;
            this.valueHitIndex = valueHitIndex;
        }

        public SegmentBoundary getContextStart() {
            return contextStart;
        }

        public SegmentBoundary getHit() {
            return hit;
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
