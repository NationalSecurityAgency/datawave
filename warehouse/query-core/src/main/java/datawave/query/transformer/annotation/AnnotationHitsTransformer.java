package datawave.query.transformer.annotation;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
//import java.util.SortedMap;
import java.util.TreeMap;

import javax.annotation.Nullable;

//import datawave.query.transformer.annotation.model.Term;
//import datawave.query.transformer.annotation.model.TermHit;
import org.apache.accumulo.core.data.Key;
import org.apache.commons.jexl3.parser.ASTEQNode;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ParseException;
import org.apache.log4j.Logger;

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
import datawave.query.transformer.DocumentTransform;
//import datawave.query.transformer.annotation.model.AllHit;
import datawave.query.transformer.annotation.model.AllHits;

/**
 * This iterator will lookup and search annotations for hits as well as provide context
 */
public class AnnotationHitsTransformer extends DocumentTransform.DefaultDocumentTransform {
    private static final Logger log = Logger.getLogger(AnnotationHitsTransformer.class);
    // private static final AllHits EMPTY_ALL_HITS = new AllHits();

    private final AnnotationDataAccess annotationDataAccess;
    private final AllHitsFactory allHitsFactory;
    private final int contextBoundary;
    private final Set<String> validTypes;
    private final Set<String> validQueryFields;
    private final String targetField;

    private Set<String> searchHitTerms;
    private ObjectMapper objectMapper;

    public AnnotationHitsTransformer(AnnotationDataAccess annotationDataAccess, AllHitsFactory allHitsFactory, int contextBoundary, Set<String> validTypes,
                    Set<String> validQueryFields, String targetField) {
        this.annotationDataAccess = annotationDataAccess;
        this.allHitsFactory = allHitsFactory;
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
                        AllHits results = allHitsFactory.create(annotation.getAnnotationId(), hits, sortedSegments);
                        // TODO might be multi-valued
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

    // private void applyHit(AllHit allHit, SegmentHit segmentHit, SegmentValue hitValue) {
    // TermHit th = new TermHit();
    // th.setTermLabel(hitValue.getValue());
    // th.setConfidence(hitValue.getScore());
    // th.getTimeRange().setStartTime(segmentHit.getHitBoundary().getStart());
    // th.getTimeRange().setEndTime(segmentHit.getHitBoundary().getEnd());
    // allHit.getTermHits().add(th);
    //
    // // rollup confidence
    // if (allHit.getConfidence() < th.getConfidence()) {
    // allHit.setConfidence(th.getConfidence());
    // }
    // }
    //
    // /**
    // * Build the allHit oneBestContext while also applying the hit to the allHit in a single pass
    // * @param allHit the to be updated
    // * @param segmentHit
    // * @param contextIterator
    // */
    // private void applyContextAndHit(AllHit allHit, SegmentHit segmentHit, Iterator<Entry<SegmentBoundary,List<SegmentValue>>> contextIterator) {
    // while (contextIterator.hasNext()) {
    // Entry<SegmentBoundary,List<SegmentValue>> contextEntry = contextIterator.next();
    // SegmentBoundary boundary = contextEntry.getKey();
    // // highest score will be last
    // SegmentValue firstValue = contextEntry.getValue().get(contextEntry.getValue().size() - 1);
    // Term t = new Term();
    // t.setLabel(firstValue.getValue());
    // t.setConfidence(firstValue.getScore());
    // t.getTimeRange().setStartTime(boundary.getStart());
    // t.getTimeRange().setEndTime(boundary.getEnd());
    // allHit.getOneBestContext().add(t);
    //
    // // now check if this segment also contains the hit
    // if (segmentHit.getHitBoundary() == boundary) {
    // applyHit(allHit, segmentHit, contextEntry.getValue().get(segmentHit.getValueHitIndex()));
    // }
    // }
    // }
    //
    // private void addAllHit(AllHits allHits, AllHit allHit) {
    // allHits.getKeywordResultList().add(allHit);
    // if (allHits.getMaxTermHitConfidence() < allHit.getConfidence()) {
    // allHits.setMaxTermHitConfidence(allHit.getConfidence());
    // }
    // }
    //
    // private AllHits assembleResults(String annotationId, TreeMap<SegmentBoundary,List<SegmentValue>> sortedSegments, List<SegmentHit> hits) {
    // if (hits.isEmpty()) {
    // return EMPTY_ALL_HITS;
    // }
    //
    // // contains all hits across all boundaries
    // AllHits allHits = new AllHits();
    // allHits.setAnnotationId(annotationId);
    //
    // // contains all hits across a single boundary
    // AllHit allHit = new AllHit();
    //
    // // extract hits and convert to pojo
    // for (SegmentHit hit : hits) {
    // if (allHit.getHitBoundary() != null && !allHit.getHitBoundary().equals(hit.hitBoundary)) {
    // // create a new allHit
    // addAllHit(allHits, allHit);
    // allHit = new AllHit();
    // }
    //
    // // track the current boundary this hit will cover, this is not output, but keeps things organized
    // allHit.setHitBoundary(hit.hitBoundary);
    //
    // // both the start and end are inclusive
    // SortedMap<SegmentBoundary,List<SegmentValue>> contextView = sortedSegments.subMap(hit.getContextStart(), true, hit.getContextEnd(), true);
    // if (allHit.getOneBestContext().isEmpty()) {
    // // convert to an iterator to build the best context window
    // Iterator<Entry<SegmentBoundary,List<SegmentValue>>> itr = contextView.entrySet().iterator();
    // applyContextAndHit(allHit, hit, itr);
    // } else {
    // // just write the new TermHit
    // SegmentBoundary hitBoundary = hit.getHitBoundary();
    // SegmentValue hitValue = sortedSegments.get(hitBoundary).get(hit.getValueHitIndex());
    // applyHit(allHit, hit, hitValue);
    // }
    // }
    //
    // // add last allHit to allHits
    // addAllHit(allHits, allHit);
    //
    // return allHits;
    // }

    private void updateDocument(Entry<Key,Document> entry, AllHits allHits) {
        // convert pojo to json
        String json = null;
        try {
            json = objectMapper.writeValueAsString(allHits);
        } catch (JsonProcessingException e) {
            log.warn("Failed to write json for all hits for document: " + entry.getKey(), e);
        }

        if (json != null) {
            // TODO merge up any existing value in this field
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
