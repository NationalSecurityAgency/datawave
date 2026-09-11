package datawave.query.transformer.annotation;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import datawave.annotation.protobuf.v1.SegmentBoundary;

/** Matches standalone patterns against a normalized annotation position view. */
public final class StandaloneAnnotationMatcher {
    private final Set<Pattern> patterns;

    public StandaloneAnnotationMatcher(Set<Pattern> patterns) {
        this.patterns = patterns;
    }

    public List<SegmentHit> match(AnnotationPositionView view, int contextSize, float minScore) {
        int maxWindow = contextSize + 1;
        ArrayDeque<SegmentBoundary> window = new ArrayDeque<>(maxWindow);
        List<SegmentHit> finished = new ArrayList<>();
        Map<Integer,List<SegmentHit>> partial = new HashMap<>();
        List<SegmentBoundary> boundaries = view.getBoundaries();

        for (int boundaryIndex = 0; boundaryIndex < boundaries.size(); boundaryIndex++) {
            SegmentBoundary boundary = boundaries.get(boundaryIndex);
            if (window.size() == maxWindow)
                window.removeFirst();
            window.add(boundary);

            for (ValuePosition position : view.getValues(boundaryIndex)) {
                if (position.getValue().getScore() >= minScore && matches(position.getNormalizedValue())) {
                    partial.computeIfAbsent(boundaryIndex + contextSize, ignored -> new ArrayList<>())
                                    .add(new SegmentHit(window.getFirst(), boundary, position.getValueIndex()));
                }
            }

            List<SegmentHit> completing = partial.remove(boundaryIndex);
            if (completing != null) {
                for (SegmentHit hit : completing)
                    hit.setContextEnd(boundary);
                finished.addAll(completing);
            }
        }

        SegmentBoundary last = boundaries.isEmpty() ? null : boundaries.get(boundaries.size() - 1);
        for (List<SegmentHit> hits : partial.values()) {
            for (SegmentHit hit : hits)
                hit.setContextEnd(last);
            finished.addAll(hits);
        }
        return finished;
    }

    private boolean matches(String normalized) {
        for (Pattern pattern : patterns) {
            if (pattern.matcher(normalized).matches())
                return true;
        }
        return false;
    }
}
