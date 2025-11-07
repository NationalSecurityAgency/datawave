package datawave.util.keyword;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang3.StringUtils;

/**
 * Default implementations for pluggable utilities for generating tag clouds, includes mechanisms to partition keywords into separate tag clouds, combine or
 * merge visibility strings, and calculate scores, source collections and frequencies of individual keywords based on observed results
 */
public class DefaultTagCloudUtils implements TagCloudUtils, Serializable {
    private static final long serialVersionUID = 652771994052429009L;
    private static final String MULTI_VALUE_SEPARATOR = ",";

    @Override
    public Map<String,String> generateCombinedVisibility(Set<String> visibilities) {
        final StringBuilder b = new StringBuilder();
        visibilities.forEach(x -> b.append("(").append(x).append(")&"));
        if (b.length() > 0) {
            b.setLength(b.length() - 1);
            ColumnVisibility cv = new ColumnVisibility(b.toString());
            ColumnVisibility flat = new ColumnVisibility(cv.flatten());
            return Map.of("visibility", flat.toString());
        } else {
            return Collections.emptyMap();
        }
    }

    @Override
    public Map<String,String> generateCombinedMetadata(Map<String,Set<String>> metadata) {
        Map<String,String> combined = new HashMap<>();

        for (Entry<String,Set<String>> entry : metadata.entrySet()) {
            String joined = StringUtils.join(entry.getValue(), MULTI_VALUE_SEPARATOR);
            combined.put(entry.getKey(), joined);
        }

        return combined;
    }

    @Override
    public double calculateScore(Collection<TagCloudEntry.ScoreTuple> sourceScores, Comparator<Double> comparator, double defaultScore) {
        return sourceScores.stream().map(TagCloudEntry.ScoreTuple::getScore).sorted(comparator).findFirst().orElse(defaultScore);
    }

    @Override
    public Set<String> calculateSources(Collection<TagCloudEntry.ScoreTuple> sourceScores) {
        return sourceScores.stream().map(TagCloudEntry.ScoreTuple::getSource).collect(Collectors.toSet());
    }

    @Override
    public int calculateFrequency(Collection<TagCloudEntry.ScoreTuple> sourceScores) {
        // if multiple 'documents' have the same source identifier, count only once.
        return (int) sourceScores.stream().map(TagCloudEntry.ScoreTuple::getSource).distinct().count();
    }
}
