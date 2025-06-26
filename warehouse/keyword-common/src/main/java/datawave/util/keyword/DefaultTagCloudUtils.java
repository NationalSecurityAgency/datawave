package datawave.util.keyword;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.accumulo.core.security.ColumnVisibility;

/**
 * Default implementations for pluggable utilities for generating tag clouds, includes mechanisms to partition keywords into separate tag clouds, combine or
 * merge visibility strings, and calculate scores, source collections and frequencies of individual keywords based on observed results
 */
public class DefaultTagCloudUtils implements TagCloudUtils, Serializable {

    private static final long serialVersionUID = 652771994052429009L;

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
    public String computeIndexKey(KeywordResults results, String keyword, boolean partitionOnLanguage) {
        return (partitionOnLanguage ? results.getLanguage() + "%%" : "") + keyword;
    }

    @Override
    public String computeVisibilityKey(KeywordResults results, boolean partitionOnLanguage) {
        return partitionOnLanguage ? results.getLanguage() : "";
    }

    @Override
    public String computePartitionKey(Map.Entry<String,TagCloudEntry.Builder> entry, boolean partitionOnLanguage) {
        final String key = entry.getKey();
        final int pos = key.indexOf("%%");
        if (pos > -1) {
            return key.substring(0, pos);
        } else {
            return "";
        }
    }

    @Override
    public double calculateScore(Collection<TagCloudEntry.ScoreTuple> sourceScores) {
        // for now, choose the best (smallest) scored version of the tag.
        return sourceScores.stream().map(TagCloudEntry.ScoreTuple::getScore).min(Double::compareTo).orElse(1.0);

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
