package datawave.util.keyword;

import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;

/**
 * Interface for pluggable utilities for generating tag clouds, includes mechanisms to partition keywords into separate tag clouds, combine or merge visibility
 * strings, and calculate scores, source collections and frequencies of individual keywords based on observed results
 */
public interface TagCloudUtils {
    /**
     * Aggregate visibilities by concatenating them together with an AND ('&amp;'), and then flatten them using the Accumulo column visibility class.
     *
     * @param visibilities
     *            a set of valid visibility strings.
     * @return a single visibility
     */
    Map<String,String> generateCombinedVisibility(Set<String> visibilities);

    /**
     * Aggregate metadata from multiple sources, combining multi-valued entries into a flattened string
     *
     * @param metadata
     *            the metadata to combine
     * @return a flattened map of metadata
     */
    Map<String,String> generateCombinedMetadata(Map<String,Set<String>> metadata);

    /**
     * given a set of ScoreTuples, calculate the resulting score for the keyword entry
     *
     * @param sourceScores
     *            a list of scores
     * @param comparator
     *            the comparator for the scores
     * @return the final keyword score
     */
    double calculateScore(Collection<TagCloudEntry.ScoreTuple> sourceScores, Comparator<Double> comparator, double defaultScore);

    /**
     * given a set of ScoreTuples, calculate the resulting source set for the keyword entry
     *
     * @param sourceScores
     *            a list of scores
     * @return the final keyword frequency
     */
    Set<String> calculateSources(Collection<TagCloudEntry.ScoreTuple> sourceScores);

    /**
     * given a set of ScoreTuples, calculate the resulting frequency set for the keyword entry
     *
     * @param sourceScores
     *            a list of scores
     * @return the final keyword frequency
     */
    int calculateFrequency(Collection<TagCloudEntry.ScoreTuple> sourceScores);
}
