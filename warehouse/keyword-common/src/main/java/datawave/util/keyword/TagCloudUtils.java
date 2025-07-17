package datawave.util.keyword;

import java.util.Collection;
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
     * find the index key from the keyword results and keyword. Usually,the index key just the keyword, but when we're partitioning by language, we'll add the
     * language at the beginning of the key and use this for dividing into multiple tag clouds later.
     *
     * @param results
     *            the keyword results to drive key creation
     * @param keyword
     *            the keyword to drive key creation
     * @param partitionOnLanguage
     *            whether we should partition tag clouds on language
     * @return the index key for the results entries.
     */
    String computeIndexKey(KeywordResults results, String keyword, boolean partitionOnLanguage);

    /**
     * find the key to use for the visibility map. We have one set of visibilities for every partition. The key produced here should be identical to the
     * computePartitionKey for the same input.
     *
     * @param results
     *            the keyword results to drive key creation.
     * @param partitionOnLanguage
     *            whether we should partition tag clouds on language
     * @return the key to use for tracking visibility sets.
     */
    String computeVisibilityKey(KeywordResults results, boolean partitionOnLanguage);

    /**
     * find the partition key from the index entry. If we're partitioning by language this will be the language, otherwise this will return an empty string.
     * This can be used for both the visibilities map and tag cloud names.
     *
     * @param entry
     *            the entry to use to determine the partition key
     * @param partitionOnLanguage
     *            whether we should partition tag clouds on language
     * @return the partition key.
     */
    String computePartitionKey(Map.Entry<String,TagCloudEntry.Builder> entry, boolean partitionOnLanguage);

    /**
     * given a set of ScoreTuples, calculate the resulting score for the keyword entry
     *
     * @param sourceScores
     *            a list of scores
     * @return the final keyword score
     */
    double calculateScore(Collection<TagCloudEntry.ScoreTuple> sourceScores);

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
