package datawave.util.keyword;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import com.google.gson.Gson;

/** A tag cloud - a collection of tags that have a keyword, score, frequency and list of sources from which they originated */
public class TagCloud {

    static final Gson gson = new Gson();

    /** the 'name' of this cloud, usually a language name */
    final String name;

    /** the 'visibility' of this cloud */
    final Map<String,String> visibility;

    /** the sorted set of keywords in this cloud, including scores and sources */
    final SortedSet<TagCloudEntry> results;

    /**
     * Use the builder to create tag clouds
     *
     * @param name
     *            the name of the cloud, typically used for language
     * @param visibility
     *            the visibility of the cloud, some form of visibility marking
     * @param results
     *            the entries that belong in the tag cloud.
     */
    protected TagCloud(String name, Map<String,String> visibility, SortedSet<TagCloudEntry> results) {
        this.name = name;
        this.visibility = visibility;
        this.results = results;
    }

    public String getName() {
        return name;
    }

    public Map<String,String> getVisibility() {
        return visibility;
    }

    public Collection<TagCloudEntry> getResults() {
        return results;
    }

    /**
     * Serialize to JSON
     *
     * @return the json representation of the tag cloud.
     */
    public String toString() {
        return gson.toJson(this);
    }

    /**
     * Deserialize from JSON
     *
     * @param json
     *            the json representation of the tag cloud.
     * @return a deserialized tag clout
     */
    public static TagCloud fromJson(String json) {
        return gson.fromJson(json, TagCloud.class);
    }

    /**
     * Used to build a TagCloud object by adding a number of independent KeywordResults object and then aggregating them with the build() method.
     */
    public static class Builder {

        /** Holds the index of tuples we're capture while building this object, used to calculate the final set of results in the finish() method. */
        final transient Map<String,TagCloudEntry.Builder> index = new HashMap<>();

        /** Holds the set of visibility strings we've seen so far - this will be used for generating the overall visibility string */
        final transient Map<String,Set<String>> visibilities = new HashMap<>();

        /** The maximum number of tags to keep per cloud. Zero means unlimited */
        int maxTags = 0;

        /** Whether we should partition tag clouds based on language */
        boolean partitionOnLanguage;

        /** The comparator of tuples to keep */
        Comparator<TagCloudEntry> comparator = TagCloudEntry.ORDER_BY_SCORE;

        TagCloudUtils utils = new DefaultTagCloudUtils();

        /** Add a set of keyword extraction results to the tag cloud to be built */
        public void addResults(KeywordResults results) {
            final String source = results.getSource();
            final String language = results.getLanguage();
            final String visibility = results.getVisibility();
            final LinkedHashMap<String,Double> resultsMap = results.getKeywords();

            for (final Map.Entry<String,Double> e : resultsMap.entrySet()) {
                String key = utils.computeIndexKey(results, e.getKey(), partitionOnLanguage);
                TagCloudEntry.Builder b = index.computeIfAbsent(key, k -> new TagCloudEntry.Builder(e.getKey()).withUtilities(utils));
                b.addSourceScore(source, e.getValue(), language);
            }

            if (!visibility.isEmpty() && !resultsMap.isEmpty()) {
                String key = utils.computeVisibilityKey(results, partitionOnLanguage);
                Set<String> visibilityList = visibilities.computeIfAbsent(key, k -> new TreeSet<>());
                visibilityList.add(visibility);
            }
        }

        /**
         * Limit any tag clouds built by this builder to no more than this number of tags. In cases where we build multiple tag clouds (e.g., per language),
         * this limit will apply to individual clouds. It is <i>not</i> a max number of tags across all clouds.
         *
         * @param maxTags
         *            the max number of tags per cloud
         * @return the builder.
         */
        public Builder withMaxTags(int maxTags) {
            this.maxTags = maxTags;
            return this;
        }

        /**
         * Use the specified comparator for ranking/sorting the tag cloud entries. This is especially important if we have set a maximum number of tags using
         * the withMaxTags() method.
         *
         * @param comparator
         *            the comparator to use
         * @return the builder.
         */
        public Builder withComparator(Comparator<TagCloudEntry> comparator) {
            this.comparator = comparator;
            return this;
        }

        /**
         * Indicate that we should build one tag cloud per language
         *
         * @param partitionOnLanguage
         *            true if we should generate multiple tag clouds, one per language
         * @return the builder.
         */
        public Builder withLanguagePartitions(boolean partitionOnLanguage) {
            this.partitionOnLanguage = partitionOnLanguage;
            return this;
        }

        /**
         * Indicate that we should use a specific utilities instance.
         */
        public Builder withTagCloudUtilities(TagCloudUtils utils) {
            this.utils = utils;
            return this;
        }

        /**
         * Build one or more tag clouds using the KeywordResults introduced via addResults. Multiple clouds may be returned if the withLanguagePartitions method
         * was called with 'true' in which case we'll have one cloud per language.
         *
         * @return a list of one or more tag clouds depending on the configuration of the builder.
         */
        public List<TagCloud> build() {
            // we create one priority queue for each partition - partitions are used for generating a cloud
            // per language, by default there's a single unnamed partition.
            final Map<String,PriorityQueue<TagCloudEntry>> queueMap = new HashMap<>();

            // reverse the comparator for the priority queue because we want the worst results to be at the head of
            // the queue and removed by poll.
            final Comparator<TagCloudEntry> queueComparator = comparator.reversed();

            // partition the index of keyword results into one priority queue per partition.
            for (Map.Entry<String,TagCloudEntry.Builder> e : index.entrySet()) {
                String partitionKey = utils.computePartitionKey(e, partitionOnLanguage);
                PriorityQueue<TagCloudEntry> resultsQueue = queueMap.computeIfAbsent(partitionKey, k -> new PriorityQueue<>(queueComparator));
                resultsQueue.add(e.getValue().build());
                if (maxTags > 0 && resultsQueue.size() > maxTags) {
                    resultsQueue.poll();
                }
            }

            // Generated one tag cloud per partition.
            final List<TagCloud> tagClouds = new ArrayList<>();

            // perform the final sort for each partition
            for (Map.Entry<String,PriorityQueue<TagCloudEntry>> e : queueMap.entrySet()) {
                final SortedSet<TagCloudEntry> results = new TreeSet<>(comparator);
                results.addAll(e.getValue());
                Map<String,String> visibility = utils.generateCombinedVisibility(visibilities.get(e.getKey()));
                tagClouds.add(new TagCloud(e.getKey(), visibility, results));
            }

            return tagClouds;
        }

    }
}
