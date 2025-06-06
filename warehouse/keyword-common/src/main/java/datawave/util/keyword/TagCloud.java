package datawave.util.keyword;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.SortedSet;
import java.util.TreeSet;

import com.google.gson.Gson;

/** A tag cloud - a collection of tags that have a keyword, score, frequency and list of sources from which they originated */
public class TagCloud {

    static final Gson gson = new Gson();

    /** the 'name' of this cloud, usually a language name */
    final String name;

    /** the sorted set of keywords in this cloud, including scores and sources */
    final SortedSet<TagCloudEntry> results;

    /** Use the builder to create tag clouds */
    protected TagCloud(String name, SortedSet<TagCloudEntry> results) {
        this.name = name;
        this.results = results;
    }

    public String getName() {
        return name;
    }

    public Collection<TagCloudEntry> getResults() {
        return results;
    }

    /** Serialize to JSON */
    public String toString() {
        return gson.toJson(this);
    }

    /** Deserialize from JSON */
    public static TagCloud fromJson(String json) {
        return gson.fromJson(json, TagCloud.class);
    }

    /**
     * Used to build a TagCloud object by adding a number of independent KeywordResults object and then aggregating them with the build() method.
     */
    public static class Builder {

        /** Holds the index of tuples we're capture while building this object, used to calculate the final set of results in the finish() method. */
        final transient Map<String,TagCloudEntry.Builder> index = new HashMap<>();

        /** The maximum number of tags to keep per cloud. Zero means unlimited */
        int maxTags = 0;

        /** Whether to partition the tag clouds by language */
        boolean partitionOnLanguage = false;

        /** The comparator of tuples to keep */
        Comparator<TagCloudEntry> comparator = TagCloudEntry.ORDER_BY_SCORE;

        /** Add a set of keyword extraction results to the tag cloud to be built */
        public void addResults(KeywordResults results) {
            final String source = results.getSource();
            final String language = results.getLanguage();
            final LinkedHashMap<String,Double> resultsMap = results.getKeywords();
            for (Map.Entry<String,Double> e : resultsMap.entrySet()) {
                String key = computeIndexKey(results, e.getKey());
                TagCloudEntry.Builder b = index.computeIfAbsent(key, k -> new TagCloudEntry.Builder(e.getKey()));
                b.addSourceScore(source, e.getValue(), language);
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
         *            the comparitor to use
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
                String partitionKey = computePartitionKey(e);
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
                tagClouds.add(new TagCloud(e.getKey(), results));
            }

            return tagClouds;
        }

        /**
         * find the index key from the keyword results and keyword. Usually,the index key just the keyword, but when we're partitioning by language, we'll add
         * the language at the beginning of the key and use this for dividing into multiple tag clouds later.
         *
         * @param results
         *            the results to drive key creation
         * @param keyword
         *            the keyword to drive key creation
         * @return the index key for the results entries.
         */
        public String computeIndexKey(KeywordResults results, String keyword) {
            return (partitionOnLanguage ? results.getLanguage() + "%%" : "") + keyword;
        }

        /**
         * find the partition key from the index entry. If we're partitioning by language this will be the language, otherwise this will return an empty string.
         *
         * @param entry
         *            the entry to use to determine the partition key
         * @return the partition key.
         */
        public String computePartitionKey(Map.Entry<String,TagCloudEntry.Builder> entry) {
            final String key = entry.getKey();
            final int pos = key.indexOf("%%");
            if (pos > -1) {
                return key.substring(0, pos);
            } else {
                return "";
            }
        }
    }
}
