package datawave.query.util.keyword;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import com.google.gson.Gson;

/** A tag cloud - a collection of tags that have a keyword, score, frequency and list of sources from which they originated */
public class TagCloud {

    static final Gson gson = new Gson();

    final SortedSet<TagCloudEntry> results;

    protected TagCloud(SortedSet<TagCloudEntry> results) {
        this.results = results;
    }

    public Collection<TagCloudEntry> getResults() {
        return results;
    }

    public String toString() {
        return gson.toJson(this);
    }

    public static TagCloud fromJson(String json) {
        return gson.fromJson(json, TagCloud.class);
    }

    /**
     * Used to build a TagCloud object by adding a number of independent KeywordResults object and then aggregating them with the build() method.
     */
    public static class Builder {
        // Holds the index of tuples we're capture while building this object, used to calculate the final set of
        // results in the finish() method.
        final transient Map<String,TagCloudEntry.Builder> index = new HashMap<>();

        public void addResults(KeywordResults results) {
            final String source = results.getSource();
            final String language = results.getLanguage();
            final LinkedHashMap<String,Double> resultsMap = results.getKeywords();
            for (Map.Entry<String,Double> e : resultsMap.entrySet()) {
                TagCloudEntry.Builder b = index.computeIfAbsent(e.getKey(), TagCloudEntry.Builder::new);
                b.addSourceScore(source, e.getValue(), language);
            }
        }

        public TagCloud build() {
            final SortedSet<TagCloudEntry> results = new TreeSet<>();
            for (Map.Entry<String,TagCloudEntry.Builder> e : index.entrySet()) {
                results.add(e.getValue().build());
            }
            return new TagCloud(results);
        }
    }
}
