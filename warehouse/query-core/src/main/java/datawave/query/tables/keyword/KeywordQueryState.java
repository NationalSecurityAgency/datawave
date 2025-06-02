package datawave.query.tables.keyword;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.apache.accumulo.core.data.Range;

/** Captures the internal state of a single keyword query */
public class KeywordQueryState {

    /**
     * if true, generate cloud that consolidates keyword extraction results for all retrieved documents, if false will generate a tag cloud for each individual
     * document.
     */
    private boolean generateCloud = false;

    /**
     * if true, group tags by languge - if not, combine keywords from all languages into a single cloud.
     */
    private boolean languagePartitioned = true;

    /**
     * the maximum number of tags we will generate in the final cloud
     */
    private int maxCloudTags = 0;

    /** a list of view names we will attempt to use for content */
    private final List<String> viewNames = new ArrayList<>();

    /**
     * a map between document uids (shard/datatype/uid) and desired language, populated by parsing the query and used to configure the keyword extraction
     * algorithm
     */
    private final Map<String,String> languageMap = new HashMap<>();

    /**
     * a map between document uids (shard/datatype/uid) and the identifer used to find that uid (e.g., PAGE_ID:1234) used for display in the sources field of
     * the generated tag clouds
     */
    private final Map<String,String> identifierMap = new HashMap<>();

    /** the ranges to scan based on the query terms */
    private final Collection<Range> ranges = new TreeSet<>();

    public boolean isGenerateCloud() {
        return generateCloud;
    }

    public void setGenerateCloud(boolean generateCloud) {
        this.generateCloud = generateCloud;
    }

    public boolean isLanguagePartitioned() {
        return languagePartitioned;
    }

    public void setLanguagePartitioned(boolean languagePartitioned) {
        this.languagePartitioned = languagePartitioned;
    }

    public int getMaxCloudTags() {
        return maxCloudTags;
    }

    public void setMaxCloudTags(int maxCloudTags) {
        this.maxCloudTags = maxCloudTags;
    }

    public Map<String,String> getLanguageMap() {
        return languageMap;
    }

    public Map<String,String> getIdentifierMap() {
        return identifierMap;
    }

    public List<String> getPreferredViews() {
        return viewNames;
    }

    public Collection<Range> getRanges() {
        return new ArrayList<>(this.ranges);
    }

    public void setRanges(final Collection<Range> ranges) {
        // As a single atomic operation, clear the range and add all the
        // specified ranges
        this.ranges.clear();
        if (null != ranges) {
            this.ranges.addAll(ranges);
        }
    }
}
