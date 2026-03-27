package datawave.query.tables.keyword;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import datawave.query.tables.keyword.transform.KeywordResultsTransformer;
import datawave.webservice.result.keyword.DefaultTagCloud;
import datawave.webservice.result.keyword.DefaultTagCloudEntry;

/**
 * Utility for producing TagClouds and supporting objects for tests
 */
public class TagCloudTestUtil {
    public static final String SOPRANO_SOURCE = "20130101_0/test/-1kfeoq.-80b5fs.r0262j";
    public static final String CAPONE_SOURCE = "20130101_0/test/-cvy0gj.tlf59s.-duxzua";
    public static final String CORLEONE_SOURCE = "20130101_0/test/-d5uxna.msizfm.-oxy0iu";

    public static final String SOPRANO_UUID = "UUID:SOPRANO";
    public static final String CAPONE_UUID = "UUID:CAPONE";
    public static final String CORLEONE_UUID = "UUID:CORLEONE";

    public DefaultTagCloud getExpectedCloud(String version, Map<String,String> metadata, List<DefaultTagCloudEntry> entries) {
        DefaultTagCloud expectedCloud = new DefaultTagCloud();
        if (version.equals("1")) {
            expectedCloud.setLanguage(metadata.get("language") != null ? metadata.get("language") : metadata.get("type"));
        } else {
            expectedCloud.setMetadata(metadata);
        }
        expectedCloud.setMarkings(Map.of("visibility", "[ALL]"));
        expectedCloud.setTags(entries);
        expectedCloud.setIntermediateResult(false);

        return expectedCloud;
    }

    public DefaultTagCloud getCaponeKeywordCloud(String docId, String version) {
        List<DefaultTagCloudEntry> entries = new ArrayList<>();
        entries.add(createTagCloudEntry("kind word", 0.2052, 1, List.of(docId)));
        entries.add(createTagCloudEntry("kind", 0.2546, 1, List.of(docId)));
        entries.add(createTagCloudEntry("word", 0.2857, 1, List.of(docId)));
        entries.add(createTagCloudEntry("kind word alone", 0.4375, 1, List.of(docId)));
        entries.add(createTagCloudEntry("word alone", 0.534, 1, List.of(docId)));
        entries.add(createTagCloudEntry("get much", 0.5903, 1, List.of(docId)));
        entries.add(createTagCloudEntry("much farther", 0.5903, 1, List.of(docId)));

        DefaultTagCloud expectedCloud = new DefaultTagCloud();
        expectedCloud.setMarkings(Map.of("visibility", "[ALL]"));
        expectedCloud.setTags(entries);
        if (version.equals("1")) {
            expectedCloud.setLanguage("keyword");
        } else {
            expectedCloud.setMetadata(Map.of("view", "CONTENT", "type", KeywordResultsTransformer.LABEL));
        }
        expectedCloud.setIntermediateResult(false);
        return expectedCloud;
    }

    public DefaultTagCloudEntry createTagCloudEntry(String term, double score, int frequency, List<String> sources) {
        DefaultTagCloudEntry entry = new DefaultTagCloudEntry();
        entry.setTerm(term);
        entry.setScore(score);
        entry.setFrequency(frequency);
        entry.setSources(sources);

        return entry;
    }
}
