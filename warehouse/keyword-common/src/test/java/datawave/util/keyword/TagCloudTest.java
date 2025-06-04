package datawave.util.keyword;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.Test;

public class TagCloudTest {

    @SuppressWarnings("SpellCheckingInspection")
    //@formatter:off
    public static String[] testData = {
            "androids dream=0.1273\n" +
            "blade runner=0.0168\n" +
            "cyberpunk films=0.1386\n" +
            "film blade=0.133\n" +
            "japanese cyberpunk=0.0561\n" +
            "new wave=0.0842\n" +
            "philip k=0.0346\n" +
            "science fiction=0.0502\n" +
            "western cyberpunk=0.1367\n" +
            "william gibson=0.0951\n",

            "import static=0.0092\n" +
            "public class=0.1583\n" +
            "public class yakekeywordregressiontest=0.1151\n" +
            "public static=0.063\n" +
            "public string=0.0246\n" +
            "public void=0.1583\n" +
            "string expectedraw=0.0979\n" +
            "string input=0.0979\n" +
            "string languagecode=0.0979\n" +
            "string output=0.0678",

            "acquiring kaggle=0.4602\n" +
            "cloud next=0.3884\n" +
            "cloud next conference=0.5552\n" +
            "san francisco=0.3884\n",

            "kent beck=0.3154\n" +
            "unit testing=0.2956\n"
    };

    public static String[] testSources = {
            "cyberpunk",
            "java-source",
            "kaggle",
            "unit-testing"
    };

    public static String[] testDataWithOverlaps = {
            "alpha=0.1273\n" +
                    "bravo=0.0168\n" +
                    "charlie=0.1386\n" +
                    "delta=0.133\n" +
                    "echo=0.0561\n" +
                    "foxtrot=0.0842\n" +
                    "golf=0.0346\n" +
                    "hotel=0.0502\n" +
                    "india=0.1367\n" +
                    "juliett=0.0951\n",

            "golf=0.0092\n" +
                    "hotel=0.1583\n" +
                    "india=0.1151\n" +
                    "juliett=0.063\n" +
                    "kilo=0.0246\n" +
                    "lima=0.1583\n" +
                    "mike=0.0979\n" +
                    "november=0.0979\n" +
                    "oscar=0.0979\n" +
                    "papa=0.0678",

            "delta=0.4602\n" +
                    "romeo=0.3884\n" +
                    "echo=0.5552\n" +
                    "whiskey=0.3884\n",

            "india=0.3154\n" +
                    "sierra=0.2956\n"
    };

    public static String[] testSourcesWithOverlaps = {
            "alpha",
            "golf",
            "delta",
            "india"
    };

    // -- utilities to set up test data.

    public static KeywordResults[] createData() {
        return createDataFromArrays(testData, testSources);
    }

    public static KeywordResults[] createDataWithOverlaps() {
        return createDataFromArrays(testDataWithOverlaps, testSourcesWithOverlaps);
    }

    public static KeywordResults[] createDataFromArrays(String[] data, String[] sources) {
        KeywordResults[] result = new KeywordResults[data.length];
        for (int i = 0; i < data.length; i++) {
            String content = data[i];
            LinkedHashMap<String,Double> parsedContent = parseContent(content);
            String source = sources[i];
            result[i] = new KeywordResults(source, "content", "english", parsedContent);
        }
        return result;
    }

    public static LinkedHashMap<String,Double> parseContent(String content) {
        LinkedHashMap<String,Double> result = new LinkedHashMap<>();
        String[] lines = content.split("\\n");
        for (String line : lines) {
            String[] columns = line.split("=");
            result.put(columns[0], Double.parseDouble(columns[1]));
        }
        return result;
    }

    // -- tests here.

    @Test
    public void testSerializationLifecycle() {
        // validates serialization.
        KeywordResults[] testInput = createData();
        TagCloud.Builder builder = new TagCloud.Builder();
        Arrays.stream(testInput).forEach(builder::addResults);

        List<TagCloud> cloud = builder.build();
        assertEquals(1, cloud.size());
        assertEquals("", cloud.get(0).getName());
        String result = cloud.get(0).toString();
        TagCloud deser = TagCloud.fromJson(result);

        assertEquals("", deser.getName());
        assertTagCloudResults(26, 0.0092, 0.5552, TagCloudEntry.ORDER_BY_SCORE, deser.getResults());
    }

    @Test
    public void testBuilderWithNoLimit() {
        KeywordResults[] testInput = createData();
        TagCloud.Builder builder = new TagCloud.Builder();
        Arrays.stream(testInput).forEach(builder::addResults);
        List<TagCloud> cloud = builder.build();

        assertEquals(1, cloud.size());
        assertEquals("", cloud.get(0).getName());
        assertTagCloudResults(26, 0.0092, 0.5552, TagCloudEntry.ORDER_BY_SCORE, cloud.get(0).getResults());
    }

    @Test
    public void testBuilderWithLimit() {
        KeywordResults[] testInput = createData();
        TagCloud.Builder builder = new TagCloud.Builder().withMaxTags(10);
        Arrays.stream(testInput).forEach(builder::addResults);
        List<TagCloud> cloud = builder.build();

        assertEquals(1, cloud.size());
        assertEquals("", cloud.get(0).getName());
        assertTagCloudResults(10, 0.0092, 0.0951, TagCloudEntry.ORDER_BY_SCORE, cloud.get(0).getResults());
    }

    @Test
    public void testBuilderWithAlternateComparator() {
        final Comparator<TagCloudEntry> comparator = TagCloudEntry.ORDER_BY_FREQUENCY;

        KeywordResults[] testInput = createDataWithOverlaps();
        TagCloud.Builder builder = new TagCloud.Builder().withComparator(comparator);
        Arrays.stream(testInput).forEach(builder::addResults);
        List<TagCloud> cloud = builder.build();

        assertEquals(1, cloud.size());
        assertEquals("", cloud.get(0).getName());
        assertTagCloudResults(19, 0.0092, 0.3884, comparator, cloud.get(0).getResults());
        assertFirstAndLastFrequency(3, 1, cloud.get(0).getResults());
    }

    @Test
    public void testBuilderWithLanguages() {
        KeywordResults[] testInput = createDataWithOverlaps();
        testInput[0].setLanguage("ONE");
        testInput[1].setLanguage("TWO");
        testInput[2].setLanguage("THREE");
        testInput[3].setLanguage("ONE");

        TagCloud.Builder builder = new TagCloud.Builder().withLanguagePartitions(true);
        Arrays.stream(testInput).forEach(builder::addResults);
        List<TagCloud> cloud = builder.build();

        assertEquals(3, cloud.size());

        List<String> expectedLanguages = List.of("ONE", "TWO", "THREE");
        List<String> unseenLanguages = new ArrayList<>(expectedLanguages);
        Map<String,TagCloud> resultsMap = new HashMap<>();

        for (TagCloud c : cloud) {
            unseenLanguages.remove(c.getName());
            resultsMap.put(c.getName(), c);
        }
        assertTrue("We expected to observe the following languages, but did not: " + unseenLanguages, unseenLanguages.isEmpty());

        // make some assertions about the individual maps.
        TagCloud oneCloud = resultsMap.get("ONE");
        assertNotNull(oneCloud);
        assertTagCloudResults(11, 0.0168, 0.2956, TagCloudEntry.ORDER_BY_SCORE, oneCloud.getResults());
        assertFirstAndLastFrequency(1, 1, oneCloud.getResults());
        assertKeywordsPresent(oneCloud.getResults(), "bravo", "foxtrot");
        assertKeywordsAbsent(oneCloud.getResults(), "november", "papa");

        TagCloud twoCloud = resultsMap.get("TWO");
        assertNotNull(twoCloud);
        assertTagCloudResults(10, 0.0092, 0.1583, TagCloudEntry.ORDER_BY_SCORE, twoCloud.getResults());
        assertFirstAndLastFrequency(1, 1, twoCloud.getResults());
        assertKeywordsAbsent(twoCloud.getResults(), "bravo", "foxtrot");
        assertKeywordsPresent(twoCloud.getResults(), "november", "papa");

        TagCloud threeCloud = resultsMap.get("THREE");
        assertNotNull(threeCloud);
        assertTagCloudResults(4, 0.3884, 0.5552, TagCloudEntry.ORDER_BY_SCORE, threeCloud.getResults());
        assertFirstAndLastFrequency(1, 1, threeCloud.getResults());
        assertKeywordsAbsent(threeCloud.getResults(), "bravo", "foxtrot");
        assertKeywordsAbsent(threeCloud.getResults(), "november", "papa");
        assertKeywordsPresent(threeCloud.getResults(), "echo", "whiskey");

    }

    private static void assertKeywordsPresent(Collection<TagCloudEntry> entries, String... keywords) {
        KEYWORDS: for (String keyword : keywords) {
            for (TagCloudEntry entry : entries) {
                if (keyword.equalsIgnoreCase(entry.getKeyword())) {
                    continue KEYWORDS;
                }
            }
            fail("Could not find keyword " + keyword + " in " + entries);
        }
    }

    // -- various assertion roll-ups.

    private static void assertKeywordsAbsent(Collection<TagCloudEntry> entries, String... keywords) {
        for (String keyword : keywords) {
            for (TagCloudEntry entry : entries) {
                if (keyword.equalsIgnoreCase(entry.getKeyword())) {
                    fail("Found unexpected keyword " + keyword + " in " + entries);
                }
            }
        }
    }

    @SuppressWarnings("SameParameterValue")
    private static void assertTagCloudResults(int expectedSize, double expectedMinScore, double expectedMaxScore, Comparator<TagCloudEntry> comparator,
                    Collection<TagCloudEntry> tagCloudEntries) {
        double lastScore = 0;
        double minScore = Double.MAX_VALUE;
        double maxScore = Double.MIN_VALUE;

        assertEquals(expectedSize, tagCloudEntries.size());

        TagCloudEntry lastEntry = null;
        for (TagCloudEntry entry : tagCloudEntries) {
            assertTrue("Entry score must be greater than zero", entry.getScore() > 0);
            assertTrue("Entry frequency must be greater than zero", entry.getFrequency() > 0);
            assertFalse("Entry sources size must not be empty", entry.getSources().isEmpty());
            assertFalse("Entry keyword must not be empty", entry.getKeyword() == null || entry.getKeyword().isEmpty());

            if (lastEntry != null) {
                assertTrue("Entries must be sorted in ascending order; last entry: " + lastEntry + " current entry: " + entry,
                                comparator.compare(lastEntry, entry) < 0);
            }

            // update state
            lastScore = entry.getScore();
            lastEntry = entry;

            // track min/max
            if (lastScore < minScore) {
                minScore = lastScore;
            }

            if (lastScore > maxScore) {
                maxScore = lastScore;
            }
        }
        assertEquals("Minimum score must be " + expectedMinScore, expectedMinScore, minScore, 0.0);
        assertEquals("Maximum score must be " + expectedMaxScore, expectedMaxScore, maxScore, 0.0);
    }

    public static void assertFirstAndLastFrequency(int firstFrequency, int lastFrequency, Collection<TagCloudEntry> tagCloudEntries) {
        List<Integer> f = tagCloudEntries.stream().map(TagCloudEntry::getFrequency).collect(Collectors.toList());
        assertEquals(firstFrequency, (int) f.get(0));
        assertEquals(lastFrequency, (int) f.get(f.size() - 1));
    }
}
