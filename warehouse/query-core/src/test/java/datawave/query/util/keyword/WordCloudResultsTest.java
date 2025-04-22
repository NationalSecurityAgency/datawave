package datawave.query.util.keyword;

import java.util.LinkedHashMap;

import org.junit.BeforeClass;
import org.junit.Test;

public class WordCloudResultsTest {
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

            "unit-testing"};
    //@formatter:on

    public static KeywordResults[] testResults;

    @BeforeClass
    public static void setupTestResults() {
        testResults = createData();
    }

    public static KeywordResults[] createData() {
        KeywordResults[] result = new KeywordResults[testData.length];
        for (int i = 0; i < testData.length; i++) {
            String content = testData[i];
            LinkedHashMap<String,Double> parsedContent = parseContent(content);
            String source = testSources[i];
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

    @Test
    public void testFullLifecycle() {
        TagCloud.Builder builder = new TagCloud.Builder();
        for (KeywordResults r : testResults) {
            builder.addResults(r);
        }
        TagCloud cloudResults = builder.build();
        String result = cloudResults.toString();
        System.out.println(result);
    }
}
