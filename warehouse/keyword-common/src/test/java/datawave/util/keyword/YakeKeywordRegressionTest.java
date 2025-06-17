package datawave.util.keyword;

import static datawave.util.keyword.SimpleRegressionUtil.getExpectedFileForTest;
import static datawave.util.keyword.SimpleRegressionUtil.getInputFileForTest;
import static datawave.util.keyword.SimpleRegressionUtil.getTestsForClassData;
import static datawave.util.keyword.SimpleRegressionUtil.writeTestOutput;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import datawave.util.keyword.language.BaseYakeLanguage;
import datawave.util.keyword.language.YakeLanguage;

/**
 * Functional tests for the Yake Keyword Extraction algorithm that compare results to files stored in the test resources
 */
public class YakeKeywordRegressionTest {
    @ParameterizedTest
    @MethodSource({"findTests"})
    public void regressionTest(String testName) throws IOException {
        File inputFile = getInputFileForTest(testName, YakeKeywordRegressionTest.class);
        File expectedFile = getExpectedFileForTest(testName, YakeKeywordRegressionTest.class);

        String input = IOUtils.toString(inputFile.toURI(), StandardCharsets.UTF_8);
        String expectedRaw = IOUtils.toString(expectedFile.toURI(), StandardCharsets.UTF_8);
        String languageCode = getLanguageCodeFromFilename(inputFile.getName());

        String output = runYakeKeywordExtractor(input, languageCode);

        writeTestOutput(output, testName, YakeKeywordRegressionTest.class);
        assertEquals(expectedRaw, output);
    }

    public String runYakeKeywordExtractor(String input, String languageCode) {
        YakeLanguage language = BaseYakeLanguage.forLanguageCode(languageCode);
        if (language == null) {
            language = BaseYakeLanguage.ENGLISH;
        }

        //@formatter:off
        YakeKeywordExtractor yake = new YakeKeywordExtractor.Builder()
                .withMaxScoreThreshold(0.6f)
                .withMinNGrams(2)
                .withMaxNGrams(3)
                .withKeywordCount(10)
                .withMaxContentLength(32768)
                .withLanguage(language)
                .build();
        //@formatter:on

        Map<String,Double> keywords = yake.extractKeywords(input);
        StringBuilder output = new StringBuilder();
        keywords.entrySet().forEach(i -> output.append(i.toString()).append('\n'));
        return output.toString();
    }

    public static List<String> findTests() {
        return getTestsForClassData(YakeKeywordRegressionTest.class);
    }

    public String getLanguageCodeFromFilename(String filename) {
        return filename.substring(0, 2);
    }
}
