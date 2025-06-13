package datawave.util.keyword;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.util.keyword.language.BaseYakeLanguage;

/** Self-contained tests for the YakeKeywordExtractor, including methods that produce diagnostic output */
public class YakeKeywordExtractorTest {
    public static final Logger log = LoggerFactory.getLogger(YakeKeywordExtractorTest.class);

    // note: current sentence breaker doesn't handle the 'fish. 9047' boundary correctly.
    static final String DIAGNOSTIC_INPUT = "Apple bee Cat dog EAGLE fish. 9047 horse igu@na jackrabbit. Apple horse Cat igu@na EAGLE. Apple EAGLE. "
                    + "Apple. Apple bee Cat dog EAGLE fish.";

    //@formatter:off
    static final String EXPECTED_DIAGNOSTIC_OUTPUT =
            "{apple=0.1137, apple bee cat=0.3064, apple eagle=0.3224, bee cat=0.2834, cat=0.1493, cat dog=0.2486, " +
                    "cat dog eagle=0.1608, dog eagle=0.2331, eagle=0.1072, eagle fish=0.2615}";
    //@formatter:on

    static final String TEST_NEWS_INPUT_INPUT = "Sources tell us that Google is acquiring Kaggle a platform that hosts data "
                    + "science and machine learning competitions. Details about the transaction remain somewhat vague but "
                    + "given that Google is hosting its Cloud Next conference in San Francisco this week the official "
                    + "announcement could come as early as tomorrow.";

    static final LinkedHashMap<String,Double> EXPECTED_NEWS_OUTPUT = new LinkedHashMap<>();
    static {
        LinkedHashMap<String,Double> m = EXPECTED_NEWS_OUTPUT;
        m.put("acquiring kaggle", 0.4602);
        m.put("cloud next", 0.3884);
        m.put("cloud next conference", 0.5552);
        m.put("san francisco", 0.3884);
    }

    @Test
    public void testSteps() {
        YakeKeywordExtractor yake = new YakeKeywordExtractor.Builder().build();

        log.info(" --- sentences --- ");
        List<String> sentences = yake.breakSentences(DIAGNOSTIC_INPUT);
        sentences.forEach(log::info);

        log.info(" --- tokens --- ");
        List<List<String>> tokenizedSentences = yake.tokenizeSentences(sentences);
        tokenizedSentences.forEach(i -> log.info(i.toString()));

        log.info(" --- ngrams --- ");
        final List<List<String>> ngrams = YakeKeywordExtractor.calculateNGrams(tokenizedSentences, yake.getMinNGrams(), yake.getMaxNGrams());
        ngrams.forEach(i -> log.info(i.toString()));

        log.info(" --- left co-occurrence map --- ");
        final Map<String,Map<String,Integer>> leftCoMap = yake.buildCoOccurrence(ngrams, true);
        log.info(leftCoMap.toString());

        log.info(" --- right co-occurrence map --- ");
        final Map<String,Map<String,Integer>> rightCoMap = yake.buildCoOccurrence(ngrams, false);
        log.info(rightCoMap.toString());

        log.info(" --- basic stats --- ");
        List<TokenValue> basicStats = YakeKeywordExtractor.calculateBasicStats(tokenizedSentences);
        basicStats.forEach(i -> log.info(i.toString()));

        log.info(" --- assigned tags --- ");
        List<TaggedToken> taggedSentences = YakeKeywordExtractor.assignTags(basicStats);
        taggedSentences.forEach(i -> log.info(i.toString()));

        log.info(" --- tokens --- ");
        List<YakeToken> tokens = YakeKeywordExtractor.calculateTokenScores(basicStats, taggedSentences, leftCoMap, rightCoMap);
        tokens.forEach(i -> log.info(i.toString()));

        log.info(" --- candidate keywords --- ");
        Map<String,Long> candidateKeywords = yake.calculateCandidateKeywords(taggedSentences);
        candidateKeywords.entrySet().forEach(i -> log.info(i.toString()));

        log.info(" --- keywords --- ");
        Map<String,Double> keywords = yake.calculateKeywords(candidateKeywords, tokens);
        keywords.entrySet().forEach(i -> log.info(i.toString()));

        assertEquals(10, keywords.size());
        assertEquals(EXPECTED_DIAGNOSTIC_OUTPUT, keywords.toString());
    }

    @Test
    public void testDiagnosticInput() {
        YakeKeywordExtractor keywordExtractor = new YakeKeywordExtractor.Builder().build();
        Map<String,Double> keywords = keywordExtractor.extractKeywords(DIAGNOSTIC_INPUT);
        keywords.entrySet().forEach(i -> log.info(i.toString()));

        assertEquals(10, keywords.size());
        assertEquals(EXPECTED_DIAGNOSTIC_OUTPUT, keywords.toString());
    }

    @Test
    public void testInputWithConfiguration() {
        //@formatter:off
        YakeKeywordExtractor keywordExtractor = new YakeKeywordExtractor.Builder()
                .withMaxScoreThreshold(0.6f)
                .withMinNGrams(2)
                .withMaxNGrams(3)
                .withKeywordCount(10)
                .withLanguage(BaseYakeLanguage.ENGLISH)
                .build();
        //@formatter:on

        Map<String,Double> keywords = keywordExtractor.extractKeywords(TEST_NEWS_INPUT_INPUT);
        keywords.entrySet().forEach(i -> log.info(i.toString()));
        assertEquals(4, keywords.size());
        assertEquals(EXPECTED_NEWS_OUTPUT, keywords);
    }
}
