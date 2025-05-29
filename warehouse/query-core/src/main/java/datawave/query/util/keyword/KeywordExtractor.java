package datawave.query.util.keyword;

import static datawave.query.iterator.logic.KeywordExtractingIterator.MAX_CONTENT_CHARS;
import static datawave.query.iterator.logic.KeywordExtractingIterator.MAX_KEYWORDS;
import static datawave.query.iterator.logic.KeywordExtractingIterator.MAX_NGRAMS;
import static datawave.query.iterator.logic.KeywordExtractingIterator.MAX_SCORE;
import static datawave.query.iterator.logic.KeywordExtractingIterator.MIN_NGRAMS;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.query.table.parser.ContentKeyValueFactory;
import datawave.util.keyword.YakeKeywordExtractor;
import datawave.util.keyword.language.YakeLanguage;

/**
 * The KeywordExtractor serves as the glue between the keyword extracting iterator and the keyword extractor implementation. It interprets the iterator options
 * and translates them into configuration for the extractor, attempts to extract keywords from the view in defined order and then packages the keyword extractor
 * results into something that can be serialized and returned to the query logic.
 *
 */
public class KeywordExtractor {

    private static final Logger logger = LoggerFactory.getLogger(KeywordExtractor.class);

    private final List<String> preferredViews;
    private final Map<String,byte[]> foundContent;

    public static final KeywordResults EMPTY_RESULTS = new KeywordResults();

    /** minimum length for extracted keywords which are typically composed of multiple words/tokens */
    private int minNGrams = YakeKeywordExtractor.DEFAULT_MIN_NGRAMS;

    /** maximum length for extracted keywords */
    private int maxNGrams = YakeKeywordExtractor.DEFAULT_MAX_NGRAMS;

    /** maximum number of keywords to extract */
    private int keywordCount = YakeKeywordExtractor.DEFAULT_KEYWORD_COUNT;

    /** the maximum score threshold for keywords (lower score is better) */
    private float maxScoreThreshold = YakeKeywordExtractor.DEFAULT_MAX_SCORE_THRESHOLD;

    /** the maximum number of characters to process as input for keyword extraction */
    private int maxContentLength = YakeKeywordExtractor.DEFAULT_MAX_CONTENT_LENGTH;

    /** the source to record for the extraction */
    private final String source;

    /** the language to use for extraction */
    private final String language;

    YakeKeywordExtractor yakeKeywordExtractor;

    public KeywordExtractor(String source, List<String> preferredViews, Map<String,byte[]> foundContent, String language, Map<String,String> iteratorOptions) {
        this.source = source;
        this.preferredViews = preferredViews;
        this.foundContent = foundContent;

        parseOptions(iteratorOptions);

        YakeLanguage yakeLanguage = YakeLanguage.Registry.find(language);
        this.language = yakeLanguage.getLanguageName().toUpperCase(Locale.ROOT);

        //@formatter:off
        yakeKeywordExtractor = new YakeKeywordExtractor.Builder()
                .withMaxNGrams(minNGrams)
                .withMaxNGrams(maxNGrams)
                .withKeywordCount(keywordCount)
                .withMaxScoreThreshold(maxScoreThreshold)
                .withMaxContentLength(maxContentLength)
                .withLanguage(yakeLanguage)
                .build();
        //@formatter:on
    }

    public void parseOptions(Map<String,String> iteratorOptions) {

        if (iteratorOptions.containsKey(MIN_NGRAMS)) {
            minNGrams = Integer.parseInt(iteratorOptions.get(MIN_NGRAMS));
        }

        if (iteratorOptions.containsKey(MAX_NGRAMS)) {
            maxNGrams = Integer.parseInt(iteratorOptions.get(MAX_NGRAMS));
        }

        if (iteratorOptions.containsKey(MAX_KEYWORDS)) {
            keywordCount = Integer.parseInt(iteratorOptions.get(MAX_KEYWORDS));
        }

        if (iteratorOptions.containsKey(MAX_SCORE)) {
            maxScoreThreshold = Float.parseFloat(iteratorOptions.get(MAX_SCORE));
        }

        if (iteratorOptions.containsKey(MAX_CONTENT_CHARS)) {
            maxContentLength = Integer.parseInt(iteratorOptions.get(MAX_CONTENT_CHARS));
        }
    }

    public KeywordResults extractKeywords() {
        KeywordResults results = EMPTY_RESULTS;
        for (String viewName : preferredViews) {
            for (Map.Entry<String,byte[]> foundEntry : foundContent.entrySet()) {
                String[] parts = foundEntry.getKey().split(":");
                if (parts.length > 0 && viewName.equals(parts[0])) {
                    final byte[] encodedContent = foundEntry.getValue();
                    final byte[] decodedContent = ContentKeyValueFactory.decodeAndDecompressContent(encodedContent);
                    final String decodedString = new String(decodedContent, StandardCharsets.UTF_8);
                    final String input = decodedString.substring(0, Math.min(decodedString.length(), maxContentLength));
                    final LinkedHashMap<String,Double> keywords = yakeKeywordExtractor.extractKeywords(input);
                    if (logger.isDebugEnabled()) {
                        logger.debug("Extracted {} keywords from {} view.", keywords.size(), viewName);
                    }
                    if (!keywords.isEmpty()) {
                        results = new KeywordResults(source, viewName, language, keywords);
                        break;
                    }
                }
            }
        }
        return results;
    }
}
