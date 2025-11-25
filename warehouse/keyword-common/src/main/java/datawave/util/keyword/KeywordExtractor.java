package datawave.util.keyword;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.util.keyword.language.YakeLanguage;

/**
 * The KeywordExtractor serves as the glue between the keyword extracting iterator and the keyword extractor implementation. It interprets the iterator options
 * and translates them into configuration for the extractor, attempts to extract keywords from the view in defined order and then packages the keyword extractor
 * results into something that can be serialized and returned to the query logic. <br>
 * <br>
 * When using wildcard view names, the first view found when traversing the keys will be the one to get processed.
 *
 */
public class KeywordExtractor {
    private static final Logger logger = LoggerFactory.getLogger(KeywordExtractor.class);

    public static final String MIN_NGRAMS = "min.ngram.count";
    public static final String MAX_NGRAMS = "max.ngram.count";
    public static final String MAX_KEYWORDS = "max.keyword.count";
    public static final String MAX_SCORE = "max.score";
    public static final String MAX_CONTENT_CHARS = "max.content.chars";

    private final List<Map.Entry<String,VisibleContent>> orderedContent;

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

    /**
     * Creates a {@code KeywordExtractor}, a lightweight wrapper for {@link YakeKeywordExtractor} that handles selecting from multiple sources of content,
     * choosing a language for keyword processing and property-based configuration.
     *
     * @param source
     *            used to name the source for the content from which we'll extract keywords, carried forward into the results object potentially used for
     *            display or grouping multiple keyword sets.
     * @param orderedContent
     *            an ordered list of content. The extractor will attempt to extract keywords from each item and will stop after the first item it successfully
     *            extracts keywords from.
     * @param language
     *            the language to use when extracting content, if the language can't be found in the {@link YakeLanguage.Registry}, it will default to english.
     *            This language will also be included in the results object.
     * @param options
     *            values for the options related to {@code MIN_NGRAMS}, {@code MAX_NGRAMS}, {@code MAX_KEYWORDS}, {@code MAX_SCORE} or
     *            {@code MAX_CONTENT_CHARS}.
     */
    public KeywordExtractor(String source, List<Map.Entry<String,VisibleContent>> orderedContent, String language, Map<String,String> options) {
        this.source = source;
        this.orderedContent = orderedContent;

        parseOptions(options);

        this.language = language;
        YakeLanguage yakeLanguage = YakeLanguage.Registry.find(language);

        logger.debug("Input language was {}, resolved language for YAKE extraction is {}", this.language, yakeLanguage.getLanguageName());

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

        if (iteratorOptions.containsKey(KeywordExtractor.MAX_CONTENT_CHARS)) {
            maxContentLength = Integer.parseInt(iteratorOptions.get(KeywordExtractor.MAX_CONTENT_CHARS));
        }
    }

    /**
     * Extract keywords from the {@code orderedContent} field. Will return a KeywordResults object populated with the keywords extracted from the first view in
     * {@code orderedContent} that has a successful extraction. If no keywords can be extracted from all preferred views, returns an empty
     * {@code KeywordResults} object.
     *
     * @return a KeywordResults object containing keywords from the first view found in {@code orderedContent} that yields a non-empty set of keywords or an
     *         empty KeywordResults object if no keywords can be extracted.
     */
    @Nonnull
    public KeywordResults extractKeywords() {
        if (orderedContent.isEmpty()) {
            return EMPTY_RESULTS;
        }

        for (Map.Entry<String,VisibleContent> entry : orderedContent) {
            String view = entry.getKey();
            VisibleContent content = entry.getValue();
            // Attempts to extract keywords from VisibleContent object provided.
            final LinkedHashMap<String,Double> keywords = yakeKeywordExtractor.extractKeywords(content.getContent());
            if (logger.isDebugEnabled()) {
                logger.debug("Extracted {} keywords from {} view.", keywords.size(), view);
            }
            if (!keywords.isEmpty()) {
                return new KeywordResults(source, view, language, content.getVisibility(), keywords);
            }
        }

        return EMPTY_RESULTS;
    }
}
