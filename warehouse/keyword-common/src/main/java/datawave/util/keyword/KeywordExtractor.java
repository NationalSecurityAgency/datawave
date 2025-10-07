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

    private final List<String> preferredViews;
    private final Map<String,VisibleContent> foundContent;

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

    public KeywordExtractor(String source, List<String> preferredViews, Map<String,VisibleContent> foundContent, String language, Map<String,String> options) {
        this.source = source;
        this.preferredViews = preferredViews;
        this.foundContent = foundContent;

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
     * Extract keywords from the {@code foundContent} field in priority order based on the list of views in {@code preferredViews} field. Will return a
     * KeywordResults object populated with the keywords extracted from the first view in {@code preferredViews} that is found to contain content. If no
     * keywords can be extracted from all preferred views, returns an empty {@code KeywordResults} object.
     *
     * @return a KeywordResults object containing keywords from the first of the {@code preferredViews} found in {@code foundContent} that yields a non-empty
     *         set of keywords or an empty KeywordResults object if no keywords can be extracted.
     */
    @Nonnull
    public KeywordResults extractKeywords() {
        if (foundContent.isEmpty()) {
            return EMPTY_RESULTS;
        }
        KeywordResults results = EMPTY_RESULTS;
        for (String viewName : preferredViews) {
            for (Map.Entry<String,VisibleContent> foundEntry : foundContent.entrySet()) {
                if (viewName.endsWith("*")) {
                    final String truncatedName = viewName.substring(0, viewName.length() - 1);
                    if (foundEntry.getKey().startsWith(truncatedName)) {
                        results = extractKeywordsFromVisibleContent(viewName, foundEntry.getValue());
                    }
                } else if (viewName.equals(foundEntry.getKey())) {
                    results = extractKeywordsFromVisibleContent(viewName, foundEntry.getValue());
                }

                if (results != EMPTY_RESULTS) {
                    return results;
                }
            }
        }
        return EMPTY_RESULTS;
    }

    /**
     * Attempts to extract keywords from VisibleContent object provided.
     *
     * @param viewName
     *            the view name we're extracting from.
     * @param content
     *            the content associated with that view.
     * @return a KeywordResult object containing keywords extracted from the provided content, or the canonical {@code EMPTY_RESULTS} KeywordResults object if
     *         no keywords could be extracted.
     */
    @Nonnull
    private KeywordResults extractKeywordsFromVisibleContent(String viewName, VisibleContent content) {
        final LinkedHashMap<String,Double> keywords = yakeKeywordExtractor.extractKeywords(content.getContent());
        if (logger.isDebugEnabled()) {
            logger.debug("Extracted {} keywords from {} view.", keywords.size(), viewName);
        }
        return keywords.isEmpty() ? EMPTY_RESULTS : new KeywordResults(source, viewName, language, content.getVisibility(), keywords);
    }
}
