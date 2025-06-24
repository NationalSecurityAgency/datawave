package datawave.util.keyword;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.icu.text.BreakIterator;

import datawave.util.keyword.language.BaseYakeLanguage;
import datawave.util.keyword.language.YakeLanguage;

/**
 * An implementation of the YAKE! (Yet Another Keyword Extractor) keyword extraction algorithm that performs unsupervised extraction of keywords from a single
 * text without requiring a model or external document corpus.
 * <p>
 * To use this implementation, create an instance with the desired settings, and then call the {@link #extractKeywords(String)} method which will return a map
 * of extracted keywords and scores. Lower scores are better.
 * </p>
 * <p>
 * To learn about the YAKE! algorithm, see the following resources:
 * </p>
 * <ul>
 * <li><a href= "https://repositorio.inesctec.pt/server/api/core/bitstreams/ef121a01-a0a6-4be8-945d-3324a58fc944/content">YAKE! Collection-Independent Automatic
 * Keyword Extractor (pdf on https://repositorio.inesctec.pt/)</a></li>
 * <li><a href="https://www.sciencedirect.com/science/article/pii/S0020025519308588">YAKE! Keyword extraction from single documents using multiple local
 * features</a></li>
 * </ul>
 * <p>
 * This implementation borrows heavily from the <a href=
 * "https://github.com/JohnSnowLabs/spark-nlp/blob/master/src/main/scala/com/johnsnowlabs/nlp/annotators/keyword/yake/YakeKeywordExtraction.scala">Scala YAKE!
 * implementation</a> in SparkNLP on GitHub, but a canonical <a href="https://github.com/LIAAD/yake">Python implementation of YAKE</a> is available on GitHub as
 * well.
 * </p>
 */
public class YakeKeywordExtractor {
    public static final int DEFAULT_MIN_NGRAMS = 1;
    public static final int DEFAULT_MAX_NGRAMS = 3;
    public static final int DEFAULT_KEYWORD_COUNT = 10;
    public static final float DEFAULT_MAX_SCORE_THRESHOLD = 0.6f;
    public static final int DEFAULT_MAX_CONTENT_LENGTH = 32768;

    private static final Logger log = LoggerFactory.getLogger(YakeKeywordExtractor.class);

    /** minimum length for extracted keywords which are typically composed of multiple words/tokens */
    private final int minNGrams;

    /** maximum length for extracted keywords */
    private final int maxNGrams;

    /** maximum number of keywords to extract */
    private final int keywordCount;

    /** the maximum score threshold for keywords (lower score is better) */
    private final float maxScoreThreshold;

    /** the maximum number of characters to process as input for keyword extraction */
    private final int maxContentLength;

    /** a set of words to ignore, language dependent */
    private final Set<String> stopwords;

    /** an ICU4J BreakIterator used to split input into sentences, language dependent */
    private final BreakIterator sentenceBreakIterator;

    // use the builder to construct.
    private YakeKeywordExtractor(int minNGrams, int maxNGrams, int keywordCount, float maxScoreThreshold, int maxContentLength, YakeLanguage language) {
        this.minNGrams = minNGrams;
        this.maxNGrams = maxNGrams;
        this.keywordCount = keywordCount;
        this.maxScoreThreshold = maxScoreThreshold;
        this.maxContentLength = maxContentLength;
        this.stopwords = language.getStopwords();
        this.sentenceBreakIterator = language.getSentenceBreakIterator();
    }

    public int getKeywordCount() {
        return keywordCount;
    }

    public int getMaxContentLength() {
        return maxContentLength;
    }

    public int getMaxNGrams() {
        return maxNGrams;
    }

    public float getMaxScoreThreshold() {
        return maxScoreThreshold;
    }

    public int getMinNGrams() {
        return minNGrams;
    }

    public BreakIterator getSentenceBreakIterator() {
        return sentenceBreakIterator;
    }

    public Set<String> getStopwords() {
        return stopwords;
    }

    /**
     * Main entrypoint. Extract keywords from the string provided.
     *
     * @param input
     *            a string to process.
     * @return a map of keywords to scores, ranked from highest to lowest score.
     */
    public LinkedHashMap<String,Double> extractKeywords(String input) {
        // trim the input to the max content length if configured.
        final String trimmedInput = (maxContentLength > 0 && input.length() > maxContentLength) ? input.substring(0, maxContentLength) : input;
        // remove newlines because they are interpreted as sentence breaks.
        final String normalizedInput = trimmedInput.replaceAll("[\\r\\n]+", " ");
        final List<String> sentenceList = breakSentences(normalizedInput);
        final List<List<String>> tokenizedSentences = tokenizeSentences(sentenceList);
        return extractKeywords(tokenizedSentences);
    }

    /**
     * Extract keywords from a collection of tokenized sentences. Ties each of the processing steps together into a complete workflow.
     *
     * @param tokenizedSentences
     *            a list of lists; sentences that are already broken into tokens.
     * @return a map of keywords to scores.
     */
    protected LinkedHashMap<String,Double> extractKeywords(List<List<String>> tokenizedSentences) {
        List<TokenValue> basicStats = calculateBasicStats(tokenizedSentences);
        List<List<String>> sentences = downCaseSentences(tokenizedSentences);
        final List<List<String>> ngrams = calculateNGrams(sentences, minNGrams, maxNGrams);
        Map<String,Map<String,Integer>> coLeft = buildCoOccurrence(ngrams, true);
        Map<String,Map<String,Integer>> coRight = buildCoOccurrence(ngrams, false);
        List<TaggedToken> taggedSentences = assignTags(basicStats);
        List<YakeToken> tokens = calculateTokenScores(basicStats, taggedSentences, coLeft, coRight);
        Map<String,Long> candidateKeywords = calculateCandidateKeywords(taggedSentences);
        return calculateKeywords(candidateKeywords, tokens);
    }

    /**
     * Given a set of tokenized sentences, generate a list of tuples containing (word, sentence id) pairs. The word is unchanged from its original form.
     *
     * @param tokenizedSentences
     *            the list of tokenized sentences
     * @return tuple of (word, sentence id pairs).
     */
    protected static List<TokenValue> calculateBasicStats(List<List<String>> tokenizedSentences) {
        final List<TokenValue> resultsFlattenedIndexed = new ArrayList<>();
        for (int i = 0; i < tokenizedSentences.size(); i++) {
            for (String token : tokenizedSentences.get(i)) {
                resultsFlattenedIndexed.add(new TokenValue(token, i));
            }
        }
        return resultsFlattenedIndexed;
    }

    /**
     * Down-cases each of the tokens received using {@link String#toLowerCase()} and returns the result
     *
     * @param tokenizedSentences
     *            a list of lists of sentence tokens to process
     * @return a list-of-lists of down-cased tokens.
     */
    private static List<List<String>> downCaseSentences(List<List<String>> tokenizedSentences) {
        return tokenizedSentences.stream().map(x -> x.stream().map(String::toLowerCase).collect(Collectors.toList())).collect(Collectors.toList());
    }

    /**
     * Given a list of word, sentence id pairs as generated by {@link #calculateBasicStats ( List)}, generate a list of (word, sentence, position, tag), where
     * the tag indicates the 'type' of word we're dealing with as produced by the {@link #calculateTag ( String, int)} method.
     *
     * @param basicStats
     *            (word, sentence id pairs) tuples.
     * @return (word, sentence, position, tag) tuples.
     */
    protected static List<TaggedToken> assignTags(List<TokenValue> basicStats) {
        List<TaggedToken> tags = new ArrayList<>();
        int sentenceId = 0;
        int position = 0;

        for (TokenValue e : basicStats) {
            if (e.getValue() != sentenceId) {
                sentenceId += 1;
                position = 0;
            }
            String tag = calculateTag(e.getToken(), position);
            position += 1;
            tags.add(new TaggedToken(e.getToken(), sentenceId, position, tag));
        }

        return tags;
    }

    /**
     * Obtain a tag for a word based on its characteristics and position in the sentence. Possible tags are:
     * <dl>
     * <dt>a</dt>
     * <dd>all uppercase</dd>
     * <dt>d</dt>
     * <dd>all digits</dd>
     * <dt>n</dt>
     * <dd>initial uppercase, not at start of sentence (possibly a proper noun)</dd>
     * <dt>p</dt>
     * <dd>plain word, nothing special about it</dd>
     * <dt>u</dt>
     * <dd>unspecified</dd>
     * </dl>
     * These 'features' will be used in the overall keyword selection algorithm.
     *
     * @param word
     *            the word to analyze.
     * @param position
     *            the position of the word in input sentence
     * @return the tag for the word.
     */
    protected static String calculateTag(String word, int position) {
        try {
            String word2 = word.replace(",", "");
            Float.parseFloat(word2);
            return "d";
        } catch (NumberFormatException e) {
            int wordLength = word.length();
            int digitCount = countDigits(word, wordLength);
            int alphaCount = countLetters(word, wordLength);

            if ((digitCount > 0 && alphaCount > 0) || (digitCount == 0 && alphaCount == 0) || (digitCount + alphaCount != wordLength)) {
                return "u";
            } else {
                int upperCount = countUpper(word, wordLength);

                if (wordLength == upperCount) {
                    return "a";
                } else if (upperCount == 1 && wordLength > 1 && Character.isUpperCase(word.charAt(0)) && position > 0) {
                    return "n";
                }
            }

            return "p";
        }
    }

    /**
     * Used in {@link #calculateTokenScores( List, List, Map, Map)} to find the median sentence offset for a word, another feature for keyword selection
     *
     * @param sentenceOffsets
     *            a list of sentence offsets for a word, used to calculate median
     * @return the median sentence offset for a word
     */
    private static int calculateMedianSentenceOffset(List<Integer> sentenceOffsets) {
        // In order if you are not sure that 'seq' is sorted
        sentenceOffsets.sort(Integer::compareTo);
        int size = sentenceOffsets.size();
        int pos = size / 2;

        if (size % 2 == 1)
            return (sentenceOffsets.get(pos));
        else {
            int up = sentenceOffsets.get(pos);
            int down = sentenceOffsets.get(pos - 1);
            return (up + down) / 2;
        }
    }

    /**
     * Count the digits in a word, a feature used for tagging a word in {@link #calculateTag ( String, int)}.
     *
     * @param word
     *            the word to analyze.
     * @param length
     *            the maximum number of characters to inspect.
     * @return the number of digits found in the word as determined by {@link Character#isDigit(char)}.
     */
    private static int countDigits(String word, int length) {
        int count = 0;
        for (int i = 0; i < length; i++) {
            if (Character.isDigit(word.charAt(i))) {
                count++;
            }
        }
        return count;
    }

    /**
     * Count the letters in a word, a feature used for tagging a word in {@link #calculateTag ( String, int)}.
     *
     * @param word
     *            the word to analyze.
     * @param length
     *            the maximum number of characters to inspect.
     * @return the number of letters found in the word as determined by {@link Character#isLetter(char)}.
     */
    private static int countLetters(String word, int length) {
        int count = 0;
        for (int i = 0; i < length; i++) {
            if (Character.isLetter(word.charAt(i))) {
                count++;
            }
        }
        return count;
    }

    /**
     * Count the upper case characters in a word, a feature used for tagging a word in {@link #calculateTag ( String, int)}
     *
     * @param word
     *            the word to analyze.
     * @param length
     *            the maximum number of characters to inspect.
     * @return the number of upper case letters found in the word as determined by {@link Character#isUpperCase(char)}.
     */
    private static int countUpper(String word, int length) {
        int count = 0;
        for (int i = 0; i < length; i++) {
            if (Character.isUpperCase(word.charAt(i))) {
                count++;
            }
        }
        return count;
    }

    /**
     * Given a list of tokenized sentences, generate a list of candidate keywords from those sentences following the specified ngram sizes. Ngrams are generated
     * using a sliding window over the input text. We will always return at least one ngram per sentence.
     *
     * @param sentences
     *            a list of sentences each split into words (should be down-cased).
     * @param minNGrams
     *            the minimum ngram size.
     * @param maxNGrams
     *            the maximum ngram size.
     * @return a list of ngrams generated from the input sentences ordered based on where they appeared in the input sentences. These ngrams are not
     *         deduplicated and the words in these ngrams are not normalized in any way. This generates ngrams of every length between min and max ngram size.
     */
    protected static List<List<String>> calculateNGrams(List<List<String>> sentences, int minNGrams, int maxNGrams) {
        final List<List<String>> ngrams = new ArrayList<>();
        for (List<String> tokens : sentences) { // for each sentence
            final int limit = tokens.size();
            for (int start = 0; start < limit; start++) { // for each token
                for (int length = minNGrams; length <= maxNGrams; length++) { // for each desired ngram size
                    int end = findEnd(start, length, limit);
                    if (end > start) {
                        ngrams.add(tokens.subList(start, end));
                    }
                }
            }
        }
        return ngrams;
    }

    /**
     * Identifies the end offset in a range that meets the specified start and size while respecting the specified limit. If the specified size is larger than
     * the limit, end will cover the entire range. Otherwise, return an end that will satisfy the start+size as long as that does not exceed the limit. In that
     * case, return an end that matches the start.
     * <p>
     * Generally this can be used to create 'windows' into a collection of the specified size which mimics Scala's slide() behavior.
     * </p>
     *
     * @param start
     *            starting position
     * @param size
     *            size of the desired window
     * @param limit
     *            the largest possible position in the window
     * @return the end position of the window, respecting the requested size and limit.
     */
    private static int findEnd(int start, int size, int limit) {
        int end; // the end of the window
        if (size > limit) {
            // if the window size is larger than the number of positions, we'll at
            // least return and end that covers the existing tokens, this mimics scala's slide()
            // behavior.
            end = limit;
        } else {
            // otherwise, if there are enough positions remaining to generate a full window,
            // return an end window.
            end = start + size;

            // if the end exceeds the limit, avoid generating any range.
            if (end > limit) {
                end = start;
            }
        }
        return end;
    }

    /**
     * Build a co-occurrence matrix of words based on a set of ngrams. Words are down-cased prior to adding them to the matrix. We track the max sentence of
     * co-occurrence instances for each pair of words.
     *
     * @param ngrams
     *            the set of ngrams to analyze
     * @param left
     *            if true, the matrix is based on preceding words (e.g., those that appear to the left of the target word.) if false, the matrix is based on
     *            following words (e.g., those that appear to the right of the target word.)
     * @return a map where the key is a word and the value is the list of words that co-occur with that word. For each co-occurrence, we track the max sentence
     *         id where that co-occurrence happened.
     */
    protected Map<String,Map<String,Integer>> buildCoOccurrence(List<List<String>> ngrams, boolean left) {
        final Map<String,Map<String,Integer>> coMap = new HashMap<>();
        ngrams.forEach(elements -> {
            final String head = left ? elements.get(0).toLowerCase() : elements.get(elements.size() - 1).toLowerCase();

            elements.forEach(element -> {
                final String el = element.toLowerCase();
                if (!el.equals(head)) {
                    final Map<String,Integer> map = coMap.computeIfAbsent(head, x -> new HashMap<>());
                    final int value = coMap.get(head).computeIfAbsent(el, x -> 0);
                    map.put(el, value + 1);
                }
            });
        });
        return coMap;
    }

    /**
     * This pulls together all features to determine the token statistics for potential keywords.
     *
     * @param basicStats
     *            word statistics generated by {@link #calculateBasicStats ( List)}
     * @param tags
     *            data structure containing tagged words generated by {@link #assignTags( List)}
     * @param coLeft
     *            left co-occurrence matrix generated by {@link #buildCoOccurrence ( List, boolean)}
     * @param coRight
     *            right co-occurrence matrix generated by {@link #buildCoOccurrence ( List, boolean)}
     * @return a list of scored YakeToken objects containing the statistics for each potential keyword
     */
    protected static List<YakeToken> calculateTokenScores(List<TokenValue> basicStats, List<TaggedToken> tags, final Map<String,Map<String,Integer>> coLeft,
                    final Map<String,Map<String,Integer>> coRight) {

        if (basicStats.isEmpty()) {
            return Collections.emptyList();
        }

        // group stats by token to calculate stats and counts.
        final Map<String,List<TokenValue>> tokenMap = basicStats.stream().collect(Collectors.groupingBy(TokenValue::getLowercaseToken));

        // calculate stats - average tf, tf standard deviation and max tf across all tokens.
        final int[] tokenFrequencies = tokenMap.values().stream().mapToInt(List::size).toArray();
        final double sum = IntStream.of(tokenFrequencies).sum();
        final double count = tokenMap.size();
        final double avg = sum / count;
        final double std = Math.sqrt(IntStream.of(tokenFrequencies).mapToDouble(a -> Math.pow(((double) a) - avg, 2)).sum() / count);
        final double maxTF = IntStream.of(tokenFrequencies).mapToDouble(x -> (double) x).max().orElse(0);
        final int numSentences = basicStats.stream().mapToInt(TokenValue::getValue).max().orElse(0) + 1;

        if (log.isDebugEnabled()) {
            log.debug(" --- matrices and stats --- ");
            log.debug("coLeft: {}", coLeft);
            log.debug("coRight: {}", coRight);
            log.debug("sum: {}, count: {}, avg: {}, std: {}, maxTF: {}, numSentences: {}", sum, count, avg, avg, maxTF, numSentences);
            log.debug(" --- ");
        }

        // create token objects.
        final Stream<YakeToken> tokenStream = tokenMap.values().stream().map(x -> new TokenValue(x.get(x.size() - 1).getToken(), x.size()))
                        .map(x -> new YakeToken(x.getLowercaseToken(), x.getValue(), numSentences, avg, std, maxTF,
                                        coLeft.getOrDefault(x.getLowercaseToken(), Collections.emptyMap()),
                                        coRight.getOrDefault(x.getLowercaseToken(), Collections.emptyMap())));
        final List<YakeToken> tokens = tokenStream.collect(Collectors.toList());

        // count a tags for each token
        final Stream<TokenValue> aTags = tags.stream().filter(x -> x.getTag().equals("a")).collect(Collectors.groupingBy(TaggedToken::getLowercaseToken))
                        .entrySet().stream().map(e -> new TokenValue(e.getKey(), e.getValue().size()));
        aTags.forEach(x -> tokens.stream().filter(y -> y.token.equals(x.getToken())).forEach(y -> y.aCount = x.getValue()));

        // count n tags for each token
        final Stream<TokenValue> nTags = tags.stream().filter(x -> x.getTag().equals("n")).collect(Collectors.groupingBy(TaggedToken::getLowercaseToken))
                        .entrySet().stream().map(e -> new TokenValue(e.getKey(), e.getValue().size()));
        nTags.forEach(x -> tokens.stream().filter(y -> y.token.equals(x.getToken())).forEach(y -> y.nCount = x.getValue()));

        // calculate median sentence offset for each token
        final Stream<TokenValue> medianTags = tags.stream().collect(Collectors.groupingBy(TaggedToken::getLowercaseToken)).entrySet().stream()
                        .map(e -> new TokenValue(e.getKey(),
                                        calculateMedianSentenceOffset(e.getValue().stream().map(TaggedToken::getSentenceId).collect(Collectors.toList()))));
        medianTags.forEach(x -> tokens.stream().filter(y -> y.token.equals(x.getToken())).forEach(y -> y.medianSentenceOffset = x.getValue()));

        // calculate number of sentences for each token
        final Stream<TokenValue> sizeTags = tags.stream().collect(Collectors.groupingBy(TaggedToken::getLowercaseToken)).entrySet().stream()
                        .map(e -> new TokenValue(e.getKey(), e.getValue().size()));
        sizeTags.forEach(x -> tokens.stream().filter(y -> y.token.equals(x.getToken())).forEach(y -> y.numberOfSentences = x.getValue()));

        return tokens;
    }

    /**
     * Calculate the list of candidate keywords using the scored tokens.
     *
     * @param sentences
     *            the list of tagged tokens generated by {@link #assignTags( List)}
     * @return a Map of candidate keywords - the keyword is the key, the score is the value
     */
    protected Map<String,Long> calculateCandidateKeywords(List<TaggedToken> sentences) {
        final Set<String> stopwords = this.getStopwords();

        // group tokens by sentences to prevent forming ngrams across sentences.
        final Map<Integer,List<TaggedToken>> tokensBySentence = sentences.stream().collect(Collectors.groupingBy(TaggedToken::getSentenceId));

        // build candidate keywords from ranges of tokens using sliding ngram windows
        final List<String> candidateKeywords = new ArrayList<>();
        for (List<TaggedToken> sentenceTokens : tokensBySentence.values()) {
            final int limit = sentenceTokens.size();
            for (int start = 0; start < limit; start++) { // for each token in sentence
                NGRAM_SIZE: for (int length = minNGrams; length <= maxNGrams; length++) { // for each desired ngram size
                    int end = findEnd(start, length, limit);
                    if (end > start) {
                        // skip candidate keywords that start with a stopword.
                        if (stopwords.contains(sentenceTokens.get(start).getLowercaseToken())) {
                            continue;
                        }

                        // skip candidate keywords that end with a stopword
                        if (stopwords.contains(sentenceTokens.get(end - 1).getLowercaseToken())) {
                            continue;
                        }

                        // skip candidate keywords that contain a 'u' or 'd' token
                        for (int pos = start; pos < end; pos++) {
                            TaggedToken t = sentenceTokens.get(pos);
                            if (t.getTag().equals("u") || t.getTag().equals("d")) {
                                continue NGRAM_SIZE;
                            }
                        }

                        // build the candidate keyword string (comma-delimited tokens)
                        final StringBuilder b = new StringBuilder();
                        for (int pos = start; pos < end; pos++) {
                            b.append(sentenceTokens.get(pos).getLowercaseToken()).append(",");
                        }
                        if (b.length() > 0) {
                            b.setLength(b.length() - 1);
                        }
                        candidateKeywords.add(b.toString());
                    }
                }
            }
        }

        // group candidate keywords by frequency
        return candidateKeywords.stream().collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
    }

    /**
     * Calculate the final set of scored keywords, using the results of {@link #calculateTokenScores( List, List, Map, Map)} and
     * {@link #calculateTokenScores( List, List, Map, Map)}, performing the final scoring operation and filtering based on threshold and max token count.
     *
     * @param candidateKeywords
     *            the list of scored candidate keywords generated by {@link #calculateCandidateKeywords ( List)}
     * @param tokens
     *            the scored tokens generated by {@link #calculateTokenScores( List, List, Map, Map)}
     * @return a LinkedHashMap of keywords, sorted alphabetically. The key is the keyword, value is the score.
     */
    protected LinkedHashMap<String,Double> calculateKeywords(Map<String,Long> candidateKeywords, List<YakeToken> tokens) {

        Stream<TokenScore> kwStream = candidateKeywords.entrySet().stream().map(e -> {
            double product_s = 1;
            double sum_s = 0;

            final String[] words = e.getKey().split(",");
            final long frequency = e.getValue();

            for (int ind = 0; ind < words.length; ind++) {
                final String target = words[ind];

                List<YakeToken> word = tokens.stream().filter(k -> k.token.equals(target)).collect(Collectors.toList());
                if (!stopwords.contains(target) && !word.isEmpty()) {
                    YakeToken t = word.get(0);
                    double tScore = t.tScore();
                    product_s *= tScore;
                    sum_s += tScore;
                } else {
                    double prev_prob = 0.0;

                    if (ind > 0) { // previous token available
                        final String prev_target = words[ind - 1];
                        List<YakeToken> prev_tokens = tokens.stream().filter(k -> k.token.equals(prev_target)).collect(Collectors.toList());
                        if (!prev_tokens.isEmpty()) {
                            final YakeToken prev_token = prev_tokens.get(0);
                            double prev_co = (double) prev_token.rightCo.getOrDefault(target, 0);
                            prev_prob = prev_co / prev_token.termFrequency;
                        }
                    }

                    double next_prob = 0.0;

                    if (ind < (words.length - 1)) { // next token available
                        final String next_target = words[ind + 1];
                        List<YakeToken> next_tokens = tokens.stream().filter(k -> k.token.equals(target)).collect(Collectors.toList());
                        if (!next_tokens.isEmpty()) {
                            final YakeToken next_token = next_tokens.get(0);
                            double next_co = (double) next_token.rightCo.getOrDefault(next_target, 0);
                            next_prob = next_co / next_token.termFrequency;
                        }
                    }

                    double bi_probability = prev_prob * next_prob;
                    product_s = product_s * (1 + (1 - bi_probability));
                    sum_s -= (1 - bi_probability);
                }
            }

            double S_kw = product_s / (frequency * (1 + sum_s));
            S_kw = Math.round(S_kw * 10000.0) / 10000.0; // keep only the last 4 decimal places.
            return new TokenScore(String.join(" ", words), S_kw);
        });

        return selectFinalKeywords(kwStream);
    }

    /**
     * Given a list of scored keywords, apply the score and keyword count thresholds and return a list of the final set of keywords.
     *
     * @param kwStream
     *            the stream of scored keywords to evaluate.
     * @return a LinkedHashMap of selected keywords and scores, sorted by keyword, ascending.
     */
    protected LinkedHashMap<String,Double> selectFinalKeywords(Stream<TokenScore> kwStream) {

        // filter against the threshold if one is set.
        final Stream<TokenScore> filteredStream = this.getMaxScoreThreshold() > 0 ? kwStream.filter(x -> x.getScore() <= this.getMaxScoreThreshold())
                        : kwStream;

        final Stream<TokenScore> sortedByScoreAscending = filteredStream.sorted(Comparator.comparing(TokenScore::getScore));

        // todo: implement similarity-based deduplication here

        // limit the number of keywords if a limit is set.
        final Stream<TokenScore> limitedStream = this.getKeywordCount() > 0 ? sortedByScoreAscending.limit(this.getKeywordCount()) : sortedByScoreAscending;

        return limitedStream.sorted(Comparator.comparing(TokenScore::getToken)) // by keyword ascending
                        .collect(Collectors.toMap(TokenScore::getToken, TokenScore::getScore, Double::sum, LinkedHashMap::new));
    }

    /**
     * Given a list of sentences, generate tokens by splitting on spaces Doesn't do anything other than split the sentences on spaces.
     *
     * @param sentences
     *            the sentences to tokenize.
     * @return a list-of-lists, where each item in the top level list * corresponds to a sentence, and each item in the lower level lists corresponds to tokens
     */
    public List<List<String>> tokenizeSentences(List<String> sentences) {
        return sentences.stream().map(this::tokenizeSentence).filter(Predicate.not(List::isEmpty)).collect(Collectors.toList());
    }

    /**
     * Given a sentence, generate tokens by splitting on spaces
     *
     * @param sentence
     *            the sentence to tokenize
     * @return a list of tokens from the sentence
     */
    public List<String> tokenizeSentence(String sentence) {
        // todo: investigate improved tokenization
        if (log.isDebugEnabled()) {
            log.debug("--- begin ---");
            log.debug("'''{}'''", sentence);
            log.debug("--- end ---\n");
        }

        //@formatter:off
        final List<String> sentenceTokens = Arrays.stream(sentence.split("\\s+")) // split on space
                .filter(Predicate.not(String::isEmpty))
                .map(s -> s.endsWith(".") ? s.substring(0, s.length()-1) : s) // remove trailing periods
                .collect(Collectors.toList());
        //@formatter:on

        if (!sentenceTokens.isEmpty()) {
            sentenceTokens.add("."); // always as a placeholder for end-of-sentence.
        }
        return sentenceTokens;
    }

    /**
     * Given text input, break that text into a series of sentences based on the configured language.
     *
     * @param input
     *            input to break into sentences.
     * @return a list of sentences.
     */
    public List<String> breakSentences(String input) {
        // todo: investigate better sentence breaking
        List<String> sentences = new ArrayList<>();
        synchronized (sentenceBreakIterator) {
            sentenceBreakIterator.setText(input);
            int start = sentenceBreakIterator.first();
            for (int end = sentenceBreakIterator.next(); end != BreakIterator.DONE; start = end, end = sentenceBreakIterator.next()) {
                String sentence = input.substring(start, end).trim();
                if (!sentence.isBlank()) {
                    sentences.add(sentence);
                }
            }
        }
        return sentences;
    }

    public static class Builder {
        /** minimum length for extracted keywords which are typically composed of multiple words/tokens */
        private int minNGrams = DEFAULT_MIN_NGRAMS;

        /** maximum length for extracted keywords */
        private int maxNGrams = DEFAULT_MAX_NGRAMS;

        /** maximum number of keywords to extract */
        private int keywordCount = DEFAULT_KEYWORD_COUNT;

        /** the maximum score threshold for keywords (lower score is better) */
        private float maxScoreThreshold = -1;

        /** the maximum number of characters to process as input for keyword extraction */
        private int maxContentLength = DEFAULT_MAX_CONTENT_LENGTH;

        private YakeLanguage language = BaseYakeLanguage.ENGLISH;

        public Builder() {}

        public Builder withMinNGrams(int minNGrams) {
            this.minNGrams = minNGrams;
            return this;
        }

        public Builder withMaxNGrams(int maxNGrams) {
            this.maxNGrams = maxNGrams;
            return this;
        }

        public Builder withKeywordCount(int keywordCount) {
            this.keywordCount = keywordCount;
            return this;
        }

        public Builder withMaxScoreThreshold(float maxScoreThreshold) {
            this.maxScoreThreshold = maxScoreThreshold;
            return this;
        }

        public Builder withMaxContentLength(int maxContentLength) {
            this.maxContentLength = maxContentLength;
            return this;
        }

        public Builder withLanguage(YakeLanguage language) {
            this.language = language;
            return this;
        }

        public YakeKeywordExtractor build() {
            return new YakeKeywordExtractor(minNGrams, maxNGrams, keywordCount, maxScoreThreshold, maxContentLength, language);
        }
    }
}
