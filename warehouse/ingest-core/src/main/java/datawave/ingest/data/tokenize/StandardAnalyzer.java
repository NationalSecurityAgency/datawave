package datawave.ingest.data.tokenize;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.StopwordAnalyzerBase;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.standard.ClassicFilter;

/**
 * Filters {@link StandardTokenizer} with {@link LowerCaseFilter} {@link ClassicFilter} and {@link StopFilter}, using a list of English stop words (unless
 * otherwise specified).
 * <p>
 * This analyzer does NOT provide the ability to apply BASIS RLP processing
 */
public class StandardAnalyzer extends StopwordAnalyzerBase {

    /** Default maximum allowed token length */
    public static final int DEFAULT_MAX_TOKEN_LENGTH = 255;

    /** Default token truncation length */
    public static final int DEFAULT_TRUNCATE_TOKEN_LENGTH = 64;

    private int maxTokenLength = DEFAULT_MAX_TOKEN_LENGTH;
    private int tokenTruncateLength = DEFAULT_MAX_TOKEN_LENGTH;

    protected boolean applyAccentFilter = false;
    protected TokenSearch searchUtil;

    /**
     * Build an analyzer with the default stop words: ({@link EnglishAnalyzer#ENGLISH_STOP_WORDS_SET}).
     * <p>
     * This hides the matchVersion parameter we don't always want consumers to have to be concerned with it. Generally matchVersion will be set to the current
     * Lucene version.
     */
    public StandardAnalyzer() {
        this(EnglishAnalyzer.ENGLISH_STOP_WORDS_SET);
    }

    /**
     * Build an analyzer with the given specified words.
     * <p>
     * This hides the matchVersion parameter we don't always want consumers to have to be concerned with it. Generally matchVersion will be set to the current
     * Lucene version.
     *
     * @param stopWords
     *            words to create analyzer with
     */
    public StandardAnalyzer(CharArraySet stopWords) {
        super(stopWords);
    }

    public StandardAnalyzer(TokenSearch searchUtil) {
        super(searchUtil.getInstanceStopwords());
        this.searchUtil = searchUtil;
    }

    /**
     * @see #setMaxTokenLength
     * @return max token length
     */
    public int getMaxTokenLength() {
        return maxTokenLength;
    }

    /**
     * Set maximum allowed token length. If a token is seen that exceeds this length then it is discarded. This setting only takes effect the next time
     * tokenStream or tokenStream is called.
     *
     * @param length
     *            length of the token
     */
    public void setMaxTokenLength(int length) {
        maxTokenLength = length;
    }

    /**
     * Set the length beyond which tokens will be truncated. Tokens longer than this length will be truncated to this length.
     *
     * @param length
     *            the default token truncate length
     */
    public void setTokenTruncateLength(int length) {
        tokenTruncateLength = length;
    }

    @Override
    protected TokenStreamComponents createComponents(final String fieldName) {
        final StandardTokenizer src = new StandardTokenizer();
        if (maxTokenLength > 0) {
            src.setMaxTokenLength(maxTokenLength);
        }
        if (tokenTruncateLength > 0) {
            src.setTokenTruncateLength(tokenTruncateLength);
        }

        TokenStream tok = new ClassicFilter(src);
        if (stopwords != null) {
            // stopwords are not case-sensitive, so we need to lower case tokens
            // before checking them in the stop filter.
            tok = new LowerCaseFilter(tok);
            tok = new StopFilter(tok, stopwords);
        }
        if (searchUtil != null) {
            // generate synonyms if a search filter is provided.
            tok = new TokenSearchSynonymFilter(tok, searchUtil);
        }
        if (applyAccentFilter) {
            // NOTE: datawave normalization should take care of removing accents
            // from words, so this can likely be removed but we leave it for compatibility
            tok = new AccentFilter(tok);
        }
        return new TokenStreamComponents(src, tok);
    }

    @Override
    protected TokenStream normalize(String fieldName, TokenStream in) {
        /*
         * NOTE: the normalize() method is used for query processing and there are things that we do not rely on this Analyzer to do in that case, including:
         * down-casing query terms, removing stopwords, generating synonyms, removing accents. In some cases the Analyzer may do these things at indexing time
         * as a part of createComponents
         */
        return new ClassicFilter(in);
    }
}
