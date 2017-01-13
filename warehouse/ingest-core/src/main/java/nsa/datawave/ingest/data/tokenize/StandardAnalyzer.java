package nsa.datawave.ingest.data.tokenize;

import java.io.IOException;
import java.io.Reader;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.analysis.util.StopwordAnalyzerBase;
import org.apache.lucene.util.Version;

/**
 * Filters {@link StandardTokenizer} with {@link StandardFilter}, {@link LowerCaseFilter} and {@link StopFilter}, using a list of English stop words (unless
 * otherwise specified).
 * <p/>
 * This analyzer does NOT provide the ability to apply BASIS RLP processing
 * 
 */
public class StandardAnalyzer extends StopwordAnalyzerBase {
    
    /** Default maximum allowed token length */
    public static final int DEFAULT_MAX_TOKEN_LENGTH = 255;
    public static final int DEFAULT_TRUNCATE_TOKEN_LENGTH = 1024;
    
    private int maxTokenLength = DEFAULT_MAX_TOKEN_LENGTH;
    private int tokenTruncateLength = DEFAULT_MAX_TOKEN_LENGTH;
    
    protected boolean applyAccentFilter = false;
    protected TokenSearch searchUtil;
    
    /**
     * An unmodifiable set containing some common English words that are usually not useful for searching.
     */
    public static final CharArraySet STOP_WORDS_SET = StopAnalyzer.ENGLISH_STOP_WORDS_SET;
    
    /**
     * Build an analyzer with the default stop words: ({@link #STOP_WORDS_SET}).
     * <p/>
     * This hides the matchVersion parameter we don't always want consumers to have to be concerned with it. Generally matchVersion will be set to the current
     * Lucene version.
     */
    public StandardAnalyzer() {
        this(STOP_WORDS_SET);
    }
    
    /**
     * Build an analyzer with the given specified words.
     * <p/>
     * This hides the matchVersion parameter we don't always want consumers to have to be concerned with it. Generally matchVersion will be set to the current
     * Lucene version.
     * 
     * @param stopWords
     */
    public StandardAnalyzer(CharArraySet stopWords) {
        super(Version.LUCENE_47, stopWords);
    }
    
    public StandardAnalyzer(TokenSearch searchUtil) {
        super(Version.LUCENE_47, searchUtil.getInstanceStopwords());
    }
    
    /**
     * Set maximum allowed token length. If a token is seen that exceeds this length then it is discarded. This setting only takes effect the next time
     * tokenStream or tokenStream is called.
     */
    public void setMaxTokenLength(int length) {
        maxTokenLength = length;
    }
    
    /**
     * Set the length beyond which tokens will be truncated. Tokens longer than this length will be truncated to this length.
     * 
     * @param length
     */
    public void setTokenTruncateLength(int length) {
        tokenTruncateLength = length;
    }
    
    /**
     * @see #setMaxTokenLength
     */
    public int getMaxTokenLength() {
        return maxTokenLength;
    }
    
    @Override
    protected TokenStreamComponents createComponents(final String fieldName, final Reader reader) {
        final StandardTokenizer src = new StandardTokenizer(reader);
        
        if (maxTokenLength > 0) {
            src.setMaxTokenLength(maxTokenLength);
        }
        
        if (tokenTruncateLength > 0) {
            src.setTokenTruncateLength(tokenTruncateLength);
        }
        
        // Retain Standard Filter's old APOSTROPHE and ACRONYM handling
        // from Lucene 3.0
        @SuppressWarnings("deprecation")
        TokenStream tok = new StandardFilter(Version.LUCENE_30, src);
        
        tok = new LowerCaseFilter(matchVersion, tok);
        
        if (stopwords != null) {
            tok = new StopFilter(matchVersion, tok, stopwords);
        }
        
        if (searchUtil != null) {
            tok = new TokenSearchSynonymFilter(tok, searchUtil);
        }
        
        if (applyAccentFilter) {
            tok = new AccentFilter(tok);
        }
        
        return new TokenStreamComponents(src, tok) {
            @Override
            protected void setReader(final Reader reader) throws IOException {
                src.setMaxTokenLength(StandardAnalyzer.this.maxTokenLength);
                super.setReader(reader);
            }
        };
    }
}
