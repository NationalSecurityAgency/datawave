package datawave.ingest.util;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;

import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.mapreduce.MemberShipTest;

import datawave.util.StringUtils;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.ngram.NGramTokenizer;
import org.apache.lucene.analysis.standard.ClassicFilter;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import com.google.common.hash.BloomFilter;

/**
 * Base implementation for tokenizing normalized content into n-grams, which are applied as an update to a BloomFilter
 * 
 * @see com.google.common.hash.BloomFilter
 * @see datawave.ingest.util.BloomFilterUtil
 */
public class NGramTokenizationStrategy extends AbstractNGramTokenizationStrategy {
    private final Logger log = Logger.getLogger(NGramTokenizationStrategy.class);
    private boolean loggedInvalidMaxNGramLength;
    
    /**
     * Constructor
     */
    public NGramTokenizationStrategy() {
        super();
    }
    
    /**
     * Constructor
     * 
     * @param filter
     *            Updated with n-grams tokenized from normalized content
     */
    public NGramTokenizationStrategy(final BloomFilter<String> filter) {
        super(filter);
    }
    
    /**
     * Constructor
     * 
     * @param source
     *            strategy with which to delegate tokenization operations
     */
    public NGramTokenizationStrategy(final AbstractNGramTokenizationStrategy source) {
        super(source);
    }
    
    /**
     * Increments the tokenizer and returns the next n-gram in the stream, or null at some termination state, such as EOS.
     * 
     * 
     * @param tokenizer
     *            The tokenizer responsible for generating the next available n-gram
     * @return the next n-gram in the stream, or null at some termination state, such as EOS
     */
    protected String increment(final NGramTokenizer tokenizer) throws TokenizationException {
        String ngram = super.increment(tokenizer);
        if (null == ngram) {
            try {
                if ((null != tokenizer) && tokenizer.incrementToken()) {
                    final CharTermAttribute charTermAttribute = tokenizer.getAttribute(CharTermAttribute.class);
                    if (null != charTermAttribute) {
                        ngram = charTermAttribute.toString();
                        charTermAttribute.resizeBuffer(0);
                    } else {
                        ngram = null;
                    }
                } else {
                    ngram = null;
                }
            } catch (final IOException e) {
                throw new TokenizationException("Could not get next n-gram from NGramTokenizer", e);
            }
        }
        
        return ngram;
    }
    
    /**
     * Creates n-grams based on normalized content. N-gram strings will be no longer than the specified length.
     * 
     * @param content
     *            Normalized field name and value
     * @param maxNGramLength
     *            Maximum length of tokenized n-grams
     * @return The number of tokenized n-grams
     * @throws TokenizationException
     *             for issues with tokenization
     */
    public int tokenize(final NormalizedContentInterface content, int maxNGramLength) throws TokenizationException {
        // Initialize return value and try tokenization using the parent
        int ngramCount = super.tokenize(content, maxNGramLength);
        
        // If the parent did not tokenize, try it below
        if (ngramCount < 0) {
            // Validate max n-gram length
            if ((maxNGramLength <= 0) && !this.loggedInvalidMaxNGramLength) {
                this.log.error("Cannot tokenize n-grams with a maximum length of " + maxNGramLength, new IllegalArgumentException());
                this.loggedInvalidMaxNGramLength = true;
            }
            // Otherwise, try to tokenize
            else {
                // Validate content and extract info
                final String fieldValue;
                if (null != content) {
                    fieldValue = content.getIndexedFieldValue();
                } else {
                    fieldValue = null;
                }
                
                // Tokenize
                NGramTokenizer tokenizer = null;
                TokenStream tokenStream = null;
                TokenizationException exception = null;
                try {
                    // Create a reader and determine, if applicable, the max number of allowed n-grams
                    final Reader reader;
                    if (null != fieldValue) {
                        reader = new StringReader(fieldValue);
                    } else {
                        reader = new StringReader(StringUtils.EMPTY_STRING);
                    }
                    
                    // Create the tokenizer and tokenizing stream
                    tokenizer = new NGramTokenizer(2, maxNGramLength);
                    tokenizer.setReader(reader);
                    tokenizer.reset();
                    tokenStream = new ClassicFilter(tokenizer);
                    tokenStream = new LowerCaseFilter(tokenStream);
                    tokenStream.addAttribute(CharTermAttribute.class);
                    
                    // Reset the n-gram count
                    ngramCount = 0;
                    
                    // Increment the tokenizer and applied any generated n-grams
                    String ngram = null;
                    while (null != (ngram = this.increment(tokenizer))) {
                        if (this.updateFilter(ngram, content)) {
                            ngramCount++;
                        }
                    }
                } catch (final TokenizationException e) {
                    exception = e;
                    throw e;
                } catch (final IOException e) {
                    exception = new TokenizationException(e);
                    throw exception;
                } finally {
                    // Close the tokenizer
                    if (null != tokenizer) {
                        try {
                            tokenizer.end();
                            tokenizer.close();
                        } catch (final IOException ignore) {}
                    }
                    // Close the stream
                    if (null != tokenStream) {
                        try {
                            tokenStream.close();
                        } catch (final IOException e) {
                            // Prevent this secondary problem from masking a more serious one
                            if (null == exception) {
                                throw new TokenizationException(e);
                            }
                        }
                    }
                }
            }
        }
        
        return ngramCount;
    }
    
    /**
     * Applies a tokenized n-gram to the BloomFilter based on the specified normalized content
     * 
     * @param ngram
     *            An n-gram generated from the specified normalized content
     * @param content
     *            A normalized field name and value
     * @return true, if the n-gram was applied to the strategy's BloomFilter
     */
    protected boolean updateFilter(final String ngram, final NormalizedContentInterface content) throws TokenizationException {
        boolean updated = super.updateFilter(ngram, content);
        if (!updated) {
            final BloomFilter<String> filter = this.getFilter();
            if ((null != ngram) && (null != filter)) {
                MemberShipTest.update(filter, ngram);
                updated = true;
            } else {
                updated = false;
            }
        }
        
        return updated;
    }
}
