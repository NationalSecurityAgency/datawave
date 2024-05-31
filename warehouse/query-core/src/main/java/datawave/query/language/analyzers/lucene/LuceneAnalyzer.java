package datawave.query.language.analyzers.lucene;

import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.language.analyzers.LanguageAnalyzer;

/**
 * Base class for lucene-based language analyzers
 */
public abstract class LuceneAnalyzer extends LanguageAnalyzer {

    private static final Logger log = LoggerFactory.getLogger(LuceneAnalyzer.class);

    // lucene-specific implementations of stopwords and stem exclusions
    protected CharArraySet stopwords;
    protected CharArraySet stemExclusions;

    protected Analyzer analyzer;

    protected LuceneAnalyzer() {

    }

    protected LuceneAnalyzer(Analyzer analyzer) {
        this.analyzer = analyzer;
    }

    @Override
    public Set<String> findAlternates(String field, String text) {
        Set<String> tokens = new HashSet<>();

        try (TokenStream tokenStream = getTokenStream(field, text)) {
            tokenStream.reset();

            while (tokenStream.incrementToken()) {
                String token = tokenStream.getAttribute(CharTermAttribute.class).toString();
                log.debug("tok: {}", token);
                if (token != null && !text.equals(token)) {
                    tokens.add(token);
                }
            }

        } catch (Exception e) {
            log.error(e.getMessage());
            throw new DatawaveFatalQueryException(e);
        }
        return tokens;
    }

    public TokenStream getTokenStream(String field, String text) {
        return analyzer.tokenStream(field, text);
    }

    @Override
    public String findBestAlternate(String field, String text) {
        Set<String> tokens = findAlternates(field, text);
        if (!tokens.isEmpty()) {
            return tokens.iterator().next();
        }
        return null;
    }

    public CharArraySet getStopwords() {
        return stopwords;
    }

    public void setStopwords(CharArraySet stopwords) {
        this.stopwords = stopwords;
    }

    public CharArraySet getStemExclusions() {
        return stemExclusions;
    }

    public void setStemExclusions(CharArraySet stemExclusions) {
        this.stemExclusions = stemExclusions;
    }
}
