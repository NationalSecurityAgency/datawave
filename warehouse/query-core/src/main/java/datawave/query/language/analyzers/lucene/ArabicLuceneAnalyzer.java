package datawave.query.language.analyzers.lucene;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.ar.ArabicAnalyzer;
import org.apache.lucene.analysis.ar.ArabicStemFilter;
import org.apache.lucene.analysis.core.DecimalDigitFilter;
import org.tartarus.snowball.SnowballProgram;

import datawave.query.language.analyzers.LanguageAnalyzer;

/**
 * Example {@link LanguageAnalyzer} for the Arabic language
 */
public class ArabicLuceneAnalyzer extends LanguageAwareAnalyzer {

    public ArabicLuceneAnalyzer() {
        super(ArabicAnalyzer.getDefaultStopSet());
    }

    @Override
    public boolean matches(String text) {
        return true;
    }

    @Override
    protected TokenStream getBaseFilter(TokenStream stream) {
        stream = new LowerCaseFilter(stream);
        return new DecimalDigitFilter(stream);
    }

    @Override
    protected TokenStream getStemFilter(TokenStream stream) {
        return new ArabicStemFilter(stream);
    }

    @Override
    protected SnowballProgram getStemmer() {
        throw new IllegalStateException("ArabicLuceneAnalyzer does not use getStemmer()");
    }

    @Override
    public CharArraySet getDefaultStopWords() {
        return ArabicAnalyzer.getDefaultStopSet();
    }
}
