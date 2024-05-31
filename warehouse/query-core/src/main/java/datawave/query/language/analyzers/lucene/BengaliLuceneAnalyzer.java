package datawave.query.language.analyzers.lucene;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.bn.BengaliAnalyzer;
import org.apache.lucene.analysis.bn.BengaliNormalizationFilter;
import org.apache.lucene.analysis.bn.BengaliStemFilter;
import org.apache.lucene.analysis.core.DecimalDigitFilter;
import org.apache.lucene.analysis.in.IndicNormalizationFilter;
import org.tartarus.snowball.SnowballProgram;

/**
 * Example {@link LanguageAwareAnalyzer} for the Basque language
 */
public class BengaliLuceneAnalyzer extends LanguageAwareAnalyzer {

    protected BengaliLuceneAnalyzer() {
        super(BengaliAnalyzer.getDefaultStopSet());
    }

    @Override
    public boolean matches(String text) {
        return true;
    }

    @Override
    public TokenStream getBaseFilter(TokenStream stream) {
        stream = new LowerCaseFilter(stream);
        return new DecimalDigitFilter(stream);
    }

    @Override
    public TokenStream getStemFilter(TokenStream stream) {
        stream = new IndicNormalizationFilter(stream);
        stream = new BengaliNormalizationFilter(stream);
        stream = getStopWordFilter(stream);
        return new BengaliStemFilter(stream);
    }

    @Override
    protected SnowballProgram getStemmer() {
        throw new IllegalStateException("BengaliLuceneAnalyzer does not use getStemmer()");
    }

    @Override
    public CharArraySet getDefaultStopWords() {
        return BengaliAnalyzer.getDefaultStopSet();
    }
}
