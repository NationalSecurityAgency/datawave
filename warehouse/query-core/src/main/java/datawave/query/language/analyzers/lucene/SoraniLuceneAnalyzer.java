package datawave.query.language.analyzers.lucene;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.ckb.SoraniAnalyzer;
import org.apache.lucene.analysis.ckb.SoraniNormalizationFilter;
import org.apache.lucene.analysis.ckb.SoraniStemFilter;
import org.apache.lucene.analysis.core.DecimalDigitFilter;

/**
 * Example {@link LanguageAwareAnalyzer} for the Sorani language
 */
public class SoraniLuceneAnalyzer extends LanguageAwareAnalyzer {

    protected SoraniLuceneAnalyzer() {
        super(SoraniAnalyzer.getDefaultStopSet());
    }

    @Override
    public boolean matches(String text) {
        return true;
    }

    @Override
    protected TokenStream getBaseFilter(TokenStream stream) {
        stream = new SoraniNormalizationFilter(stream);
        stream = new LowerCaseFilter(stream);
        return new DecimalDigitFilter(stream);
    }

    @Override
    protected TokenStream getStemFilter(TokenStream stream) {
        return new SoraniStemFilter(stream);
    }

    @Override
    public CharArraySet getDefaultStopWords() {
        return SoraniAnalyzer.getDefaultStopSet();
    }
}
