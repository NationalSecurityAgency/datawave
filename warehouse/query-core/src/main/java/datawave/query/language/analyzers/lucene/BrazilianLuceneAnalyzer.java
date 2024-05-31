package datawave.query.language.analyzers.lucene;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.br.BrazilianAnalyzer;
import org.apache.lucene.analysis.br.BrazilianStemFilter;
import org.tartarus.snowball.SnowballProgram;

/**
 * Example {@link LanguageAwareAnalyzer} for the Brazilian language
 */
public class BrazilianLuceneAnalyzer extends LanguageAwareAnalyzer {

    protected BrazilianLuceneAnalyzer() {
        super(BrazilianAnalyzer.getDefaultStopSet());
    }

    @Override
    public boolean matches(String text) {
        return true;
    }

    protected TokenStream getStemFilter(TokenStream stream) {
        return new BrazilianStemFilter(stream);
    }

    @Override
    protected SnowballProgram getStemmer() {
        throw new IllegalArgumentException("BrazilianLuceneAnalyzer does not use getStemmer()");
    }

    @Override
    public CharArraySet getDefaultStopWords() {
        return BrazilianAnalyzer.getDefaultStopSet();
    }
}
