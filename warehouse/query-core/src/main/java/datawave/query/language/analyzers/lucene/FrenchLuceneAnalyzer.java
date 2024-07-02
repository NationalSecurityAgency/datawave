package datawave.query.language.analyzers.lucene;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.fr.FrenchAnalyzer;
import org.apache.lucene.analysis.fr.FrenchLightStemFilter;
import org.apache.lucene.analysis.util.ElisionFilter;

/**
 * Example {@link LanguageAwareAnalyzer} for the French language
 */
public class FrenchLuceneAnalyzer extends LanguageAwareAnalyzer {

    public FrenchLuceneAnalyzer() {
        super(FrenchAnalyzer.getDefaultStopSet());
    }

    @Override
    public boolean matches(String text) {
        return true;
    }

    @Override
    protected TokenStream getBaseFilter(TokenStream stream) {
        stream = new ElisionFilter(stream, FrenchAnalyzer.DEFAULT_ARTICLES);
        return new LowerCaseFilter(stream);
    }

    @Override
    protected TokenStream getStemFilter(TokenStream stream) {
        return new FrenchLightStemFilter(stream);
    }

    @Override
    public CharArraySet getDefaultStopWords() {
        return FrenchAnalyzer.getDefaultStopSet();
    }
}
