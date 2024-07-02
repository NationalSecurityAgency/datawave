package datawave.query.language.analyzers.lucene;

import java.util.List;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.ca.CatalanAnalyzer;
import org.apache.lucene.analysis.util.ElisionFilter;
import org.tartarus.snowball.SnowballProgram;
import org.tartarus.snowball.ext.CatalanStemmer;

/**
 * Example {@link LanguageAwareAnalyzer} for the Catalan language
 */
public class CatalanLuceneAnalyzer extends LanguageAwareAnalyzer {

    protected CatalanLuceneAnalyzer() {
        super(CatalanAnalyzer.getDefaultStopSet());
    }

    @Override
    public boolean matches(String text) {
        return true;
    }

    @Override
    protected TokenStream getBaseFilter(TokenStream stream) {
        // CatalanAnalyzer has a private DEFAULT_ARTICLES variable but no way to access it
        CharArraySet articles = new CharArraySet(List.of("d", "l", "m", "n", "s", "t"), true);
        stream = new ElisionFilter(stream, articles);
        return new LowerCaseFilter(stream);
    }

    @Override
    protected SnowballProgram getStemmer() {
        return new CatalanStemmer();
    }

    @Override
    public CharArraySet getDefaultStopWords() {
        return CatalanAnalyzer.getDefaultStopSet();
    }
}
