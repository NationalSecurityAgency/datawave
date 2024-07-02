package datawave.query.language.analyzers.lucene;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tr.ApostropheFilter;
import org.apache.lucene.analysis.tr.TurkishAnalyzer;
import org.apache.lucene.analysis.tr.TurkishLowerCaseFilter;
import org.tartarus.snowball.SnowballProgram;
import org.tartarus.snowball.ext.TurkishStemmer;

/**
 * Example {@link LanguageAwareAnalyzer} for the Turkish language
 */
public class TurkishLuceneAnalyzer extends LanguageAwareAnalyzer {

    protected TurkishLuceneAnalyzer() {
        super(TurkishAnalyzer.getDefaultStopSet());
    }

    @Override
    public boolean matches(String text) {
        return true;
    }

    @Override
    protected TokenStream getBaseFilter(TokenStream stream) {
        stream = new ApostropheFilter(stream);
        return new TurkishLowerCaseFilter(stream);
    }

    @Override
    protected SnowballProgram getStemmer() {
        return new TurkishStemmer();
    }

    @Override
    public CharArraySet getDefaultStopWords() {
        return TurkishAnalyzer.getDefaultStopSet();
    }
}
