package datawave.query.language.analyzers.lucene;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.da.DanishAnalyzer;
import org.tartarus.snowball.SnowballProgram;
import org.tartarus.snowball.ext.DanishStemmer;

/**
 * Example {@link LanguageAwareAnalyzer} for the Danish language
 */
public class DanishLuceneAnalyzer extends LanguageAwareAnalyzer {

    protected DanishLuceneAnalyzer() {
        super(DanishAnalyzer.getDefaultStopSet());
    }

    @Override
    public boolean matches(String text) {
        return true;
    }

    protected SnowballProgram getStemmer() {
        return new DanishStemmer();
    }

    @Override
    public CharArraySet getDefaultStopWords() {
        return DanishAnalyzer.getDefaultStopSet();
    }
}
