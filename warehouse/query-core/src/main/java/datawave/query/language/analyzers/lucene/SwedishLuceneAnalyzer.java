package datawave.query.language.analyzers.lucene;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.sv.SwedishAnalyzer;
import org.tartarus.snowball.SnowballProgram;
import org.tartarus.snowball.ext.SwedishStemmer;

/**
 * Example {@link LanguageAwareAnalyzer} for the Swedish language
 */
public class SwedishLuceneAnalyzer extends LanguageAwareAnalyzer {

    protected SwedishLuceneAnalyzer() {
        super(SwedishAnalyzer.getDefaultStopSet());
    }

    protected SnowballProgram getStemmer() {
        return new SwedishStemmer();
    }

    @Override
    public boolean matches(String text) {
        return true;
    }

    @Override
    public CharArraySet getDefaultStopWords() {
        return SwedishAnalyzer.getDefaultStopSet();
    }
}
