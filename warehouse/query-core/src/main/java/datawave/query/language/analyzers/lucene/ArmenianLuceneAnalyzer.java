package datawave.query.language.analyzers.lucene;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.hy.ArmenianAnalyzer;
import org.tartarus.snowball.SnowballProgram;
import org.tartarus.snowball.ext.ArmenianStemmer;

/**
 * Example {@link LanguageAwareAnalyzer} for the Armenian language
 */
public class ArmenianLuceneAnalyzer extends LanguageAwareAnalyzer {

    protected ArmenianLuceneAnalyzer() {
        super(ArmenianAnalyzer.getDefaultStopSet());
    }

    @Override
    public boolean matches(String text) {
        return true;
    }

    @Override
    protected SnowballProgram getStemmer() {
        return new ArmenianStemmer();
    }

    @Override
    public CharArraySet getDefaultStopWords() {
        return ArmenianAnalyzer.getDefaultStopSet();
    }
}
