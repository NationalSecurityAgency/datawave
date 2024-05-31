package datawave.query.language.analyzers.lucene;

import java.util.List;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.it.ItalianAnalyzer;
import org.apache.lucene.analysis.it.ItalianLightStemFilter;
import org.apache.lucene.analysis.util.ElisionFilter;

/**
 * Example {@link LanguageAwareAnalyzer} for the Italian language
 */
public class ItalianLuceneAnalyzer extends LanguageAwareAnalyzer {

    protected ItalianLuceneAnalyzer() {
        super(ItalianAnalyzer.getDefaultStopSet());
    }

    @Override
    public boolean matches(String text) {
        return true;
    }

    @Override
    protected TokenStream getBaseFilter(TokenStream stream) {
        CharArraySet articles = new CharArraySet(List.of("c", "l", "all", "dall", "dell", "nell", "sull", "coll", "pell", "gl", "agl", "dagl", "degl", "negl",
                        "sugl", "un", "m", "t", "s", "v", "d"), true);
        stream = new ElisionFilter(stream, articles);
        return new LowerCaseFilter(stream);
    }

    @Override
    protected TokenStream getStemFilter(TokenStream stream) {
        return new ItalianLightStemFilter(stream);
    }

    @Override
    public CharArraySet getDefaultStopWords() {
        return ItalianAnalyzer.getDefaultStopSet();
    }
}
