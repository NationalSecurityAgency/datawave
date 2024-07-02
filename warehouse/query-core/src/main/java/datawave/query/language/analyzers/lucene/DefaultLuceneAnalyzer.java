package datawave.query.language.analyzers.lucene;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.en.EnglishAnalyzer;

import datawave.ingest.data.tokenize.StandardAnalyzer;

/**
 * Default implementation of a {@link LanguageAwareAnalyzer} that uses the {@link StandardAnalyzer}
 */
public class DefaultLuceneAnalyzer extends LanguageAwareAnalyzer {

    public DefaultLuceneAnalyzer() {
        super(EnglishAnalyzer.getDefaultStopSet());
    }

    @Override
    public boolean matches(String text) {
        return true;
    }

    @Override
    public CharArraySet getDefaultStopWords() {
        return EnglishAnalyzer.getDefaultStopSet();
    }
}
