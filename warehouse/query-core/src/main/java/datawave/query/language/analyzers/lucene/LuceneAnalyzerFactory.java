package datawave.query.language.analyzers.lucene;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory that creates one or more of {@link LanguageAwareAnalyzer}s
 */
public class LuceneAnalyzerFactory {

    private static final Logger log = LoggerFactory.getLogger(LuceneAnalyzerFactory.class);

    private static final String ALL_ANALYZERS = "ar,bg,bn,br,ca,cjk,cz,da,de,el,en,es,eu,fa,fi,fr,ga,gl,hu,hy,id,it,lt,lv,nl,no,pt,ro,ru,sv,th,tr";

    private static final Set<String> ALL_ANALYZERS_SET = Set.of(ALL_ANALYZERS.split(","));

    private Map<String,Class<?>> analyzerMap = new HashMap<>();

    public LuceneAnalyzerFactory() {
        // empty constructor
    }

    public List<LanguageAwareAnalyzer> createAnalyzers(Set<String> codes) {
        return createAnalyzers(codes, Collections.emptySet(), Collections.emptySet(), Collections.emptySet(), Collections.emptySet());
    }

    public List<LanguageAwareAnalyzer> createAnalyzers(Set<String> codes, Set<String> stems, Set<String> lemmas, Set<String> unigrams, Set<String> bigrams) {
        Set<String> shortCodes = new HashSet<>();
        if (codes.contains("all")) {
            shortCodes.addAll(ALL_ANALYZERS_SET);
        } else {
            shortCodes.addAll(codes);
        }
        shortCodes.addAll(codes);
        shortCodes.addAll(stems);
        shortCodes.addAll(lemmas);
        shortCodes.addAll(unigrams);
        shortCodes.addAll(bigrams);

        List<LanguageAwareAnalyzer> analyzers = new ArrayList<>();
        for (String code : shortCodes) {
            LanguageAwareAnalyzer analyzer = createAnalyzer(code);
            if (analyzer != null) {
                analyzer.setStopWordsEnabled(true); // stop words always enabled by default
                analyzer.setStemmingEnabled(stems.isEmpty() || !stems.contains(code));
                analyzer.setLemmasEnabled(lemmas.isEmpty() || !lemmas.contains(code));
                analyzer.setUnigrammingEnabled(unigrams.isEmpty() || !unigrams.contains(code));
                analyzer.setBigrammingEnabled(bigrams.isEmpty() || !bigrams.contains(code));
                analyzers.add(analyzer);
            }
        }
        return analyzers;
    }

    /**
     * Create an appropriate {@link LuceneAnalyzer} from the provided short code.
     *
     * @param code
     *            a short code
     * @return a lucene language analyzer
     */
    protected LanguageAwareAnalyzer createAnalyzer(String code) {
        code = code.toLowerCase();

        if (analyzerMap.containsKey(code)) {
            Class<?> clazz = analyzerMap.get(code);
            if (clazz != null) {
                try {
                    LanguageAwareAnalyzer analyzer = (LanguageAwareAnalyzer) clazz.newInstance();
                    log.debug("created analyzer for class {}", clazz.getName());
                    return analyzer;
                } catch (InstantiationException | IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        switch (code) {
            case "ar":
                return new ArabicLuceneAnalyzer();
            case "bg":
                return new BulgarianLuceneAnalyzer();
            case "bn":
                return new BengaliLuceneAnalyzer();
            case "br":
                return new BrazilianLuceneAnalyzer();
            case "ca":
                return new CatalanLuceneAnalyzer();
            case "cjk":
                return new CJKLuceneAnalyzer();
            case "cz":
                return new CzechLuceneAnalyzer();
            case "da":
                return new DanishLuceneAnalyzer();
            case "de":
                return new GermanLuceneAnalyzer();
            case "el":
                return new GreekLuceneAnalyzer();
            case "en":
                return new EnglishLuceneAnalyzer();
            case "es":
                return new SpanishLuceneAnalyzer();
            case "eu":
                return new BasqueLuceneAnalyzer();
            case "fa":
                return new PersianLuceneAnalyzer();
            case "fi":
                return new FinnishLuceneAnalyzer();
            case "fr":
                return new FrenchLuceneAnalyzer();
            case "ga":
                return new IrishLuceneAnalyzer();
            case "gl":
                return new GalicianLuceneAnalyzer();
            case "hi":
                return new HindiLuceneAnalyzer();
            case "hu":
                return new HungarianLuceneAnalyzer();
            case "hy":
                return new ArmenianLuceneAnalyzer();
            case "id":
                return new IndonesianLuceneAnalyzer();
            case "it":
                return new ItalianLuceneAnalyzer();
            case "lt":
                return new LithuanianLuceneAnalyzer();
            case "lv":
                return new LatvianLuceneAnalyzer();
            case "nl":
                return new DutchLuceneAnalyzer();
            case "no":
                return new NorwegianLuceneAnalysis();
            case "pt":
                return new PortugueseLuceneAnalyzer();
            case "ro":
                return new RomanianLuceneAnalyzer();
            case "ru":
                return new RussianLuceneAnalyzer();
            case "sv":
                return new SwedishLuceneAnalyzer();
            case "th":
                return new ThaiLuceneAnalyzer();
            case "tr":
                return new TurkishLuceneAnalyzer();
            case "default":
                return new DefaultLuceneAnalyzer();
        }
        return null;
    }

    public Map<String,Class<?>> getAnalyzerMap() {
        return analyzerMap;
    }

    public void setAnalyzerMap(Map<String,Class<?>> analyzerMap) {
        this.analyzerMap = analyzerMap;
    }
}
