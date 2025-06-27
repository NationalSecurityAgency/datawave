package datawave.util.keyword.language;

import java.util.Collections;
import java.util.Locale;
import java.util.Set;

import com.ibm.icu.text.BreakIterator;

/**
 * Implements a base set of languages for the YakeKeywordExtractor, defines mechanisms for stopwords and sentence breaking. The languages here are based on
 * those available in Spark MLlib.
 */
public enum BaseYakeLanguage implements YakeLanguage {
    DANISH("danish", "da"),
    DUTCH("dutch", "nl"),
    ENGLISH("english", "en"),
    FINNISH("finnish", "fi"),
    FRENCH("french", "fr"),
    GERMAN("german", "de"),
    HUNGARIAN("hungarian", "hu"),
    ITALIAN("italian", "it"),
    NORWEGIAN("norwegian", "no"),
    PORTUGUESE("portuguese", "pt"),
    RUSSIAN("russian", "ru"),
    SPANISH("spanish", "es"),
    SWEDISH("swedish", "sv"),
    TURKISH("turkish", "tu"),
    UNKNOWN("unknown", "zz");

    final String stopwordsLanguage;
    final String sentenceLocale;

    BaseYakeLanguage(String stopwordsLanguage, String sentenceLocale) {
        this.stopwordsLanguage = stopwordsLanguage;
        this.sentenceLocale = sentenceLocale;
        YakeLanguage.Registry.add(this);
    }

    public String getLanguageName() {
        return stopwordsLanguage;
    }

    @Override
    public String getLanguageCode() {
        return sentenceLocale;
    }

    @Override
    public Set<String> getStopwords() {
        // todo: cache immutable instances?
        return stopwordsLanguage.equals("unknown") ? Collections.emptySet() : Stopwords.loadDefaultStopWords(stopwordsLanguage);
    }

    @Override
    public BreakIterator getSentenceBreakIterator() {
        final Locale locale = (sentenceLocale.equals("zz")) ? Locale.US : Locale.forLanguageTag(sentenceLocale);
        return BreakIterator.getSentenceInstance(locale);
    }

    public static YakeLanguage forLanguageCode(String languageCode) {
        for (YakeLanguage lang : BaseYakeLanguage.values()) {
            if (lang.getLanguageCode().equals(languageCode)) {
                return lang;
            }
        }
        return null;
    }

    public static YakeLanguage forLanguageName(String languageName) {
        for (YakeLanguage lang : BaseYakeLanguage.values()) {
            if (lang.getLanguageName().equals(languageName)) {
                return lang;
            }
        }
        return null;
    }
}
