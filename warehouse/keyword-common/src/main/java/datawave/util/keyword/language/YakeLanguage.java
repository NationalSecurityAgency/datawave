package datawave.util.keyword.language;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.icu.text.BreakIterator;

/**
 * Defines an extensible interface for language management, providing methods to get language specific stopwords, sentence break iterators, register and look up
 * languages.
 */
public interface YakeLanguage {

    Logger logger = LoggerFactory.getLogger(YakeLanguage.class);

    /**
     * Get the name of this language instance.
     *
     * @return a language name string.
     */
    String getLanguageName();

    /**
     * Get the language code for this language instance - should correspond to the ISO-630-1 two-letter code.
     *
     * @return the code for this language.
     */
    String getLanguageCode();

    /**
     * Get the stopwords for this language instances.
     *
     * @return a Set of stopwords for this language.
     */
    Set<String> getStopwords();

    /**
     * Get a break iterator that will break text into sentences for this language's default locale.
     *
     * @return a BreakIterator for this language.
     */
    BreakIterator getSentenceBreakIterator();

    /** Manages an extendable collection of languages. */
    class Registry {
        /** maps a language name and code to a yake language implementation */
        static final Map<String,YakeLanguage> languageRegistry = new HashMap<>();

        /**
         * add the language to the language registry so it can be retrieved by name or code.
         *
         * @param language
         *            the language to add to the registry
         */
        public static void add(YakeLanguage language) {
            languageRegistry.put(language.getLanguageName(), language);
            languageRegistry.put(language.getLanguageCode(), language);
        }

        /**
         * find a language in the language registry based on its name or code.
         *
         * @param rawLanguage
         *            the language to find, this is down-cased before lookup.
         * @return the corresponding YakeLanguage for the specified language, or the ENGLISH language implementation if one can not be found.
         */
        public static YakeLanguage find(String rawLanguage) {
            if (rawLanguage == null || rawLanguage.isEmpty()) {
                logger.debug("No language name or code provided, returning default language, English");
                return BaseYakeLanguage.ENGLISH;
            }
            String language = rawLanguage.toLowerCase(Locale.US);
            final YakeLanguage yakeLanguage = languageRegistry.get(language);
            if (yakeLanguage == null) {
                logger.warn("Unable for find language for language name or code '{}', defaulting to English", language);
                return BaseYakeLanguage.ENGLISH;
            }
            return yakeLanguage;
        }
    }

    /**
     * Manages loading a series of files containing stopword lists for various languages. The files are stored as resources in the
     * datawave.util.keyword.stopwords package.
     */
    class Stopwords {

        /**
         * Load stopwords for the specified language.
         *
         * @param language
         *            the language for the desired stopword list.
         * @return a set of stopwords.
         * @throws IllegalStateException
         *             wraps an io exception encountered when loading the stop list as a resource from the classpath.
         */
        public static Set<String> loadDefaultStopWords(String language) {
            try (BufferedReader r = getReaderForStopwordResource(language)) {
                Set<String> stopwords = new HashSet<>();
                String line;
                while ((line = r.readLine()) != null) {
                    stopwords.add(line.trim());
                }
                return stopwords;
            } catch (IOException e) {
                throw new IllegalStateException("Error loading stopwords for '" + language + "'", e);
            }
        }

        /**
         * Obtain a reader for the specified stopword resource.
         *
         * @param language
         *            the language for the desired stopword list.
         * @return a Reader for a stopword file.
         * @throws IOException
         *             if there is a problem opening the specified stopword resource.
         */
        private static BufferedReader getReaderForStopwordResource(String language) throws IOException {
            String resource = "/datawave/util/keyword/stopwords/" + language + ".txt";
            InputStream is = YakeLanguage.class.getResourceAsStream(resource);
            if (is == null) {
                throw new IOException("Unable to find resource " + resource + " for language " + language);
            }
            return new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
        }
    }
}
