package datawave.ingest.data.tokenize;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;

import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.WordlistLoader;
import org.apache.lucene.util.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Closer;

import datawave.util.ObjectFactory;

public interface TokenSearch {

    CharArraySet getInstanceStopwords();

    void setReverse(boolean reverse);

    boolean isReverse();

    boolean isEmailDomainTokensEnabled();

    void setEmailDomainTokensEnabled(boolean emailDomainTokensEnabled);

    boolean isDirtyWordTokensEnabled();

    void setDirtyWordTokensEnabled(boolean dirtyWordsTokensEnabled);

    boolean isFileWordTokensEnabled();

    void setFileWordTokensEnabled(boolean fileWordTokensEnabled);

    boolean isEmailWordTokensEnabled();

    void setEmailWordTokensEnabled(boolean emailWordTokensEnabled);

    boolean isUrlWordTokensEnabled();

    void setUrlWordTokensEnabled(boolean urlWordTokensEnabled);

    boolean isTermWordTokensEnabled();

    void setTermWordTokensEnabled(boolean termWordTokensEnabled);

    void setMaxURLDecodes(int maxUrlDecodes);

    int getMaxURLDecodes();

    /**
     * Get the synonyms for the given term type. If includeTerm is specified, the down-cased version of the original term is added to the synonyms list. If it
     * is not specified, the down-cased term will be added to the synonym list only if it is different from the original term.
     *
     * @param zw
     *            The term for which to obtain synonyms
     * @param termType
     *            The type of the term specified in the <code>term</code> parameter.
     * @param includeTerm
     *            A boolean flag to indicate whether the original tag should be included in the synonym list. If true, a down-cased copy of the original term is
     *            included in the synonym list, otherwise, we include downcased terms as a synonym only when they differ from the original term.
     * @return A list of strings that are synonyms for the term provided. Optionally contains a downcased version of the original term.
     */
    Collection<String> getSynonyms(String[] zw, String termType, boolean includeTerm);

    List<String> getSynonyms(String term, String termType, boolean includeTerm);

    void getTokenWords(String input, String zone, Collection<String> synonyms);

    String[] getTokenWords(String input);

    Collection<String> emailAddressTokens(String[] zw, boolean reverse_indexing, boolean includeTerm);

    List<String> emailAddressTokens(String term, boolean reverse_indexing, boolean includeTerm);

    Collection<String> ipAddressTokens(String[] zw, boolean reverse_indexing, boolean includeTerm);

    List<String> ipAddressTokens(String term, boolean reverse_indexing, boolean includeTerm);

    List<String> timestampTokens(String term, boolean reverse_indexing, boolean includeTerm);

    Collection<String> timestampTokens(String[] zw, boolean reverse_indexing, boolean includeTerm);

    Collection<String> filePathTokens(String[] zw, boolean reverse_indexing, boolean includeTerm);

    List<String> filePathTokens(String term, boolean reverse_indexing, boolean includeTerm);

    Collection<String> httpRequestTokens(String[] zw, boolean reverse_indexing, boolean includeTerm);

    List<String> httpRequestTokens(String term, boolean reverse_indexing, boolean includeTerm);

    Collection<String> dirtyTokens(String[] zw, boolean includeTerm);

    List<String> dirtyTokens(String term, boolean includeTerm);

    Collection<String> urlTokens(String[] zw, boolean reverse_indexing, boolean includeTerm);

    List<String> urlTokens(String term, boolean reverse_indexing, boolean includeTerm);

    List<String> getTermSynonyms(String term);

    boolean isStop(String term);

    Collection<String> getTermSynonyms(String[] term, boolean includeTerm);

    List<String> getTermSynonyms(String term, boolean includeTerm);

    /**
     * Factory for loading a concrete TokenSearch instance. Utility method for loading a stopwords resource file is provided for convenience.
     */
    class Factory {

        private static final Logger logger = LoggerFactory.getLogger(Factory.class);

        private Factory() {}

        /**
         * Returns the default TokenSearch implementation. <br>
         * <br>
         * Throws a RuntimeException if the default implementation cannot be instantiated for any reason
         *
         * @return tokensearch default instance
         */
        public static TokenSearch newInstance() {
            try {
                return new DefaultTokenSearch();
            } catch (Throwable t) {
                logger.error("Failed to instantiate default TokenSearch", t);
                throw new RuntimeException(t);
            }
        }

        /**
         * If tokenSearchClass is not null/empty, the specified class will be instantiated via no-arg constructor and returned. If the specified class can not
         * be instantiated for any reason, then an instance of Throwable will be thrown. <br>
         * <br>
         * If tokenSearchClass is null/empty, then a RuntimeException will be thrown.
         *
         * @param tokenSearchClass
         *            name of tokensearch class
         * @return a tokensearch instance
         */
        public static TokenSearch newInstance(String tokenSearchClass) {
            if (null == tokenSearchClass || tokenSearchClass.isEmpty()) {
                throw new IllegalArgumentException("tokenSearchClass argument cannot be null/empty");
            }
            return (TokenSearch) ObjectFactory.create(tokenSearchClass);
        }

        /**
         * If tokenSearchClass is not null/empty, the specified class will be instantiated via the specified constructor args (best-match). If the class can not
         * be instantiated for any reason, then an instance of Throwable will be thrown. <br>
         * <br>
         * If tokenSearchClass is null/empty, then a RuntimeException will be thrown.
         *
         * @param tokenSearchClass
         *            name of the tokensearch class
         * @param args
         *            arguments for creating the class
         * @return a tokensearch instance
         */
        public static TokenSearch newInstance(String tokenSearchClass, Object... args) {
            if (null == tokenSearchClass || tokenSearchClass.isEmpty()) {
                throw new IllegalArgumentException("tokenSearchClass argument cannot be null/empty");
            }
            return (TokenSearch) ObjectFactory.create(tokenSearchClass, args);
        }

        /**
         * Load stopwords from the specified file located in the classpath.
         * <p>
         * If a directory name is specified, e.g: <code>tmp/stopwords.txt</code> that path will be used when searching for the resource. Otherwise, the package
         * contianing the DefaultTokenSearch class may be used.
         * <p>
         * The current thread's context classloader will be used to load the specified filename as a resource.
         *
         * @param filename
         *            the filename containing the stoplist to load, located using the rules described above.
         * @return a lucene {@code CharArraySet} containing the stopwords. This is configured to be case insensitive.
         * @throws IOException
         *             if there is a problem finding or loading the specified stop word file..
         */
        public static CharArraySet loadStopWords(String filename) throws IOException {
            Closer closer = Closer.create();
            try {
                CharArraySet stopSet = new CharArraySet(16, true /* ignore case */);
                String pkg = Factory.class.getPackage().getName().replace('.', '/');
                String resource = filename.indexOf("/") > -1 ? filename : (pkg + "/" + filename);
                InputStream resourceStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(resource);
                logger.info("Loading stopwords file " + filename + " from resource " + resource);
                if (resourceStream == null) {
                    throw new FileNotFoundException("Unable to load stopword file as resource " + filename);
                }
                Reader reader = IOUtils.getDecodingReader(resourceStream, StandardCharsets.UTF_8);
                closer.register(reader);
                CharArraySet set = WordlistLoader.getWordSet(reader, "#", stopSet);
                logger.info("Loaded " + set.size() + " stopwords from " + filename + " (" + resource + ")");
                return set;
            } finally {
                closer.close();
            }
        }
    }
}
