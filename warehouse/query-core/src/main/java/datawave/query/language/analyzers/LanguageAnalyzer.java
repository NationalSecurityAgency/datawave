package datawave.query.language.analyzers;

import java.util.Set;

/**
 * Simple interface that supports language identification and expansion.
 * <p>
 * Implementations are provided as an example only, no guarantee is made about the accuracy, precision, or overall correctness.
 * <p>
 * Extending classes can support stemming, lemmatization, stop words, and stem exclusions, and n-grams.
 */
public abstract class LanguageAnalyzer {

    private boolean stemming = false;
    private boolean lemmatization = false;

    /**
     * Find alternates to the provided piece of text
     *
     * @param field
     *            the field
     * @param text
     *            the raw text
     * @return a list of alternates
     */
    public abstract Set<String> findAlternates(String field, String text);

    /**
     * Find a single alternate for the provided piece of text
     *
     * @param field
     *            the field
     * @param text
     *            the raw text
     * @return the best alternate
     */
    public abstract String findBestAlternate(String field, String text);

    /**
     * Determine if the provided text matches the language
     *
     * @param text
     *            the raw text
     * @return true if this analyzer should run
     */
    public abstract boolean matches(String text);

    /**
     * Getter for stemming support
     *
     * @return true if stemming is supported
     */
    public boolean isStemming() {
        return stemming;
    }

    /**
     * Setter for stemming support
     *
     * @param stemming
     *            flag for stemming support
     */
    public void setStemming(boolean stemming) {
        this.stemming = stemming;
    }

    /**
     * Getter for lemmatization support
     *
     * @return true is lemmatization is supported
     */
    public boolean isLemmatization() {
        return lemmatization;
    }

    /**
     * Setter for lemmatization support
     *
     * @param lemmatization
     *            flag for lemmatization support
     */
    public void setLemmatization(boolean lemmatization) {
        this.lemmatization = lemmatization;
    }

}
