package datawave.query.config;

import java.io.Serializable;

import com.google.common.base.Objects;

import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.logic.BaseQueryLogic;
import datawave.microservice.query.Query;
import datawave.microservice.query.QueryImpl;
import datawave.query.tables.keyword.KeywordQueryLogic;
import datawave.query.tables.keyword.KeywordQueryState;
import datawave.util.keyword.YakeKeywordExtractor;

/**
 * Thin wrapper around GenericQueryConfiguration for use by the {@link KeywordQueryLogic}
 */
public class KeywordQueryConfiguration extends GenericQueryConfiguration implements Serializable {
    @SuppressWarnings("unused")
    private static final long serialVersionUID = 1678850178943683419L;

    int minNgrams = YakeKeywordExtractor.DEFAULT_MIN_NGRAMS;
    int maxNgrams = YakeKeywordExtractor.DEFAULT_MAX_NGRAMS;
    int maxKeywords = YakeKeywordExtractor.DEFAULT_KEYWORD_COUNT;
    float maxScore = YakeKeywordExtractor.DEFAULT_MAX_SCORE_THRESHOLD;
    int maxContentChars = YakeKeywordExtractor.DEFAULT_MAX_CONTENT_LENGTH;

    private transient KeywordQueryState state;

    public KeywordQueryConfiguration() {
        super();
        setQuery(new QueryImpl());
    }

    public KeywordQueryConfiguration(BaseQueryLogic<?> configuredLogic, Query query) {
        super(configuredLogic);
        setQuery(query);
    }

    public KeywordQueryConfiguration(KeywordQueryConfiguration other) {
        copyFrom(other);
    }

    public void copyFrom(KeywordQueryConfiguration other) {
        super.copyFrom(other);

        this.setMinNgrams(other.minNgrams);
        this.setMaxNgrams(other.maxNgrams);
        this.setMaxKeywords(other.maxKeywords);
        this.setMaxScore(other.maxScore);
        this.setMaxContentChars(other.maxContentChars);
        this.setState(other.getState());
    }

    public int getMaxContentChars() {
        return maxContentChars;
    }

    public void setMaxContentChars(int maxContentChars) {
        this.maxContentChars = maxContentChars;
    }

    public int getMaxKeywords() {
        return maxKeywords;
    }

    public void setMaxKeywords(int maxKeywords) {
        this.maxKeywords = maxKeywords;
    }

    public int getMaxNgrams() {
        return maxNgrams;
    }

    public void setMaxNgrams(int maxNgrams) {
        this.maxNgrams = maxNgrams;
    }

    public float getMaxScore() {
        return maxScore;
    }

    public void setMaxScore(float maxScore) {
        this.maxScore = maxScore;
    }

    public int getMinNgrams() {
        return minNgrams;
    }

    public void setMinNgrams(int minNgrams) {
        this.minNgrams = minNgrams;
    }

    public KeywordQueryState getState() {
        return state;
    }

    public void setState(KeywordQueryState state) {
        this.state = state;
    }

    /**
     * Factory method that instantiates a fresh KeywordQueryConfiguration
     *
     * @return - a clean KeywordQueryConfiguration
     */
    public static KeywordQueryConfiguration create() {
        return new KeywordQueryConfiguration();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass())
            return false;
        if (!super.equals(o))
            return false;
        KeywordQueryConfiguration that = (KeywordQueryConfiguration) o;
        return minNgrams == that.minNgrams && maxNgrams == that.maxNgrams && maxKeywords == that.maxKeywords && Float.compare(maxScore, that.maxScore) == 0
                        && maxContentChars == that.maxContentChars && Objects.equal(state, that.state);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(super.hashCode(), minNgrams, maxNgrams, maxKeywords, maxScore, maxContentChars, state);
    }

    // todo: implement serialization methods
}
