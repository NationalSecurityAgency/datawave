package datawave.query.util.keyword;

import static java.util.Comparator.nullsLast;

import java.util.Comparator;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.base.Objects;

/**
 * A tag cloud entry, a single keyword with an accompanying score, frequency and list of sources where the keyword was found.
 */
public class TagCloudEntry implements Comparable<TagCloudEntry> {

    /** the keyword that represents the tag */
    final String keyword;
    /** the score/weight/importance of the keyword, range and polarity depends on the extraction algorithm */
    final double score;
    /**
     * the relative frequency of the keyword, could be based on the extraction algorithm, but minimally a count of items in sources
     */
    final int frequency;
    /** the sources where this tag was found */
    final Set<String> sources;

    protected TagCloudEntry(String keyword, double score, int frequency, Set<String> sources) {
        this.keyword = keyword;
        this.score = score;
        this.frequency = frequency;
        this.sources = sources;
    }

    public String getKeyword() {
        return keyword;
    }

    public double getScore() {
        return score;
    }

    public int getFrequency() {
        return frequency;
    }

    public Set<String> getSources() {
        return sources;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass())
            return false;
        TagCloudEntry that = (TagCloudEntry) o;
        return Double.compare(score, that.score) == 0 && frequency == that.frequency && Objects.equal(keyword, that.keyword);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(keyword, frequency, score);
    }

    @Override
    public int compareTo(@Nonnull TagCloudEntry other) {
        return naturalOrder.compare(this, other);
    }

    //@formatter:off
    public static final Comparator<TagCloudEntry> naturalOrder = nullsLast(Comparator
            .comparingDouble(TagCloudEntry::getScore)
            .thenComparing(TagCloudEntry::getFrequency)
            .thenComparing(TagCloudEntry::getKeyword));
    //@formatter:on

    /**
     * A builder for a tag cloud entry. Allows scores and sources for this keyword to be accumulated and the resulting entry produced with the build() call.
     */
    public static class Builder {
        String keyword;
        final SortedSet<ScoreTuple> sourceScores = new TreeSet<>();

        public Builder(String keyword) {
            this.keyword = keyword;
        }

        public void addSourceScore(String source, double score, String language) {
            sourceScores.add(new ScoreTuple(source, score, language));
        }

        public TagCloudEntry build() {
            // todo: make this modular/pluggable?
            //@formatter:off
            final double score = sourceScores.stream()
                    .map(ScoreTuple::getScore)
                    .min(Double::compareTo)
                    .orElse(1.0);

            final Set<String> sources = sourceScores.stream()
                    .map(ScoreTuple::getSource)
                    .collect(Collectors.toSet());
            //@formatter:on
            return new TagCloudEntry(keyword, score, sourceScores.size(), sources);
        }
    }

    /** A tuple of source, score and language used when building a tag cloud entry */
    public static class ScoreTuple implements Comparable<ScoreTuple> {
        final String source;
        final double score;
        final String language;

        public ScoreTuple(String source, double score, String language) {
            this.source = source;
            this.score = score;
            this.language = language;
        }

        public String getLanguage() {
            return language;
        }

        public double getScore() {
            return score;
        }

        public String getSource() {
            return source;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass())
                return false;
            ScoreTuple that = (ScoreTuple) o;
            return Double.compare(score, that.score) == 0 && Objects.equal(source, that.source) && Objects.equal(language, that.language);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(source, score, language);
        }

        @Override
        public int compareTo(@Nonnull ScoreTuple other) {
            return naturalOrder.compare(this, other);
        }

        //@formatter:off
        public static final Comparator<ScoreTuple> naturalOrder = nullsLast(Comparator
                .comparingDouble(ScoreTuple::getScore)
                .thenComparing(ScoreTuple::getSource)
                .thenComparing(ScoreTuple::getLanguage));
        //@formatter:on
    }
}
