package datawave.test.framework.generators.query.term;

import java.util.List;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;

import datawave.data.type.Type;

/**
 * Builds a {@code content:phrase} query from the first {@code wordCount} words of a field value, with an equality term per normalized word in its plan.
 */
public class PhraseTerm extends ValueDependentQueryTerm {

    private static final int DEFAULT_WORD_COUNT = 2;

    private final int wordCount;

    public PhraseTerm() {
        this(DEFAULT_WORD_COUNT);
    }

    public PhraseTerm(int wordCount) {
        Preconditions.checkArgument(wordCount >= 2, "wordCount must be at least 2 to form a phrase");
        this.wordCount = wordCount;
    }

    @Override
    protected void apply(String value) {
        Preconditions.checkNotNull(value, "Value cannot be null");
        Preconditions.checkState(metadata.getNormalizers().size() == 1, "content fields must have exactly one normalizer");

        String[] words = value.split(" ");
        Preconditions.checkState(words.length >= wordCount, "phrase does not contain enough words");

        List<String> selected = List.of(words).subList(0, wordCount);
        Type<?> normalizer = metadata.getNormalizers().get(0);
        List<String> normalizedWords = selected.stream().map(normalizer::normalize).collect(Collectors.toList());

        String field = metadata.getNormalizedFieldName();
        String quotedWords = selected.stream().map(w -> "'" + w + "'").collect(Collectors.joining(", "));
        String quotedNormalizedWords = normalizedWords.stream().map(w -> "'" + w + "'").collect(Collectors.joining(", "));
        String equalityTerms = normalizedWords.stream().map(w -> field + " == '" + w + "'").collect(Collectors.joining(" && "));

        this.queryTerm = "content:phrase(" + field + ", termOffsetMap, " + quotedWords + ")";
        this.planTerm = "content:phrase(" + field + ", termOffsetMap, " + quotedNormalizedWords + ") && " + equalityTerms;
        this.eventIds = metadata.getEventIdsForValue(value);
    }
}
