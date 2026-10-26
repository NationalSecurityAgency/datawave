package datawave.test.framework.generators.query.term;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;

import datawave.data.type.Type;

/**
 * Builds a {@code content:phrase} query from the first {@code wordCount} words of a field value, with an equality term per normalized word in its plan.
 */
public class PhraseTerm extends ValueDependentQueryTerm {

    private static final int DEFAULT_WORD_COUNT = 2;
    private static final String PHRASE_PREFIX = "content:phrase(";
    private static final String TERM_OFFSET_ARGUMENT = ", termOffsetMap, ";

    private final int wordCount;
    private Map<List<String>,List<Integer>> eventIdsByPhrase;

    public PhraseTerm() {
        this(DEFAULT_WORD_COUNT);
    }

    public PhraseTerm(int wordCount) {
        Preconditions.checkArgument(wordCount >= 2, "wordCount must be at least 2 to form a phrase");
        this.wordCount = wordCount;
    }

    @Override
    protected void prepareField() {
        // Tokens can be indexed (I/E/TF), index-only (I/TF), or event-only (E/TF).
        // Event-only phrases can use an indexed anchor elsewhere in the query.
        Preconditions.checkState(metadata.isContentField(), "phrase queries require a content field with term-frequency metadata");
        Preconditions.checkState(metadata.getNormalizers().size() == 1, "content fields must have exactly one normalizer");

        // Build once per field evaluation, rather than rescanning every value for each generated phrase.
        Type<?> normalizer = metadata.getNormalizers().get(0);
        Map<List<String>,Set<Integer>> matches = new HashMap<>();
        for (String value : new LinkedHashSet<>(metadata.getValues())) {
            List<Integer> ids = metadata.getEventIdsForValue(value);
            if (ids.isEmpty()) {
                continue;
            }

            // Match the writers' tokenization and normalization, preserving adjacent token positions.
            List<String> words = Arrays.stream(value.split(" ")).map(normalizer::normalize).collect(Collectors.toList());
            Set<List<String>> seen = new HashSet<>();
            for (int start = 0; start <= words.size() - wordCount; start++) {
                List<String> phrase = List.copyOf(words.subList(start, start + wordCount));
                if (seen.add(phrase)) {
                    matches.computeIfAbsent(phrase, key -> new TreeSet<>()).addAll(ids);
                }
            }
        }

        eventIdsByPhrase = new HashMap<>();
        matches.forEach((phrase, ids) -> eventIdsByPhrase.put(phrase, List.copyOf(ids)));
    }

    @Override
    protected void apply(String value) {
        Preconditions.checkNotNull(value, "Value cannot be null");

        String[] words = value.split(" ");
        Preconditions.checkState(words.length >= wordCount, "phrase does not contain enough words");

        List<String> selected = List.of(words).subList(0, wordCount);
        Type<?> normalizer = metadata.getNormalizers().get(0);
        List<String> normalizedWords = selected.stream().map(normalizer::normalize).collect(Collectors.toList());

        String field = metadata.getNormalizedFieldName();
        String quotedWords = selected.stream().map(w -> "'" + w + "'").collect(Collectors.joining(", "));
        String quotedNormalizedWords = normalizedWords.stream().map(w -> "'" + w + "'").collect(Collectors.joining(", "));
        String equalityTerms = normalizedWords.stream().map(w -> field + " == '" + w + "'").collect(Collectors.joining(" && "));

        this.queryTerm = PHRASE_PREFIX + field + TERM_OFFSET_ARGUMENT + quotedWords + ")";
        this.planTerm = PHRASE_PREFIX + field + TERM_OFFSET_ARGUMENT + quotedNormalizedWords + ") && " + equalityTerms;
        this.eventIds = eventIdsByPhrase.getOrDefault(normalizedWords, List.of());
    }
}
