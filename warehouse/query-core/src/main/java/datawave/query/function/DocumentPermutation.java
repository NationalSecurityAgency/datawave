package datawave.query.function;

import datawave.query.attributes.Document;
import org.apache.accumulo.core.data.Key;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map.Entry;

/**
 * A document permutation is used to permute the fields in a document.
 *
 * NOTE: Implementations of a DocumentPermutation used with the ShardQueryLogic must have either a default constructor, or a constructor that takes in a
 * TypeMetadata object as a single argument.
 */
public interface DocumentPermutation extends Permutation<Entry<Key,Document>> {

    /**
     * This is a convenience implementation of DocumentPermutation that aggregates a list of document permutations
     */
    class DocumentPermutationAggregation implements DocumentPermutation {
        private final List<DocumentPermutation> permutations;

        public DocumentPermutationAggregation(List<DocumentPermutation> permutations) {
            this.permutations = permutations;
        }

        @Nullable
        @Override
        public Entry<Key,Document> apply(@Nullable Entry<Key,Document> input) {
            for (DocumentPermutation permutation : permutations) {
                input = permutation.apply(input);
            }
            return input;
        }
    }
}
