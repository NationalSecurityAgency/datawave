package datawave.query.discovery;

import datawave.data.normalizer.NormalizationException;
import datawave.data.type.Type;

/**
 * The use case here was that a method worked the same for both patterns and literals, except for the method call on the normalizer. This abstracts that away
 * which call is made to make client code a bit more flexible.
 *
 * See {@link PatternNormalization} and {@link LiteralNormalization}.
 */
public interface Normalization {
    String normalize(Type<?> normalizer, String field, String value) throws NormalizationException;
}
