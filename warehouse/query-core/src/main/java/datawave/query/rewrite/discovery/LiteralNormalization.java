package datawave.query.rewrite.discovery;

import datawave.data.type.Type;
import datawave.data.normalizer.NormalizationException;

public class LiteralNormalization implements Normalization {
    public String normalize(Type<?> normalizer, String field, String value) throws NormalizationException {
        return normalizer.normalize(value);
    }
}
