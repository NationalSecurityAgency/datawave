package datawave.query.discovery;

import datawave.data.normalizer.NormalizationException;
import datawave.data.type.Type;

public class PatternNormalization implements Normalization {
    public String normalize(Type<?> normalizer, String field, String value) throws NormalizationException {
        return normalizer.normalizeRegex(value);
    }
}
