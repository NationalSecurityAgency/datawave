package datawave.query.discovery;

import datawave.data.type.Type;
import datawave.data.normalizer.NormalizationException;

public class PatternNormalization implements Normalization {
    public String normalize(Type<?> normalizer, String field, String value) throws NormalizationException {
        return normalizer.normalizeRegex(value);
    }
}
