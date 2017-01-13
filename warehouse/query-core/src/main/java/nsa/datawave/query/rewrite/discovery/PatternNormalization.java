package nsa.datawave.query.rewrite.discovery;

import nsa.datawave.data.type.Type;
import nsa.datawave.data.normalizer.NormalizationException;

public class PatternNormalization implements Normalization {
    public String normalize(Type<?> normalizer, String field, String value) throws NormalizationException {
        return normalizer.normalizeRegex(value);
    }
}
