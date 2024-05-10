package datawave.age.off.util;

import java.util.Iterator;
import java.util.Map;

public class AgeOffCsvToMatchPatternFormatterConfiguration {
    private String staticLabel;
    private char quoteCharacter;
    private boolean shouldQuoteLiteral;
    private boolean useColons; // when false, use equals sign
    private boolean shouldPadEquivalence; // when false, no spaces around equals sign
    private boolean shouldLowerCaseLiterals;
    private boolean shouldUpperCaseLiterals;
    private Map<String,String> valueMapping;
    private boolean useOverrides;
    private boolean disableLabel;
    private Iterator<String> input;

    private AgeOffCsvToMatchPatternFormatterConfiguration() {}

    public String getStaticLabel() {
        return staticLabel;
    }

    public char getQuoteCharacter() {
        return quoteCharacter;
    }

    public boolean shouldQuoteLiteral() {
        return shouldQuoteLiteral;
    }

    public boolean useColons() {
        return useColons;
    }

    public boolean useOverrides() {
        return useOverrides;
    }

    public boolean shouldPadEquivalence() {
        return shouldPadEquivalence;
    }

    public boolean shouldLowerCaseLiterals() {
        return shouldLowerCaseLiterals;
    }

    public boolean shouldUpperCaseLiterals() {
        return shouldUpperCaseLiterals;
    }

    public Map<String,String> getValueMapping() {
        return valueMapping;
    }

    public boolean shouldDisableLabel() {
        return disableLabel;
    }

    public Iterator<String> getInputIterator() {
        return input;
    }

    public static class Builder {

        final AgeOffCsvToMatchPatternFormatterConfiguration result = new AgeOffCsvToMatchPatternFormatterConfiguration();

        public Builder useStaticLabel(String label) {
            this.result.staticLabel = label;
            return this;
        }

        public Builder quoteLiterals(char quoteCharacter) {
            this.result.shouldQuoteLiteral = true;
            this.result.quoteCharacter = quoteCharacter;
            return this;
        }

        public Builder useColonForEquivalence() {
            this.result.useColons = true;
            return this;
        }

        public Builder padEquivalencesWithSpace() {
            this.result.shouldPadEquivalence = true;
            return this;
        }

        public Builder toUpperCaseLiterals() {
            this.result.shouldUpperCaseLiterals = true;
            return this;
        }

        public Builder toLowerCaseLiterals() {
            this.result.shouldLowerCaseLiterals = true;
            return this;
        }

        public Builder useAgeOffMapping(Map<String,String> valueMapping) {
            this.result.valueMapping = valueMapping;
            return this;
        }

        public AgeOffCsvToMatchPatternFormatterConfiguration build() {
            return result;
        }

        public Builder useOverrides() {
            this.result.useOverrides = true;
            return this;
        }

        public Builder disableLabel() {
            this.result.disableLabel = true;
            return this;
        }

        public Builder setInput(Iterator<String> input) {
            this.result.input = input;
            return this;
        }
    }
}
