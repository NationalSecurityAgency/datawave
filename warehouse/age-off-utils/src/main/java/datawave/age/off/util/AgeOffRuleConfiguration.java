package datawave.age.off.util;

import java.util.ArrayList;

import javax.xml.bind.JAXBElement;

import datawave.iterators.filter.ageoff.FilterRule;

public class AgeOffRuleConfiguration {
    private static final String DEFAULT_INDENTATION = "     ";
    private AgeOffCsvToMatchPatternFormatterConfiguration patternConfiguration;
    private String ruleLabel;
    private Class<? extends FilterRule> filterClass;
    private boolean shouldMerge;
    private String indentation = DEFAULT_INDENTATION;
    private String ttlDuration;
    private String ttlUnits;
    private ArrayList<JAXBElement<?>> customElements;

    private AgeOffRuleConfiguration() {}

    public String getIndentation() {
        return indentation;
    }

    public ArrayList<JAXBElement<?>> getCustomElements() {
        return customElements;
    }

    public boolean shouldMerge() {
        return shouldMerge;
    }

    public Class<? extends FilterRule> getFilterClass() {
        return filterClass;
    }

    public String getRuleLabel() {
        return ruleLabel;
    }

    public AgeOffCsvToMatchPatternFormatterConfiguration getPatternConfiguration() {
        return patternConfiguration;
    }

    public String getTtlUnits() {
        return ttlUnits;
    }

    public String getTtlDuration() {
        return ttlDuration;
    }

    public static class Builder {
        private final AgeOffRuleConfiguration result = new AgeOffRuleConfiguration();
        private AgeOffCsvToMatchPatternFormatterConfiguration.Builder patternConfigurationBuilder;

        public Builder withPatternConfigurationBuilder(AgeOffCsvToMatchPatternFormatterConfiguration.Builder patternConfigurationBuilder) {
            this.patternConfigurationBuilder = patternConfigurationBuilder;
            return this;
        }

        public Builder withRuleLabel(String ruleLabel) {
            result.ruleLabel = ruleLabel;
            return this;
        }

        public Builder withFilterClass(Class<? extends FilterRule> filterClass) {
            result.filterClass = filterClass;
            return this;
        }

        public Builder useMerge() {
            result.shouldMerge = true;
            return this;
        }

        public Builder withIndentation(String indentation) {
            result.indentation = indentation;
            return this;
        }

        public Builder withTtl(String duration, String units) {
            result.ttlDuration = duration;
            result.ttlUnits = units;
            return this;
        }

        public AgeOffRuleConfiguration build() {
            if (this.patternConfigurationBuilder != null) {
                result.patternConfiguration = this.patternConfigurationBuilder.build();
            }
            return result;
        }

        public void addCustomElement(JAXBElement<?> customElement) {
            if (result.customElements == null) {
                result.customElements = new ArrayList<>();
            }
            result.customElements.add(customElement);
        }
    }
}
