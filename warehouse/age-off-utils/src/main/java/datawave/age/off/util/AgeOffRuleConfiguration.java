package datawave.age.off.util;

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;

import javax.xml.bind.JAXBElement;

import datawave.iterators.filter.ageoff.FilterRule;

public class AgeOffRuleConfiguration {
    private static final String DEFAULT_INDENTATION = "     ";
    private AgeOffCsvToMatchPatternFormatterConfiguration patternConfiguration;
    private String ruleLabel;
    private Class<? extends FilterRule> filterClass;
    private boolean shouldMerge;
    private Writer writer;
    private String indentation = DEFAULT_INDENTATION;
    private String ttlDuration;
    private String ttlUnits;
    private ArrayList<JAXBElement<?>> customElements;

    private AgeOffRuleConfiguration() {}

    public Writer getWriter() {
        return writer;
    }

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

        public Builder setWriter(Writer writer) {
            result.writer = writer;
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
                // add two indentations: one for items under the rule element and another for items under the matchPattern element
                Writer indentingPatternWriter = new IndentingDelegatingWriter(result.indentation + result.indentation, result.getWriter());
                this.patternConfigurationBuilder.setWriter(indentingPatternWriter);
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

    static class IndentingDelegatingWriter extends Writer {
        private final Writer writer;
        private final String indentation;

        public IndentingDelegatingWriter(String indentation, Writer writer) {
            this.indentation = indentation;
            this.writer = writer;
        }

        @Override
        public void write(String str) throws IOException {
            writer.write(this.indentation + str);
        }

        @Override
        public void write(char[] chars, int i, int i1) throws IOException {
            writer.write(chars, i, i1);
        }

        @Override
        public void flush() throws IOException {
            writer.flush();
        }

        @Override
        public void close() throws IOException {
            writer.close();
        }
    }
}
