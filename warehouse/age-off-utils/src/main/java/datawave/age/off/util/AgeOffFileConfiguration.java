package datawave.age.off.util;

import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

public class AgeOffFileConfiguration {
    private Writer writer;
    private String parentFileName;
    private final List<AgeOffRuleConfiguration> ruleConfigurations = new ArrayList<>();
    private String indentation;

    private AgeOffFileConfiguration() {}

    public Writer getWriter() {
        return writer;
    }

    public String getParentFileName() {
        return parentFileName;
    }

    public String getIndentation() {
        return indentation;
    }

    public List<AgeOffRuleConfiguration> getRuleConfigurations() {
        return ruleConfigurations;
    }

    public static class Builder {
        private final AgeOffFileConfiguration fileConfiguration = new AgeOffFileConfiguration();
        private final List<AgeOffRuleConfiguration.Builder> ruleBuilders = new ArrayList<>();

        public Builder setWriter(Writer writer) {
            fileConfiguration.writer = writer;
            return this;
        }

        public Builder withParentFile(String parentFileName) {
            fileConfiguration.parentFileName = parentFileName;
            return this;
        }

        public Builder addNextRule(AgeOffRuleConfiguration.Builder ruleConfigurationBuilder) {
            ruleBuilders.add(ruleConfigurationBuilder);
            return this;
        }

        public Builder withIndentation(String indentation) {
            fileConfiguration.indentation = indentation;
            return this;
        }

        public AgeOffFileConfiguration build() {
            for (AgeOffRuleConfiguration.Builder ruleConfigurationBuilder : ruleBuilders) {
                // add two indentations: one for under the ageOffConfiguration element and another to go under the rules element
                String ruleIndentation = fileConfiguration.indentation + fileConfiguration.indentation;
                Writer indentingPatternWriter = new AgeOffRuleConfiguration.IndentingDelegatingWriter(ruleIndentation, fileConfiguration.getWriter());
                ruleConfigurationBuilder.setWriter(indentingPatternWriter);
                fileConfiguration.ruleConfigurations.add(ruleConfigurationBuilder.build());
            }
            return fileConfiguration;
        }
    }
}
