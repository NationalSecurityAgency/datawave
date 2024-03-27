package datawave.age.off.util;

import java.util.ArrayList;
import java.util.List;

public class AgeOffFileConfiguration {
    private String parentFileName;
    private final List<AgeOffRuleConfiguration> ruleConfigurations = new ArrayList<>();
    private String indentation;

    private AgeOffFileConfiguration() {}

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
                fileConfiguration.ruleConfigurations.add(ruleConfigurationBuilder.build());
            }
            return fileConfiguration;
        }
    }
}
