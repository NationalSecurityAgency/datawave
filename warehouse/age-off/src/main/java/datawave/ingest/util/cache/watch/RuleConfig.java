package datawave.ingest.util.cache.watch;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.w3c.dom.Element;

public class RuleConfig {
    private final int priority;
    private final String filterClassName;
    private final String ttlValue;
    private final String ttlUnits;
    private final String matchPattern;
    private final String label;
    private final boolean isMerge;
    private final Map<String,String> extendedOptions;
    private final List<Element> customElements;

    private RuleConfig(Builder builder) {
        this.priority = builder.priority;
        this.filterClassName = builder.filterClassName;
        this.ttlValue = builder.ttlValue;
        this.ttlUnits = builder.ttlUnits;
        this.matchPattern = builder.matchPattern;
        this.label = builder.label;
        this.isMerge = builder.isMerge;
        this.extendedOptions = builder.extendedOptions;
        this.customElements = builder.customElements;
    }

    public int getPriority() {
        return priority;
    }

    public String getFilterClassName() {
        return filterClassName;
    }

    public String getTtlValue() {
        return ttlValue;
    }

    public String getTtlUnits() {
        return ttlUnits;
    }

    public String getMatchPattern() {
        return matchPattern;
    }

    public String getLabel() {
        return label;
    }

    public boolean getIsMerge() {
        return isMerge;
    }

    public Map<String,String> getExtendedOptions() {
        return extendedOptions;
    }

    public List<Element> getCustomElements() {
        return customElements;
    }

    /**
     * Builder for a {@link RuleConfig}
     */
    public static class Builder {
        public int priority;
        public String filterClassName;
        public String ttlValue = null;
        public String ttlUnits = null;
        public String matchPattern = null;
        public String label;
        public boolean isMerge = false;
        public Map<String,String> extendedOptions = new HashMap<>();
        public List<Element> customElements = new ArrayList<>();

        public Builder(String filterClassName, int priority) {
            this.filterClassName = filterClassName;
            this.priority = priority;
        }

        public Builder setTtlValue(String ttlValue) {
            this.ttlValue = ttlValue;
            return this;
        }

        public Builder setTtlUnits(String ttlUnits) {
            this.ttlUnits = ttlUnits;
            return this;
        }

        public Builder setMatchPattern(String matchPattern) {
            this.matchPattern = matchPattern;
            return this;
        }

        public Builder setLabel(String label) {
            this.label = label;
            return this;
        }

        public Builder setIsMerge(boolean isMerge) {
            this.isMerge = isMerge;
            return this;
        }

        public Builder setExtendedOptions(Map<String,String> extendedOptions) {
            this.extendedOptions = extendedOptions;
            return this;
        }

        public Builder setCustomElements(List<Element> customElements) {
            this.customElements = customElements;
            return this;
        }

        public RuleConfig build() {
            return new RuleConfig(this);
        }

        public String getLabel() {
            return label;
        }
    }
}
