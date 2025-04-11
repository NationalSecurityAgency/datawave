package datawave.ingest.data.config.ingest;

import datawave.ingest.data.config.ingest.WhindexIngest;

import java.util.List;
import java.util.Objects;

public class WhindexConfig {

    private String valueField;
    private List<String> values;
    private String sourceField;
    private String destField;
    private boolean overloaded;

    public String getValueField() {
        return valueField;
    }

    public void setValueField(String valueField) {
        this.valueField = valueField;
    }

    public List<String> getValues() {
        return values;
    }

    public void setValues(List<String> values) {
        this.values = values;
    }

    public String getSourceField() {
        return sourceField;
    }

    public void setSourceField(String sourceField) {
        this.sourceField = sourceField;
    }

    public String getDestField() {
        return destField;
    }

    public void setDestField(String destField) {
        this.destField = destField;
    }

    public boolean isOverloaded() {
        return overloaded;
    }

    public void setOverloaded(boolean overloaded) {
        this.overloaded = overloaded;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        WhindexConfig config = (WhindexConfig) o;
        return overloaded == config.overloaded && Objects.equals(valueField, config.valueField) && Objects.equals(values, config.values) && Objects.equals(sourceField, config.sourceField) && Objects.equals(destField, config.destField);
    }

    @Override
    public int hashCode() {
        return Objects.hash(valueField, values, sourceField, destField, overloaded);
    }

}