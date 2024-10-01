package datawave.query.planner.scanhints;

import datawave.query.config.ScanHintRule;

public abstract class BaseScanHintRule<T> implements ScanHintRule<T> {
    private String table = "shard";
    private String name;
    private String value;

    public String getTable() {
        return this.table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getHintName() {
        return this.name;
    }

    public void setHintName(String name) {
        this.name = name;
    }

    public String getHintValue() {
        return this.value;
    }

    public void setHintValue(String value) {
        this.value = value;
    }
}
