package datawave.query.cardinality;

import java.util.ArrayList;
import java.util.List;

public class CardinalityScannerConfiguration {

    private String zookeepers;
    private String instanceName;
    private String username;
    private String password;
    private String tableName;
    private String auths;

    private String beginDate = null;
    private String endDate = null;
    private List<String> fields = new ArrayList<>();
    private CardinalityScanner.DateAggregationType dateAggregationMode = CardinalityScanner.DateAggregationType.DAY;
    private boolean maintainDatatypes = true;
    private boolean intersect = false;
    private boolean sortByCardinality = false;

    public String getZookeepers() {
        return zookeepers;
    }

    public void setZookeepers(String zookeepers) {
        this.zookeepers = zookeepers;
    }

    public String getInstanceName() {
        return instanceName;
    }

    public void setInstanceName(String instanceName) {
        this.instanceName = instanceName;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getAuths() {
        return auths;
    }

    public void setAuths(String auths) {
        this.auths = auths;
    }

    public String getBeginDate() {
        return beginDate;
    }

    public void setBeginDate(String beginDate) {
        this.beginDate = beginDate;
    }

    public String getEndDate() {
        return endDate;
    }

    public void setEndDate(String endDate) {
        this.endDate = endDate;
    }

    public List<String> getFields() {
        return fields;
    }

    public void setFields(List<String> fields) {
        this.fields = fields;
    }

    public void setDateAggregateMode(CardinalityScanner.DateAggregationType dateAggregationMode) {
        this.dateAggregationMode = dateAggregationMode;
    }

    public void setMaintainDatatypes(boolean maintainDatatypes) {
        this.maintainDatatypes = maintainDatatypes;
    }

    public CardinalityScanner.DateAggregationType getDateAggregateMode() {
        return dateAggregationMode;
    }

    public boolean getMaintainDatatypes() {
        return maintainDatatypes;
    }

    public void setIntersect(boolean intersect) {
        this.intersect = intersect;
    }

    public boolean getIntersect() {
        return intersect;
    }

    public boolean getSortByCardinality() {
        return sortByCardinality;
    }

    public void setSortByCardinality(boolean sortByCardinality) {
        this.sortByCardinality = sortByCardinality;
    }
}
