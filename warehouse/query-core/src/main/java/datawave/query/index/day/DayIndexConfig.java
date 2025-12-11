package datawave.query.index.day;

import java.util.Date;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.jexl3.parser.JexlNode;

import com.google.common.collect.Multimap;

import datawave.query.config.ShardQueryConfiguration;

/**
 * Holds relevant config options from the {@link ShardQueryConfiguration} and possibly others.
 */
public class DayIndexConfig {

    private JexlNode node;
    private Set<String> indexedFields;
    private Date startDate;
    private Date endDate;

    private int numIndexThreads;

    // when the number of days in a query date range exceed this threshold, use the
    // year index as a filter
    private int dayIndexThreshold;

    private String dayIndexTableName;
    private String yearIndexTableName;
    private Set<Authorizations> auths;
    private AccumuloClient client;

    private Multimap<String,String> valuesAndFields;

    /**
     * Constructor that copies relevant fields from the {@link ShardQueryConfiguration}.
     *
     * @param config
     *            a ShardQueryConfiguration
     */
    public DayIndexConfig(ShardQueryConfiguration config) {
        this.node = config.getQueryTree();
        this.indexedFields = config.getIndexedFields();
        this.startDate = config.getBeginDate();
        this.endDate = config.getEndDate();
        this.numIndexThreads = config.getNumIndexLookupThreads();
        this.dayIndexThreshold = config.getDayIndexThreshold();
        this.dayIndexTableName = config.getDayIndexTableName();
        this.yearIndexTableName = config.getYearIndexTableName();
        this.auths = config.getAuthorizations();
        this.client = config.getClient();
    }

    public JexlNode getNode() {
        return node;
    }

    public void setNode(JexlNode node) {
        this.node = node;
    }

    public Set<String> getIndexedFields() {
        return indexedFields;
    }

    public void setIndexedFields(Set<String> indexedFields) {
        this.indexedFields = indexedFields;
    }

    public Date getStartDate() {
        return startDate;
    }

    public void setStartDate(Date startDate) {
        this.startDate = startDate;
    }

    public Date getEndDate() {
        return endDate;
    }

    public void setEndDate(Date endDate) {
        this.endDate = endDate;
    }

    public int getNumIndexThreads() {
        return numIndexThreads;
    }

    public void setNumIndexThreads(int numIndexThreads) {
        this.numIndexThreads = numIndexThreads;
    }

    public String getDayIndexTableName() {
        return dayIndexTableName;
    }

    public void setDayIndexTableName(String dayIndexTableName) {
        this.dayIndexTableName = dayIndexTableName;
    }

    public Set<Authorizations> getAuths() {
        return auths;
    }

    public void setAuths(Set<Authorizations> auths) {
        this.auths = auths;
    }

    public AccumuloClient getClient() {
        return client;
    }

    public void setClient(AccumuloClient client) {
        this.client = client;
    }

    public Multimap<String,String> getValuesAndFields() {
        return valuesAndFields;
    }

    public void setValuesAndFields(Multimap<String,String> valuesAndFields) {
        this.valuesAndFields = valuesAndFields;
    }

    public int getDayIndexThreshold() {
        return dayIndexThreshold;
    }

    public void setDayIndexThreshold(int dayIndexThreshold) {
        this.dayIndexThreshold = dayIndexThreshold;
    }

    public String getYearIndexTableName() {
        return yearIndexTableName;
    }

    public void setYearIndexTableName(String yearIndexTableName) {
        this.yearIndexTableName = yearIndexTableName;
    }
}
