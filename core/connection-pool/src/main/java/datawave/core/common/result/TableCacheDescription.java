package datawave.core.common.result;

import java.io.Serializable;
import java.util.Date;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
@XmlAccessorType(XmlAccessType.NONE)
public class TableCacheDescription implements Serializable {
    private static final long serialVersionUID = 1L;

    @XmlAttribute
    private String tableName = null;

    @XmlAttribute
    private String connectionPoolName = null;

    @XmlAttribute
    private String authorizations = null;

    @XmlAttribute
    private Long reloadInterval = null;

    @XmlAttribute
    private Long maxRows = null;

    @XmlAttribute
    private Date lastRefresh = null;

    @XmlAttribute
    private Boolean currentlyRefreshing = null;

    public String getTableName() {
        return tableName;
    }

    public String getConnectionPoolName() {
        return connectionPoolName;
    }

    public String getAuthorizations() {
        return authorizations;
    }

    public Long getReloadInterval() {
        return reloadInterval;
    }

    public Long getMaxRows() {
        return maxRows;
    }

    public Date getLastRefresh() {
        return lastRefresh;
    }

    public Boolean getCurrentlyRefreshing() {
        return currentlyRefreshing;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void setConnectionPoolName(String connectionPoolName) {
        this.connectionPoolName = connectionPoolName;
    }

    public void setAuthorizations(String authorizations) {
        this.authorizations = authorizations;
    }

    public void setReloadInterval(Long reloadInterval) {
        this.reloadInterval = reloadInterval;
    }

    public void setMaxRows(Long maxRows) {
        this.maxRows = maxRows;
    }

    public void setLastRefresh(Date lastRefresh) {
        this.lastRefresh = lastRefresh;
    }

    public void setCurrentlyRefreshing(Boolean currentlyRefreshing) {
        this.currentlyRefreshing = currentlyRefreshing;
    }

}
