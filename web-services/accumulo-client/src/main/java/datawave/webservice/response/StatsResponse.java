package datawave.webservice.response;

import java.util.List;

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;

import datawave.webservice.response.objects.Server;
import datawave.webservice.response.objects.Table;
import datawave.webservice.response.objects.Totals;
import datawave.webservice.result.BaseResponse;

@XmlRootElement(name = "stats")
@XmlAccessorType(XmlAccessType.FIELD)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class StatsResponse extends BaseResponse {
    
    private static final long serialVersionUID = 1L;
    
    @XmlElementWrapper(name = "servers")
    @XmlElement(name = "server")
    private List<Server> servers = null;
    
    @XmlElementWrapper(name = "tables")
    @XmlElement(name = "table")
    private List<Table> tables = null;
    
    @XmlElement
    private Totals totals = null;
    
    public List<Server> getServers() {
        return servers;
    }
    
    public List<Table> getTables() {
        return tables;
    }
    
    public Totals getTotals() {
        return totals;
    }
    
    public void setServers(List<Server> servers) {
        this.servers = servers;
    }
    
    public void setTables(List<Table> tables) {
        this.tables = tables;
    }
    
    public void setTotals(Totals totals) {
        this.totals = totals;
    }
    
}
