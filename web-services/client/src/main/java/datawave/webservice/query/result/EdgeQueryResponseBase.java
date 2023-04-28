package datawave.webservice.query.result;

import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;

import datawave.webservice.query.result.edge.EdgeBase;
import datawave.webservice.query.result.event.HasMarkings;
import datawave.webservice.result.BaseQueryResponse;

@XmlAccessorType(XmlAccessType.NONE)
public abstract class EdgeQueryResponseBase extends BaseQueryResponse implements HasMarkings {
    
    protected Map<String,String> markings;
    
    public abstract void addEdge(EdgeBase edge);

    public abstract void setEdges(List<EdgeBase> edges);
    
    public abstract List<? extends EdgeBase> getEdges();
    
    public abstract void setTotalResults(long totalResults);
    
    public abstract long getTotalResults();
    
}
