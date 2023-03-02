package datawave.webservice.query.map;

import datawave.webservice.HtmlProvider;
import datawave.webservice.result.BaseResponse;

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This response includes information about what query geometries were present in a given query. The geometries are displayed on a map using leaflet.
 */
@XmlRootElement(name = "QueryGeometry")
@XmlAccessorType(XmlAccessType.NONE)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class QueryGeometryResponse extends BaseResponse implements HtmlProvider {
    
    private static final long serialVersionUID = 1L;
    
    private static final String TITLE = "Query Geometry";
    
    // @formatter:off
    private static final String LEAFLET_INCLUDES =
            "<link rel='stylesheet' type='text/css' href='/leaflet.css' />\n" +
            "<script type='text/javascript' src='/leaflet.js'></script>\n";
    private static final String JQUERY_INCLUDES =
            "<script type='text/javascript' src='/jquery.min.js'></script>\n";
    private static final String MAP_INCLUDES =
            "<link rel='stylesheet' type='text/css' href='/queryMap.css' />\n" +
            "<script type='text/javascript' src='/queryMap.js'></script>";
    // @formatter:on
    
    public QueryGeometryResponse() {
        this(null, null);
    }
    
    public QueryGeometryResponse(String queryId, String basemaps) {
        this.queryId = queryId;
        this.basemaps = basemaps;
    }
    
    @XmlElement(name = "queryId", nillable = true)
    protected String queryId = null;
    
    @XmlTransient
    protected String basemaps = null;
    
    @XmlElementWrapper(name = "features")
    @XmlElement(name = "feature")
    protected List<QueryGeometry> result = null;
    
    @Override
    public String getTitle() {
        if (queryId != null)
            return TITLE + " - " + queryId;
        return TITLE;
    }
    
    @Override
    public String getHeadContent() {
        String basemapData = "<script type='text/javascript'>var basemaps = " + basemaps + ";</script>\n";
        String featureData = "<script type='text/javascript'>var features = " + toGeoJsonFeatures() + ";</script>\n";
        return String.join("\n", featureData, JQUERY_INCLUDES, LEAFLET_INCLUDES, basemapData, MAP_INCLUDES);
    }
    
    @Override
    public String getPageHeader() {
        return getTitle();
    }
    
    @Override
    public String getMainContent() {
        return "<div align='center'><div id='map' style='height: calc(100% - 35px); width: 100%; position: fixed; top: 36px; left: 0px;'></div></div>";
    }
    
    private String toGeoJsonFeatures() {
        if (!this.result.isEmpty())
            return "[ " + this.result.stream().map(QueryGeometry::toGeoJsonFeature).collect(Collectors.joining(", ")) + " ]";
        else
            return "undefined";
    }
    
    public String getQueryId() {
        return queryId;
    }
    
    public void setQueryId(String queryId) {
        this.queryId = queryId;
    }
    
    public List<QueryGeometry> getResult() {
        return result;
    }
    
    public void setResult(List<QueryGeometry> result) {
        this.result = result;
    }
    
    public String getBasemaps() {
        return basemaps;
    }
    
    public void setBasemaps(String basemaps) {
        this.basemaps = basemaps;
    }
}
