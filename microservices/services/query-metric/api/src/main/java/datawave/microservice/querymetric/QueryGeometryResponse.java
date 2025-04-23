package datawave.microservice.querymetric;

import java.util.List;
import java.util.stream.Collectors;

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import org.springframework.web.servlet.ModelAndView;

import com.fasterxml.jackson.annotation.JsonIgnore;

import datawave.webservice.result.BaseResponse;

/**
 * This response includes information about what query geometries were present in a given query. The geometries are displayed on a map using leaflet.
 */
@XmlRootElement(name = "QueryGeometry")
@XmlAccessorType(XmlAccessType.NONE)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class QueryGeometryResponse extends BaseResponse {
    
    private static final long serialVersionUID = 1L;
    
    @XmlTransient
    protected String header;
    @XmlTransient
    protected String footer;
    
    protected String basePath = "/querymetric";
    
    public QueryGeometryResponse() {}
    
    public void setBasePath(String basePath) {
        this.basePath = basePath;
    }
    
    public ModelAndView createModelAndView() {
        ModelAndView mav = new ModelAndView();
        mav.setViewName("querymetricgeometry");
        mav.addObject("basemapScript", "var basemaps = " + this.basemaps + ";");
        mav.addObject("geoJsonFeaturesScript", "var features = " + toGeoJsonFeatures() + ";");
        mav.addObject("basePath", this.basePath);
        mav.addObject("queryId", getQueryId());
        mav.addObject("header", header);
        mav.addObject("footer", footer);
        return mav;
    }
    
    @XmlElement(name = "queryId", nillable = true)
    protected String queryId = null;
    
    @JsonIgnore
    @XmlTransient
    protected String basemaps = null;
    
    @XmlElementWrapper(name = "features")
    @XmlElement(name = "feature")
    protected List<QueryGeometry> result = null;
    
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
    
    public void setHeader(String header) {
        this.header = header;
    }
    
    public void setFooter(String footer) {
        this.footer = footer;
    }
}
