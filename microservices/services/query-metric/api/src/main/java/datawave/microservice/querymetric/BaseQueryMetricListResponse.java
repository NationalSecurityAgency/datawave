package datawave.microservice.querymetric;

import java.util.List;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlTransient;

import org.springframework.web.servlet.ModelAndView;

import datawave.webservice.result.BaseResponse;

public abstract class BaseQueryMetricListResponse<T extends BaseQueryMetric> extends BaseResponse {
    
    private static final long serialVersionUID = 1L;
    @XmlElementWrapper(name = "queryMetrics")
    @XmlElement(name = "queryMetric")
    protected List<T> result = null;
    @XmlElement
    protected int numResults = 0;
    @XmlElement
    protected boolean isGeoQuery = false;
    @XmlTransient
    private boolean administratorMode = false;
    @XmlTransient
    protected String header;
    @XmlTransient
    protected String footer;
    
    protected String basePath = "/querymetric";
    protected String viewName = "querymetric";
    
    public void setBasePath(String basePath) {
        this.basePath = basePath;
    }
    
    private static String numToString(long number) {
        return (number == -1 || number == 0) ? "" : Long.toString(number);
    }
    
    public List<T> getResult() {
        return result;
    }
    
    public int getNumResults() {
        return numResults;
    }
    
    public void setResult(List<T> result) {
        this.result = result;
        this.numResults = this.result.size();
    }
    
    public void setNumResults(int numResults) {
        this.numResults = numResults;
    }
    
    public boolean isAdministratorMode() {
        return administratorMode;
    }
    
    public void setAdministratorMode(boolean administratorMode) {
        this.administratorMode = administratorMode;
    }
    
    public boolean isGeoQuery() {
        return isGeoQuery;
    }
    
    public void setGeoQuery(boolean geoQuery) {
        isGeoQuery = geoQuery;
    }
    
    public void setViewName(String viewName) {
        this.viewName = viewName;
    }
    
    abstract public ModelAndView createModelAndView();
    
    public void setHeader(String header) {
        this.header = header;
    }
    
    public void setFooter(String footer) {
        this.footer = footer;
    }
}
