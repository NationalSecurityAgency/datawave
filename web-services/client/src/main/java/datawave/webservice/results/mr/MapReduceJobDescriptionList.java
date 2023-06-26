package datawave.webservice.results.mr;

import datawave.webservice.result.BaseResponse;

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@XmlRootElement(name = "MapReduceJobDescriptionList")
@XmlAccessorType(XmlAccessType.NONE)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class MapReduceJobDescriptionList extends BaseResponse implements Serializable {
    private static final long serialVersionUID = 1L;

    @XmlElementWrapper(name = "MapReduceJobDescriptionList")
    @XmlElement(name = "MapReduceJobDescription")
    List<MapReduceJobDescription> results = new ArrayList<MapReduceJobDescription>();

    public List<MapReduceJobDescription> getResults() {
        return results;
    }

    public void setResults(List<MapReduceJobDescription> results) {
        this.results = results;
    }
}
