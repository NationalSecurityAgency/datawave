package datawave.webservice.results.mr;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import jakarta.xml.bind.annotation.XmlAccessOrder;
import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorOrder;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlElementWrapper;
import jakarta.xml.bind.annotation.XmlRootElement;

import datawave.webservice.result.BaseResponse;

@XmlRootElement(name = "MapReduceJobDescriptionList")
@XmlAccessorType(XmlAccessType.NONE)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class MapReduceJobDescriptionList extends BaseResponse implements Serializable {
    private static final long serialVersionUID = -3015609044850863121L;

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
