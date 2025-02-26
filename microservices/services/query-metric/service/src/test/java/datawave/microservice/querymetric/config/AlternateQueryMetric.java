package datawave.microservice.querymetric.config;

import javax.xml.bind.annotation.XmlElement;

import datawave.microservice.querymetric.BaseQueryMetric;
import datawave.microservice.querymetric.QueryMetric;

public class AlternateQueryMetric extends QueryMetric {
    
    @XmlElement
    private String extraField;
    
    public AlternateQueryMetric() {
        super();
    }
    
    public AlternateQueryMetric(AlternateQueryMetric other) {
        super(other);
        this.extraField = other.extraField;
    }
    
    @Override
    public BaseQueryMetric duplicate() {
        return new AlternateQueryMetric(this);
    }
    
    public void setExtraField(String extraField) {
        this.extraField = extraField;
    }
    
    public String getExtraField() {
        return extraField;
    }
}
