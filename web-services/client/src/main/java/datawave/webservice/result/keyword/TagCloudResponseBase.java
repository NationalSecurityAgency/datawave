package datawave.webservice.result.keyword;

import java.util.List;

import jakarta.xml.bind.annotation.XmlAccessOrder;
import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorOrder;
import jakarta.xml.bind.annotation.XmlAccessorType;

import datawave.webservice.result.BaseQueryResponse;

@XmlAccessorType(XmlAccessType.NONE)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public abstract class TagCloudResponseBase extends BaseQueryResponse {
    public abstract List<TagCloudBase> getTagClouds();

    public abstract void setTagClouds(List<TagCloudBase> tagClouds);

    public abstract void merge(TagCloudResponseBase other);
}
