package datawave.webservice.result.keyword;

import java.util.List;

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlAccessorType;

import datawave.webservice.result.BaseQueryResponse;

@XmlAccessorType(XmlAccessType.NONE)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public abstract class TagCloudResponseBase extends BaseQueryResponse {
    public abstract List<TagCloudBase> getTagClouds();

    public abstract void setTagClouds(List<TagCloudBase> tagClouds);

    public abstract void merge(TagCloudResponseBase other);
}
