package datawave.webservice.result;

import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;

import datawave.webservice.query.result.event.FacetsBase;
import datawave.webservice.query.result.event.HasMarkings;

/**
 *
 */
@XmlAccessorType(XmlAccessType.NONE)
public abstract class FacetQueryResponseBase extends BaseQueryResponse implements HasMarkings {

    protected transient Map<String,String> markings;

    public abstract Long getTotalEvents();

    public abstract long getTotalResults();

    public abstract List<? extends FacetsBase> getFacets();

    public abstract void setFacets(List<? extends FacetsBase> facets);

    public abstract void addFacet(FacetsBase facetInterface);

    public abstract void setMarkings(Map<String,String> markings);
}
