package datawave.webservice.result;

import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;

import datawave.marking.Markings;
import datawave.webservice.query.result.event.FacetsBase;
import datawave.webservice.query.result.event.HasMarkings;

/**
 *
 */
@XmlAccessorType(XmlAccessType.NONE)
public abstract class FacetQueryResponseBase extends BaseQueryResponse implements HasMarkings {
    private static final long serialVersionUID = -3483112784845232037L;

    protected transient Markings<?> markings;

    public abstract Long getTotalEvents();

    public abstract long getTotalResults();

    public abstract List<? extends FacetsBase> getFacets();

    public abstract void setFacets(List<? extends FacetsBase> facets);

    public abstract void addFacet(FacetsBase facetInterface);
}
