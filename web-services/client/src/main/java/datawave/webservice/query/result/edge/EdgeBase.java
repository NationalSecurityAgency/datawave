package datawave.webservice.query.result.edge;

import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlSeeAlso;

import datawave.webservice.query.result.event.HasMarkings;

@XmlAccessorType(XmlAccessType.NONE)
@XmlSeeAlso(DefaultEdge.class)
public interface EdgeBase extends HasMarkings {
    String getSource();

    void setSource(String source);

    String getSink();

    void setSink(String sink);

    String getEdgeType();

    void setEdgeType(String edgeType);

    String getEdgeRelationship();

    void setEdgeRelationship(String edgeRelationship);

    String getEdgeAttribute1Source();

    void setEdgeAttribute1Source(String edgeAttribute1);

    String getEdgeAttribute2();

    void setEdgeAttribute2(String edgeAttribute2);

    String getEdgeAttribute3();

    void setEdgeAttribute3(String edgeAttribute3);

    String getStatsType();

    void setStatsType(String statsType);

    String getDate();

    void setDate(String date);

    Long getCount();

    void setCount(Long count);

    List<Long> getCounts();

    void setCounts(List<Long> counts);

    String getLoadDate();

    void setLoadDate(String loadDate);

    String getActivityDate();

    void setActivityDate(String activityDate);

}
