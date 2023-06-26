package datawave.webservice.result;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.XmlType;

import datawave.webservice.query.exception.QueryExceptionType;
import datawave.webservice.query.result.event.DefaultFacets;
import datawave.webservice.query.result.event.FacetsBase;
import io.protostuff.Input;
import io.protostuff.Message;
import io.protostuff.Output;
import io.protostuff.Schema;

/**
 * A response object for holding data returned from edge table queries.
 */
@XmlRootElement(name = "FacetQueryResponse")
@XmlAccessorType(XmlAccessType.NONE)
@XmlType(propOrder = {"totalEvents", "facets"})
public class FacetQueryResponse extends FacetQueryResponseBase implements Serializable, TotalResultsAware, Message<FacetQueryResponse> {
    private static final long serialVersionUID = -8080688956850811620L;

    @XmlElement(name = "TotalEvents")
    private Long totalEvents = null;

    @XmlElementWrapper(name = "facets")
    @XmlElement(name = "facets")
    private List<DefaultFacets> facets = null;

    public FacetQueryResponse() {}

    public FacetQueryResponse(List<DefaultFacets> facets) {
        this.facets = new ArrayList<DefaultFacets>(facets);
    }

    public void setMarkings(Map<String,String> markings) {
        this.markings = markings;
    }

    public Map<String,String> getMarkings() {
        return markings;
    }

    public void addFacet(FacetsBase facetInterface) {
        DefaultFacets facet = (DefaultFacets) facetInterface;
        if (facets == null)
            facets = new ArrayList<DefaultFacets>();
        facets.add(facet);
    }

    public List<? extends FacetsBase> getFacets() {
        return Collections.unmodifiableList(facets);
    }

    @Override
    public void setFacets(List<? extends FacetsBase> facets) {

    }

    public static Schema<FacetQueryResponse> getSchema() {
        return SCHEMA;
    }

    @Override
    public Schema<FacetQueryResponse> cachedSchema() {
        return SCHEMA;
    }

    @XmlTransient
    private static final Schema<FacetQueryResponse> SCHEMA = new Schema<FacetQueryResponse>() {
        // schema methods

        public FacetQueryResponse newMessage() {
            return new FacetQueryResponse();
        }

        public Class<FacetQueryResponse> typeClass() {
            return FacetQueryResponse.class;
        }

        public String messageName() {
            return FacetQueryResponse.class.getSimpleName();
        }

        public String messageFullName() {
            return FacetQueryResponse.class.getName();
        }

        public boolean isInitialized(FacetQueryResponse message) {
            return true;
        }

        public void writeTo(Output output, FacetQueryResponse message) throws IOException {

            if (message.getQueryId() != null) {
                output.writeString(1, message.getQueryId(), false);
            }

            if (message.getLogicName() != null) {
                output.writeString(2, message.getLogicName(), false);
            }

            output.writeUInt64(3, message.getOperationTimeMS(), false);

            List<String> messages = message.getMessages();
            if (messages != null) {
                for (String msg : messages) {
                    if (msg != null)
                        output.writeString(4, msg, true);
                }
            }

            List<QueryExceptionType> exceptions = message.getExceptions();
            if (exceptions != null) {
                for (QueryExceptionType exception : exceptions) {
                    if (exception != null)
                        output.writeObject(5, exception, QueryExceptionType.getSchema(), true);
                }
            }

            if (message.facets != null) {
                for (DefaultFacets facet : message.facets) {
                    if (facet != null)
                        output.writeObject(6, facet, DefaultFacets.getSchema(), true);
                }
            }
        }

        public void mergeFrom(Input input, FacetQueryResponse message) throws IOException {
            LinkedList<QueryExceptionType> exceptions = null;
            int number;
            while ((number = input.readFieldNumber(this)) != 0) {
                switch (number) {
                    case 1:
                        message.setQueryId(input.readString());
                        break;
                    case 2:
                        message.setLogicName(input.readString());
                        break;
                    case 3:
                        message.setOperationTimeMS(input.readUInt64());
                        break;
                    case 4:
                        message.addMessage(input.readString());
                        break;
                    case 5:
                        if (exceptions == null)
                            exceptions = new LinkedList<QueryExceptionType>();
                        exceptions.add(input.mergeObject(null, QueryExceptionType.getSchema()));
                        break;
                    case 6:
                        if (message.facets == null)
                            message.facets = new ArrayList<DefaultFacets>();
                        message.facets.add(input.mergeObject(null, DefaultFacets.getSchema()));
                        break;
                    default:
                        input.handleUnknownField(number, this);
                        break;
                }
            }
            if (exceptions != null)
                message.setExceptions(exceptions);
        }

        public String getFieldName(int number) {
            switch (number) {
                case 1:
                    return "queryId";
                case 2:
                    return "logicName";
                case 3:
                    return "operationTimeMs";
                case 4:
                    return "messages";
                case 5:
                    return "exceptions";
                case 6:
                    return "acetsf";
                default:
                    return null;
            }
        }

        public int getFieldNumber(String name) {
            final Integer number = fieldMap.get(name);
            return number == null ? 0 : number.intValue();
        }

        final java.util.HashMap<String,Integer> fieldMap = new java.util.HashMap<String,Integer>();
        {
            fieldMap.put("queryId", 1);
            fieldMap.put("logicName", 2);
            fieldMap.put("operationTimeMs", 3);
            fieldMap.put("messages", 4);
            fieldMap.put("exceptions", 5);
            fieldMap.put("facets", 6);
        }
    };

    @Override
    public void setTotalResults(long totalResults) {
        this.totalEvents = totalResults;
    }

    @Override
    public long getTotalResults() {
        return totalEvents == null ? -1 : totalEvents;
    }

    public Long getTotalEvents() {
        return totalEvents;
    }

    public void setTotalEvents(Long totalEvents) {
        this.totalEvents = totalEvents;
    }
}
