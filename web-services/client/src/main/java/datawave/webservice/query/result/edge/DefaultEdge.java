package datawave.webservice.query.result.edge;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.protostuff.Input;
import io.protostuff.Message;
import io.protostuff.Output;
import io.protostuff.Schema;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

import datawave.webservice.query.util.OptionallyEncodedStringAdapter;

@XmlAccessorType(XmlAccessType.NONE)
public class DefaultEdge implements EdgeBase, Serializable, Message<DefaultEdge> {

    private static final String COLUMN_VISIBILITY = "columnVisibility";

    private static final long serialVersionUID = -1621626861385080614L;

    protected Map<String,String> markings;

    // The visibility of this edge
    @XmlElement(name = "ColumnVisibility", required = true, nillable = false)
    private String columnVisibility;

    // The identifier for this edge
    @XmlElement(name = "Identifier", required = true, nillable = false)
    @XmlJavaTypeAdapter(OptionallyEncodedStringAdapter.class)
    private String source;

    // The related identifier for this edge, or NULL if this is a stats edge.
    @XmlElement(name = "RelatedIdentifier", required = false)
    @XmlJavaTypeAdapter(OptionallyEncodedStringAdapter.class)
    private String sink;

    @XmlAttribute
    private String edgeType;

    @XmlAttribute(required = true)
    private String edgeRelationship;

    @XmlAttribute(required = true)
    private String edgeAttribute1Source;

    // The type of stats edge, or NULL if this is not a stats edge
    @XmlAttribute(required = false)
    private String statsType;

    // The date to which this edge applies
    @XmlAttribute(required = true)
    private String date;

    // The number of events collected between source and sink on date.
    // If this is a stats edge, this will be null.
    @XmlElement(name = "Count", required = false)
    private Long count;

    @JsonProperty("Counts")
    // work-around for bug in jackson-databind
    @XmlElementWrapper(name = "Counts", required = false)
    @XmlElement(name = "Count", required = false)
    private List<Long> counts;

    @XmlAttribute(required = false)
    private String edgeAttribute3;

    @XmlAttribute(required = false)
    private String edgeAttribute2;

    @XmlElement(name = "LoadDate", required = false)
    private String loadDate;

    @XmlElement(name = "ActivityDate", required = false)
    private String activityDate;

    public String getColumnVisibility() {
        return columnVisibility;
    }

    public void setColumnVisibility(String cv) {
        this.columnVisibility = cv;
    }

    @Override
    public String getSource() {
        return source;
    }

    @Override
    public String getSink() {
        return sink;
    }

    @Override
    public void setSource(String source) {
        this.source = source;
    }

    @Override
    public void setSink(String sink) {
        this.sink = sink;
    }

    @Override
    public String getEdgeType() {
        return edgeType;
    }

    @Override
    public void setEdgeType(String edgeType) {
        this.edgeType = edgeType;
    }

    @Override
    public String getEdgeRelationship() {
        return edgeRelationship;
    }

    @Override
    public void setEdgeRelationship(String edgeRelationship) {
        this.edgeRelationship = edgeRelationship;
    }

    @Override
    public String getEdgeAttribute1Source() {
        return edgeAttribute1Source;
    }

    @Override
    public void setEdgeAttribute1Source(String edgeAttribute1Source) {
        this.edgeAttribute1Source = edgeAttribute1Source;
    }

    @Override
    public String getStatsType() {
        return statsType;
    }

    @Override
    public void setStatsType(String statsType) {
        this.statsType = statsType;
    }

    @Override
    public String getDate() {
        return date;
    }

    @Override
    public void setDate(String date) {
        this.date = date;
    }

    @Override
    public Long getCount() {
        return count;
    }

    @Override
    public void setCount(Long count) {
        this.count = count;
    }

    @Override
    public List<Long> getCounts() {
        return counts;
    }

    @Override
    public void setCounts(List<Long> counts) {
        this.counts = counts;
    }

    public static Schema<DefaultEdge> getSchema() {
        return SCHEMA;
    }

    @Override
    public String getLoadDate() {
        return loadDate;
    }

    @Override
    public void setLoadDate(String loadDate) {
        this.loadDate = loadDate;
    }

    public String getActivityDate() {
        return this.activityDate;
    }

    @Override
    public void setActivityDate(String activityDate) {
        this.activityDate = activityDate;
    }

    @Override
    public void setMarkings(Map<String,String> markings) {
        this.markings = markings;
        this.setColumnVisibility(markings.get(COLUMN_VISIBILITY));
    }

    public Map<String,String> getMarkings() {
        return this.markings;
    }

    @Override
    public String getEdgeAttribute2() {
        return edgeAttribute2;
    }

    @Override
    public void setEdgeAttribute2(String edgeAttribute2) {
        this.edgeAttribute2 = edgeAttribute2;
    }

    @Override
    public String getEdgeAttribute3() {
        return edgeAttribute3;
    }

    @Override
    public void setEdgeAttribute3(String edgeAttribute3) {
        this.edgeAttribute3 = edgeAttribute3;
    }

    @Override
    public Schema<DefaultEdge> cachedSchema() {
        return SCHEMA;
    }

    @XmlTransient
    private static final Schema<DefaultEdge> SCHEMA = new Schema<DefaultEdge>() {

        @Override
        public DefaultEdge newMessage() {
            return new DefaultEdge();
        }

        @Override
        public Class<? super DefaultEdge> typeClass() {
            return DefaultEdge.class;
        }

        @Override
        public String messageName() {
            return DefaultEdge.class.getSimpleName();
        }

        @Override
        public String messageFullName() {
            return DefaultEdge.class.getName();
        }

        @Override
        public boolean isInitialized(DefaultEdge message) {
            return true;
        }

        @Override
        public void writeTo(Output output, DefaultEdge message) throws IOException {
            if (message.columnVisibility != null)
                output.writeString(1, message.columnVisibility, false);
            if (message.source != null)
                output.writeString(2, message.source, false);
            if (message.sink != null)
                output.writeString(3, message.sink, false);
            if (message.edgeType != null)
                output.writeString(4, message.edgeType, false);
            if (message.edgeRelationship != null)
                output.writeString(5, message.edgeRelationship, false);
            if (message.edgeAttribute1Source != null)
                output.writeString(6, message.edgeAttribute1Source, false);
            if (message.statsType != null)
                output.writeString(7, message.statsType, false);
            if (message.date != null)
                output.writeString(8, message.date, false);
            if (message.count != null)
                output.writeUInt64(9, message.count, false);
            if (message.counts != null) {
                for (long count : message.counts) {
                    output.writeUInt64(10, count, true);
                }
            }
            if (message.edgeAttribute3 != null)
                output.writeString(11, message.edgeAttribute3, false);
            if (message.edgeAttribute2 != null)
                output.writeString(12, message.edgeAttribute2, false);
            if (message.loadDate != null) {
                output.writeString(13, message.loadDate, false);
            }
            if (message.activityDate != null) {
                output.writeString(14, message.activityDate, false);
            }
        }

        @Override
        public void mergeFrom(Input input, DefaultEdge message) throws IOException {
            ArrayList<Long> counts = new ArrayList<Long>();
            int number;
            while ((number = input.readFieldNumber(this)) != 0) {
                switch (number) {
                    case 1:
                        message.columnVisibility = input.readString();
                        break;
                    case 2:
                        message.source = input.readString();
                        break;
                    case 3:
                        message.sink = input.readString();
                        break;
                    case 4:
                        message.edgeType = input.readString();
                        break;
                    case 5:
                        message.edgeRelationship = input.readString();
                        break;
                    case 6:
                        message.edgeAttribute1Source = input.readString();
                        break;
                    case 7:
                        message.statsType = input.readString();
                        break;
                    case 8:
                        message.date = input.readString();
                        break;
                    case 9:
                        message.count = input.readUInt64();
                        break;
                    case 10:
                        counts.add(input.readUInt64());
                        break;
                    case 11:
                        message.edgeAttribute3 = input.readString();
                        break;
                    case 12:
                        message.edgeAttribute2 = input.readString();
                        break;
                    case 13:
                        message.loadDate = input.readString();
                        break;
                    case 14:
                        message.activityDate = input.readString();
                        break;
                    default:
                        input.handleUnknownField(number, this);
                        break;
                }
            }
            if (!counts.isEmpty()) {
                message.counts = new ArrayList<Long>(counts.size());
                for (int i = 0; i < message.counts.size(); ++i) {
                    message.counts.set(i, counts.get(i));
                }
            }
        }

        @Override
        public String getFieldName(int number) {
            switch (number) {
                case 1:
                    return COLUMN_VISIBILITY;
                case 2:
                    return "source";
                case 3:
                    return "sink";
                case 4:
                    return "edgeType";
                case 5:
                    return "edgeRelationship";
                case 6:
                    return "edgeAttribute1Source";
                case 7:
                    return "statsType";
                case 8:
                    return "date";
                case 9:
                    return "count";
                case 10:
                    return "counts";
                case 11:
                    return "edgeAttribute3";
                case 12:
                    return "edgeAttribute2";
                case 13:
                    return "loadDate";
                case 14:
                    return "activityDate";
                default:
                    return null;
            }
        }

        @Override
        public int getFieldNumber(String name) {
            final Integer number = fieldMap.get(name);
            return number == null ? 0 : number.intValue();
        }

        private final HashMap<String,Integer> fieldMap = new HashMap<String,Integer>();
        {
            fieldMap.put(COLUMN_VISIBILITY, 1);
            fieldMap.put("source", 2);
            fieldMap.put("sink", 3);
            fieldMap.put("edgeType", 4);
            fieldMap.put("edgeRelationship", 5);
            fieldMap.put("edgeAttribute1Source", 6);
            fieldMap.put("statsType", 7);
            fieldMap.put("date", 8);
            fieldMap.put("count", 9);
            fieldMap.put("counts", 10);
            fieldMap.put("edgeAttribute3", 11);
            fieldMap.put("edgeAttribute2", 12);
            fieldMap.put("loadDate", 13);
            fieldMap.put("activityDate", 14);
        }
    };
}
