package datawave.webservice.results.mr;

import java.io.IOException;
import java.io.Serializable;

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlTransient;

import org.apache.commons.lang.builder.HashCodeBuilder;

import io.protostuff.Input;
import io.protostuff.Message;
import io.protostuff.Output;
import io.protostuff.Schema;

@XmlAccessorType(XmlAccessType.NONE)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class JobExecution implements Serializable, Message<JobExecution>, Comparable<JobExecution> {

    private static final long serialVersionUID = 1L;

    @XmlAttribute(name = "mapReduceJobId")
    private String mapReduceJobId;

    @XmlAttribute(name = "timestamp")
    private long timestamp;

    @XmlAttribute(name = "state")
    private String state;

    public String getMapReduceJobId() {
        return mapReduceJobId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getState() {
        return state;
    }

    public void setMapReduceJobId(String mapReduceJobId) {
        this.mapReduceJobId = mapReduceJobId;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public void setState(String state) {
        this.state = state;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37).append(this.getTimestamp()).append(this.getMapReduceJobId()).append(this.getState()).toHashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (null == o)
            return false;
        if (o == this)
            return true;
        if (o.getClass() != this.getClass())
            return false;
        JobExecution other = (JobExecution) o;
        if (this.mapReduceJobId.equals(other.mapReduceJobId) && this.state.equals(other.state) && this.timestamp == other.timestamp)
            return true;
        return false;
    }

    @Override
    public int compareTo(JobExecution o) {
        // sort by the map reduce job id, timestamp, and then state
        int result = this.mapReduceJobId.compareTo(o.mapReduceJobId);
        if (result != 0)
            return result;
        else {
            if (this.timestamp < o.timestamp)
                return -1;
            else if (this.timestamp > o.timestamp)
                return 1;
            else {
                return this.state.compareTo(o.state);
            }
        }
    }

    @Override
    public Schema<JobExecution> cachedSchema() {
        return SCHEMA;
    }

    public static Schema<JobExecution> getSchema() {
        return SCHEMA;
    }

    @XmlTransient
    private static final Schema<JobExecution> SCHEMA = new Schema<JobExecution>() {
        // schema methods

        public JobExecution newMessage() {
            return new JobExecution();
        }

        public Class<JobExecution> typeClass() {
            return JobExecution.class;
        }

        public String messageName() {
            return JobExecution.class.getSimpleName();
        }

        public String messageFullName() {
            return JobExecution.class.getName();
        }

        public boolean isInitialized(JobExecution message) {
            return true;
        }

        public void writeTo(Output output, JobExecution message) throws IOException {

            if (message.getMapReduceJobId() != null) {
                output.writeString(1, message.getMapReduceJobId(), false);
            }

            output.writeUInt64(2, message.getTimestamp(), false);

            if (message.getState() != null) {
                output.writeString(3, message.getState(), false);
            }
        }

        public void mergeFrom(Input input, JobExecution message) throws IOException {
            int number;
            while ((number = input.readFieldNumber(this)) != 0) {
                switch (number) {
                    case 1:
                        message.setMapReduceJobId(input.readString());
                        break;
                    case 2:
                        message.setTimestamp(input.readUInt64());
                        break;
                    case 3:
                        message.setState(input.readString());
                        break;
                    default:
                        input.handleUnknownField(number, this);
                        break;
                }
            }
        }

        public String getFieldName(int number) {
            switch (number) {
                case 1:
                    return "mapReduceJobId";
                case 2:
                    return "timestamp";
                case 3:
                    return "state";
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
            fieldMap.put("mapReduceJobId", 1);
            fieldMap.put("timestamp", 2);
            fieldMap.put("state", 3);
        }
    };

}
