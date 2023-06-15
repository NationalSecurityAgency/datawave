package datawave.webservice.result.bulk;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlTransient;

import io.protostuff.Input;
import io.protostuff.Message;
import io.protostuff.Output;
import io.protostuff.Schema;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

@XmlAccessorType(XmlAccessType.NONE)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class BulkResultsInfoResponse implements Serializable, Message<BulkResultsInfoResponse> {

    private static final long serialVersionUID = 1L;

    @XmlAccessorType(XmlAccessType.NONE)
    @XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
    public static class Job implements Serializable, Message<Job> {

        private static final long serialVersionUID = 1L;

        @XmlElement(name = "jobId")
        private String jobId = null;
        @XmlElementWrapper(name = "JobHistories")
        @XmlElement(name = "JobHistory")
        private TreeSet<History> history = null;

        public String getJobId() {
            return jobId;
        }

        public Set<History> getHistory() {
            return history;
        }

        public void setJobId(String jobId) {
            this.jobId = jobId;
        }

        public void setHistory(Set<History> history) {
            this.history = new TreeSet<History>(history);
        }

        public void addHistory(History history) {
            if (null == this.history)
                this.history = new TreeSet<History>();
            this.history.add(history);
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder(17, 37).append(jobId).append(history).toHashCode();
        }

        @Override
        public boolean equals(Object o) {
            if (null == o)
                return false;
            if (!(o instanceof Job))
                return false;
            Job j = (Job) o;
            return this.jobId.equals(j.jobId);
        }

        @Override
        public String toString() {
            ToStringBuilder tsb = new ToStringBuilder(this);
            tsb.append("jobId", this.jobId);
            tsb.append("history", this.history);
            return tsb.toString();
        }

        public Schema<Job> cachedSchema() {
            return SCHEMA;
        }

        public static Schema<Job> getSchema() {
            return SCHEMA;
        }

        @XmlTransient
        private static final Schema<Job> SCHEMA = new Schema<Job>() {
            public Job newMessage() {
                return new Job();
            }

            public Class<Job> typeClass() {
                return Job.class;
            }

            public String messageName() {
                return Job.class.getSimpleName();
            }

            public String messageFullName() {
                return Job.class.getName();
            }

            public boolean isInitialized(Job message) {
                return true;
            }

            public void writeTo(Output output, Job message) throws IOException {
                if (message.getJobId() != null) {
                    output.writeString(1, message.getJobId(), false);
                }

                if (message.history != null) {
                    for (History h : message.history) {
                        if (h != null)
                            output.writeObject(2, h, History.getSchema(), true);
                    }
                }
            }

            public void mergeFrom(Input input, Job message) throws IOException {
                int number;
                while ((number = input.readFieldNumber(this)) != 0) {
                    switch (number) {
                        case 1:
                            message.setJobId(input.readString());
                            break;
                        case 2:
                            if (message.history == null)
                                message.history = new TreeSet<History>();
                            message.history.add(input.mergeObject(null, History.getSchema()));
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
                        return "jobId";
                    case 2:
                        return "history";
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
                fieldMap.put("jobId", 1);
                fieldMap.put("history", 2);
            }
        };

    }

    @XmlAccessorType(XmlAccessType.NONE)
    @XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
    public static class History implements Serializable, Comparator<History>, Comparable<History>, Message<History> {

        private static final long serialVersionUID = 1L;

        @XmlAttribute(name = "timestamp")
        private Long timestamp = 0L;
        @XmlAttribute(name = "state")
        private String state = null;

        public Long getTimestamp() {
            return timestamp;
        }

        public String getState() {
            return state;
        }

        public void setTimestamp(Long timestamp) {
            this.timestamp = timestamp;
        }

        public void setState(String state) {
            this.state = state;
        }

        @Override
        public int compareTo(History o) {
            int t = (this.timestamp.compareTo(o.timestamp));
            if (0 == t) {
                return (this.state.compareTo(o.state));
            } else {
                return t;
            }
        }

        @Override
        public int compare(History o1, History o2) {
            return o1.compareTo(o2);
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder(17, 37).append(timestamp).append(state).toHashCode();
        }

        @Override
        public boolean equals(Object o) {
            if (null == o)
                return false;
            if (!(o instanceof History))
                return false;
            return this.compareTo((History) o) == 0;
        }

        @Override
        public String toString() {
            ToStringBuilder tsb = new ToStringBuilder(this);
            tsb.append("timestamp", this.timestamp);
            tsb.append("state", this.state);
            return tsb.toString();
        }

        public Schema<History> cachedSchema() {
            return SCHEMA;
        }

        public static Schema<History> getSchema() {
            return SCHEMA;
        }

        @XmlTransient
        private static final Schema<History> SCHEMA = new Schema<History>() {
            public History newMessage() {
                return new History();
            }

            public Class<History> typeClass() {
                return History.class;
            }

            public String messageName() {
                return History.class.getSimpleName();
            }

            public String messageFullName() {
                return History.class.getName();
            }

            public boolean isInitialized(History message) {
                return true;
            }

            public void writeTo(Output output, History message) throws IOException {
                if (message.getState() != null) {
                    output.writeString(1, message.getState(), false);
                }

                if (message.getTimestamp() != null) {
                    output.writeUInt64(2, message.getTimestamp(), false);
                }
            }

            public void mergeFrom(Input input, History message) throws IOException {
                int number;
                while ((number = input.readFieldNumber(this)) != 0) {
                    switch (number) {
                        case 1:
                            message.setState(input.readString());
                            break;
                        case 2:
                            message.setTimestamp(input.readUInt64());
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
                        return "state";
                    case 2:
                        return "timestamp";
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
                fieldMap.put("state", 1);
                fieldMap.put("timestamp", 2);
            }
        };

    }

    @XmlAttribute(name = "user")
    private String user = null;
    @XmlAttribute(name = "bulkResultsId")
    private String bulkResultsId = null;
    @XmlAttribute(name = "jobDirectory")
    private String jobDirectory = null;
    @XmlAttribute(name = "outputDestination")
    private String outputDestination = null;
    @XmlElement(name = "Configuration")
    private String configuration = null;
    @XmlAttribute(name = "queryId")
    private String queryId = null;
    @XmlAttribute(name = "serialization")
    private String serializationFormat = null;
    @XmlElementWrapper(name = "Jobs")
    @XmlElement(name = "Job")
    private List<Job> jobs = null;

    public String getUser() {
        return user;
    }

    public String getBulkResultsId() {
        return bulkResultsId;
    }

    public String getJobDirectory() {
        return jobDirectory;
    }

    public String getOutputDestination() {
        return outputDestination;
    }

    public String getConfiguration() {
        return configuration;
    }

    public String getQueryId() {
        return queryId;
    }

    public List<Job> getJobs() {
        return jobs;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public void setBulkResultsId(String bulkResultsId) {
        this.bulkResultsId = bulkResultsId;
    }

    public void setJobDirectory(String jobDirectory) {
        this.jobDirectory = jobDirectory;
    }

    public void setOutputDestination(String outputDestination) {
        this.outputDestination = outputDestination;
    }

    public void setConfiguration(String configuration) {
        this.configuration = configuration;
    }

    public void setQueryId(String queryId) {
        this.queryId = queryId;
    }

    public void setJobs(List<Job> job) {
        this.jobs = job;
    }

    public void addJob(Job history) {
        if (null == this.jobs)
            this.jobs = new ArrayList<Job>();
        this.jobs.add(history);
    }

    public String getSerializationFormat() {
        return serializationFormat;
    }

    public void setSerializationFormat(String serializationFormat) {
        this.serializationFormat = serializationFormat;
    }

    @Override
    public String toString() {
        ToStringBuilder tsb = new ToStringBuilder(this);
        tsb.append("user", this.user);
        tsb.append("bulkResultsId", this.bulkResultsId);
        tsb.append("jobDirectory", this.jobDirectory);
        tsb.append("outputDestination", this.outputDestination);
        tsb.append("queryId", this.queryId);
        tsb.append("serialization", this.serializationFormat);
        tsb.append("conf", this.configuration);
        tsb.append("job", this.jobs);
        return tsb.toString();
    }

    @Override
    public Schema<BulkResultsInfoResponse> cachedSchema() {
        return SCHEMA;
    }

    public static Schema<BulkResultsInfoResponse> getSchema() {
        return SCHEMA;
    }

    @XmlTransient
    private static final Schema<BulkResultsInfoResponse> SCHEMA = new Schema<BulkResultsInfoResponse>() {
        // schema methods

        public BulkResultsInfoResponse newMessage() {
            return new BulkResultsInfoResponse();
        }

        public Class<BulkResultsInfoResponse> typeClass() {
            return BulkResultsInfoResponse.class;
        }

        public String messageName() {
            return BulkResultsInfoResponse.class.getSimpleName();
        }

        public String messageFullName() {
            return BulkResultsInfoResponse.class.getName();
        }

        public boolean isInitialized(BulkResultsInfoResponse message) {
            return true;
        }

        public void writeTo(Output output, BulkResultsInfoResponse message) throws IOException {

            if (message.user != null) {
                output.writeString(1, message.user, false);
            }

            if (message.bulkResultsId != null) {
                output.writeString(2, message.bulkResultsId, false);
            }

            if (message.jobDirectory != null) {
                output.writeString(3, message.jobDirectory, false);
            }

            if (message.outputDestination != null) {
                output.writeString(4, message.outputDestination, false);
            }

            if (message.configuration != null) {
                output.writeString(5, message.configuration, false);
            }

            if (message.queryId != null) {
                output.writeString(6, message.queryId, false);
            }

            if (message.serializationFormat != null) {
                output.writeString(7, message.serializationFormat, false);
            }

            if (message.jobs != null) {
                for (Job job : message.jobs) {
                    if (job != null)
                        output.writeObject(6, job, Job.getSchema(), true);
                }
            }
        }

        public void mergeFrom(Input input, BulkResultsInfoResponse message) throws IOException {
            int number;
            while ((number = input.readFieldNumber(this)) != 0) {
                switch (number) {
                    case 1:
                        message.setUser(input.readString());
                        break;
                    case 2:
                        message.setBulkResultsId(input.readString());
                        break;
                    case 3:
                        message.setJobDirectory(input.readString());
                        break;
                    case 4:
                        message.setOutputDestination(input.readString());
                        break;
                    case 5:
                        message.setConfiguration(input.readString());
                        break;
                    case 6:
                        message.setQueryId(input.readString());
                        break;
                    case 7:
                        message.setSerializationFormat(input.readString());
                        break;
                    case 8:
                        if (message.jobs == null)
                            message.jobs = new ArrayList<Job>();
                        message.jobs.add(input.mergeObject(null, Job.getSchema()));
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
                    return "user";
                case 2:
                    return "bulkResultsId";
                case 3:
                    return "jobDirectory";
                case 4:
                    return "outputDestination";
                case 5:
                    return "configuration";
                case 6:
                    return "queryId";
                case 7:
                    return "serializationFormat";
                case 8:
                    return "jobs";
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
            fieldMap.put("user", 1);
            fieldMap.put("bulkResultsId", 2);
            fieldMap.put("jobDirectory", 3);
            fieldMap.put("outputDestination", 4);
            fieldMap.put("configuration", 5);
            fieldMap.put("queryId", 6);
            fieldMap.put("serializationFormat", 7);
            fieldMap.put("jobs", 8);
        }
    };

}
