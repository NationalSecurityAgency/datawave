package datawave.webservice.results.mr;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

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

@XmlAccessorType(XmlAccessType.NONE)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class MapReduceInfoResponse implements Serializable, Message<MapReduceInfoResponse> {

    private static final long serialVersionUID = 1L;

    @XmlAttribute(name = "id")
    private String id;

    @XmlTransient
    private String hdfs;

    @XmlTransient
    private String jobTracker;

    @XmlAttribute
    private String jobName;

    @XmlAttribute(name = "resultsDirectory")
    private String resultsDirectory;

    @XmlElement(name = "RuntimeParameters")
    private String runtimeParameters;

    @XmlAttribute(name = "workingDirectory")
    private String workingDirectory;

    @XmlElementWrapper(name = "JobExecutionHistory")
    @XmlElement(name = "JobExecution")
    private List<JobExecution> jobExecutions;

    @XmlElementWrapper(name = "ResultFiles")
    @XmlElement(name = "ResultFile")
    private List<ResultFile> resultFiles;

    public String getId() {
        return id;
    }

    public String getHdfs() {
        return hdfs;
    }

    public String getJobTracker() {
        return jobTracker;
    }

    public String getResultsDirectory() {
        return resultsDirectory;
    }

    public String getRuntimeParameters() {
        return runtimeParameters;
    }

    public String getWorkingDirectory() {
        return workingDirectory;
    }

    public List<JobExecution> getJobExecutions() {
        return jobExecutions;
    }

    public List<ResultFile> getResultFiles() {
        return resultFiles;
    }

    public String getJobName() {
        return this.jobName;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setHdfs(String hdfs) {
        this.hdfs = hdfs;
    }

    public void setJobTracker(String jobTracker) {
        this.jobTracker = jobTracker;
    }

    public void setResultsDirectory(String resultsDirectory) {
        this.resultsDirectory = resultsDirectory;
    }

    public void setRuntimeParameters(String runtimeParameters) {
        this.runtimeParameters = runtimeParameters;
    }

    public void setWorkingDirectory(String workingDirectory) {
        this.workingDirectory = workingDirectory;
    }

    public void setJobExecutions(List<JobExecution> jobExecutions) {
        this.jobExecutions = jobExecutions;
    }

    public void setResultFiles(List<ResultFile> resultFiles) {
        this.resultFiles = resultFiles;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    @Override
    public Schema<MapReduceInfoResponse> cachedSchema() {
        return SCHEMA;
    }

    public static Schema<MapReduceInfoResponse> getSchema() {
        return SCHEMA;
    }

    @XmlTransient
    private static final Schema<MapReduceInfoResponse> SCHEMA = new Schema<MapReduceInfoResponse>() {
        // schema methods

        public MapReduceInfoResponse newMessage() {
            return new MapReduceInfoResponse();
        }

        public Class<MapReduceInfoResponse> typeClass() {
            return MapReduceInfoResponse.class;
        }

        public String messageName() {
            return MapReduceInfoResponse.class.getSimpleName();
        }

        public String messageFullName() {
            return MapReduceInfoResponse.class.getName();
        }

        public boolean isInitialized(MapReduceInfoResponse message) {
            return true;
        }

        public void writeTo(Output output, MapReduceInfoResponse message) throws IOException {

            if (message.getId() != null) {
                output.writeString(1, message.getId(), false);
            }

            if (message.getResultsDirectory() != null) {
                output.writeString(2, message.getResultsDirectory(), false);
            }

            if (message.getRuntimeParameters() != null) {
                output.writeString(3, message.getRuntimeParameters(), false);
            }

            if (message.getWorkingDirectory() != null) {
                output.writeString(4, message.getWorkingDirectory(), false);
            }

            if (null != message.getJobExecutions()) {
                for (JobExecution job : message.getJobExecutions()) {
                    if (null != job)
                        output.writeObject(5, job, JobExecution.getSchema(), true);
                }
            }

            if (null != message.getResultFiles()) {
                for (ResultFile file : message.getResultFiles()) {
                    if (null != file)
                        output.writeObject(6, file, ResultFile.getSchema(), true);
                }
            }

            if (null != message.getJobName()) {
                output.writeString(7, message.getJobName(), false);
            }

        }

        public void mergeFrom(Input input, MapReduceInfoResponse message) throws IOException {
            List<ResultFile> files = null;
            List<JobExecution> jobs = null;
            int number;
            while ((number = input.readFieldNumber(this)) != 0) {
                switch (number) {
                    case 1:
                        message.setId(input.readString());
                        break;
                    case 2:
                        message.setResultsDirectory(input.readString());
                        break;
                    case 3:
                        message.setRuntimeParameters(input.readString());
                        break;
                    case 4:
                        message.setWorkingDirectory(input.readString());
                        break;
                    case 5:
                        if (jobs == null)
                            jobs = new ArrayList<JobExecution>();
                        jobs.add(input.mergeObject(null, JobExecution.getSchema()));
                        break;
                    case 6:
                        if (files == null)
                            files = new ArrayList<ResultFile>();
                        files.add(input.mergeObject(null, ResultFile.getSchema()));
                        break;
                    case 7:
                        message.setJobName(input.readString());
                        break;
                    default:
                        input.handleUnknownField(number, this);
                        break;
                }
            }
            if (jobs != null)
                message.setJobExecutions(jobs);
            if (files != null)
                message.setResultFiles(files);
        }

        public String getFieldName(int number) {
            switch (number) {
                case 1:
                    return "id";
                case 2:
                    return "resultsDirectory";
                case 3:
                    return "runtimeParameters";
                case 4:
                    return "workingDirectory";
                case 5:
                    return "jobExecutions";
                case 6:
                    return "resultFiles";
                case 7:
                    return "jobName";
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
            fieldMap.put("id", 1);
            fieldMap.put("resultsDirectory", 2);
            fieldMap.put("runtimeParameters", 3);
            fieldMap.put("workingDirectory", 4);
            fieldMap.put("jobExecutions", 5);
            fieldMap.put("resultFiles", 6);
            fieldMap.put("jobName", 7);

        }
    };

}
