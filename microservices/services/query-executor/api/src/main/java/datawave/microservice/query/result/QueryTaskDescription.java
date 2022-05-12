package datawave.microservice.query.result;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
@XmlAccessorType(XmlAccessType.NONE)
public class QueryTaskDescription {
    @XmlElement
    private String taskId;
    @XmlElement
    private String method;
    @XmlElement
    private String queryLogic;
    @XmlElement
    private String state;
    
    public QueryTaskDescription() {
        
    }
    
    public QueryTaskDescription(String taskId, String method, String queryLogic, String state) {
        setTaskId(taskId);
        setMethod(method);
        setQueryLogic(queryLogic);
        setState(state);
    }
    
    public String getTaskId() {
        return taskId;
    }
    
    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }
    
    public String getMethod() {
        return method;
    }
    
    public void setMethod(String method) {
        this.method = method;
    }
    
    public String getState() {
        return state;
    }
    
    public void setState(String state) {
        this.state = state;
    }
    
    public String getQueryLogic() {
        return queryLogic;
    }
    
    public void setQueryLogic(String queryLogic) {
        this.queryLogic = queryLogic;
    }
}
