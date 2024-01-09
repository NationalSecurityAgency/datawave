package datawave.microservice.configcheck;

public class Output {
    private String message;
    private boolean error;
    
    public String getMessage() {
        return message;
    }
    
    public void setMessage(String message) {
        this.message = message;
        this.error = false;
    }
    
    public void setErrorMessage(String message) {
        this.message = message;
        this.error = true;
    }
    
    public boolean isError() {
        return error;
    }
}
