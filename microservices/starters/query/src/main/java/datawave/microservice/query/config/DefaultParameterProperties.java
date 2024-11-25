package datawave.microservice.query.config;

import javax.annotation.Nonnegative;
import javax.validation.constraints.NotEmpty;

import org.springframework.validation.annotation.Validated;

@Validated
public class DefaultParameterProperties {
    @NotEmpty
    private String pool = "unassigned";
    @Nonnegative
    private int maxConcurrentTasks = 8;
    
    public String getPool() {
        return pool;
    }
    
    public void setPool(String pool) {
        this.pool = pool;
    }
    
    public int getMaxConcurrentTasks() {
        return maxConcurrentTasks;
    }
    
    public void setMaxConcurrentTasks(int maxConcurrentTasks) {
        this.maxConcurrentTasks = maxConcurrentTasks;
    }
    
}
