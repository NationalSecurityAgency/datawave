package datawave.microservice.query.config;

import org.springframework.validation.annotation.Validated;

import javax.annotation.Nonnegative;
import javax.validation.constraints.NotEmpty;

@Validated
public class DefaultParameterProperties {
    @NotEmpty
    private String pool = "unassigned";
    @Nonnegative
    private int maxConcurrentTasks = 10;
    
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
