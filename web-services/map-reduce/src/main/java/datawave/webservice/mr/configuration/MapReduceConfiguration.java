package datawave.webservice.mr.configuration;

import org.apache.hadoop.mapreduce.InputFormat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MapReduceConfiguration {

    private String callbackServletURL = null;
    private String mapReduceBaseDirectory = null;
    private boolean restrictInputFormats = true;
    private List<Class<? extends InputFormat<?,?>>> validInputFormats = new ArrayList<>();
    private Map<String,MapReduceJobConfiguration> jobConfiguration = null;

    public String getCallbackServletURL() {
        if (null == this.callbackServletURL)
            throw new IllegalArgumentException("Callback Servlet URL must be set");
        return callbackServletURL;
    }

    public String getMapReduceBaseDirectory() {
        if (null == this.mapReduceBaseDirectory)
            throw new IllegalArgumentException("Map Reduce base directory must be set");
        return mapReduceBaseDirectory;
    }

    public Map<String,MapReduceJobConfiguration> getJobConfiguration() {
        return this.jobConfiguration;
    }

    public MapReduceJobConfiguration getConfiguration(String name) {
        MapReduceJobConfiguration conf = jobConfiguration.get(name);
        if (null == conf)
            throw new IllegalArgumentException("No job configuration with name: " + name);

        conf.setCallbackServletURL(this.getCallbackServletURL());
        conf.setMapReduceBaseDirectory(this.getMapReduceBaseDirectory());
        return conf;
    }

    public boolean isRestrictInputFormats() {
        return restrictInputFormats;
    }

    public List<Class<? extends InputFormat<?,?>>> getValidInputFormats() {
        return validInputFormats;
    }

    public void setCallbackServletURL(String callbackServletURL) {
        this.callbackServletURL = callbackServletURL + "?jobId=$jobId&jobStatus=$jobStatus";
    }

    public void setMapReduceBaseDirectory(String mapReduceBaseDirectory) {
        this.mapReduceBaseDirectory = mapReduceBaseDirectory;
    }

    public void setJobConfiguration(Map<String,MapReduceJobConfiguration> jobConfiguration) {
        this.jobConfiguration = jobConfiguration;
    }

    public void setRestrictInputFormats(boolean restrictInputFormats) {
        this.restrictInputFormats = restrictInputFormats;
    }

    public void setValidInputFormats(List<Class<? extends InputFormat<?,?>>> validInputFormats) {
        this.validInputFormats = validInputFormats;
    }

}
