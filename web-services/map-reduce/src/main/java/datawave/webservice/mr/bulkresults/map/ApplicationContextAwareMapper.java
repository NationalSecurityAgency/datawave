package datawave.webservice.mr.bulkresults.map;

import org.apache.hadoop.mapreduce.Mapper;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class ApplicationContextAwareMapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT> extends Mapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT> {

    public static final String SPRING_CONFIG_LOCATIONS = "spring.config.locations";

    protected ApplicationContext applicationContext;

    /**
     * Create a Spring Application Context
     *
     * @param contextPath
     *            is a possibly CSV of spring config file locations
     */
    protected void setApplicationContext(String contextPath) {
        this.applicationContext = new ClassPathXmlApplicationContext(contextPath.split(","));
    }

}
