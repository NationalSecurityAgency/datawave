package datawave.core.mapreduce.bulkresults.map;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.support.ResourcePropertySource;

public class ApplicationContextAwareMapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT> extends Mapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT> {

    private static Logger log = LoggerFactory.getLogger(ApplicationContextAwareMapper.class);

    public static final String SPRING_CONFIG_LOCATIONS = "spring.config.locations";
    public static final String SPRING_CONFIG_BASE_PACKAGES = "spring.config.base-packages";
    public static final String SPRING_CONFIG_STARTING_CLASS = "spring.config.starting-class";

    protected ApplicationContext applicationContext;

    /**
     * Create a Spring Application Context
     *
     * @param contextPath
     *            is a possibly CSV of spring config file locations
     * @param basePackages
     *            is a possibly CSV of base packages to scan
     * @param startingClass
     *            the annotated starting class to be processes
     */
    protected void setApplicationContext(String contextPath, String basePackages, String startingClass) {
        AnnotationConfigApplicationContext annotationApplicationContext = new AnnotationConfigApplicationContext();

        try {
            annotationApplicationContext.getEnvironment().getPropertySources()
                            .addLast(new ResourcePropertySource(new ClassPathResource("application.properties")));
        } catch (IOException e) {
            log.error("application.properties could not be loaded", e);
            throw new RuntimeException(e);
        }

        if (basePackages != null && !basePackages.isEmpty()) {
            annotationApplicationContext.scan(basePackages.split(","));
        }

        if (startingClass != null && !startingClass.isEmpty()) {
            try {
                annotationApplicationContext.register(Class.forName(startingClass));
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("Could not find starting class: " + startingClass, e);
            }
        }

        annotationApplicationContext.refresh();

        if (contextPath != null && !contextPath.isEmpty()) {
            this.applicationContext = new ClassPathXmlApplicationContext(contextPath.split(","), annotationApplicationContext);
        } else {
            this.applicationContext = annotationApplicationContext;
        }
    }
}
