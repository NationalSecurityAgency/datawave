package datawave.core.mapreduce.bulkresults.map;

import org.junit.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.support.ResourcePropertySource;

import java.io.IOException;

public class ApplicationContextAwareMapperTest {

    @Test
    public void someTest() {
        AnnotationConfigApplicationContext annotationApplicationContext = new AnnotationConfigApplicationContext();

        try {
            annotationApplicationContext.getEnvironment().getPropertySources()
                            .addLast(new ResourcePropertySource(new ClassPathResource("application.properties")));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        String[] basePackages = annotationApplicationContext.getEnvironment().getProperty("datawave.query.mapreduce.basePackages", String[].class,
                        new String[] {"datawave.microservice"});

        System.out.println("done!");
    }
}
