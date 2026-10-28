package datawave.configuration.spring;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.parsing.BeanComponentDefinition;
import org.springframework.beans.factory.parsing.ComponentDefinition;
import org.springframework.beans.factory.parsing.EmptyReaderEventListener;
import org.springframework.beans.factory.support.GenericBeanDefinition;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * Used to record the XML files that are actually used to create the ClassPathXmlApplicationContext
 */
public class TrackingClasspathXmlApplicationContext extends ClassPathXmlApplicationContext {

    private final TrackingImportReaderEventListener trackingImportReaderEventListener = new TrackingImportReaderEventListener();

    public TrackingClasspathXmlApplicationContext(String[] configLocations, boolean refresh) {
        super(configLocations, refresh);
    }

    @Override
    protected void initBeanDefinitionReader(XmlBeanDefinitionReader reader) {
        super.initBeanDefinitionReader(reader);
        reader.setEventListener(trackingImportReaderEventListener);
    }

    @Override
    protected void loadBeanDefinitions(XmlBeanDefinitionReader reader) throws BeansException, IOException {
        super.loadBeanDefinitions(reader);

    }

    public TrackingImportReaderEventListener getTrackingImportReaderEventListener() {
        return trackingImportReaderEventListener;
    }

    public class TrackingImportReaderEventListener extends EmptyReaderEventListener {

        private final Set<String> imports = new HashSet<>();

        @Override
        public void componentRegistered(ComponentDefinition componentDefinition) {
            if (componentDefinition instanceof BeanComponentDefinition) {
                try {
                    BeanComponentDefinition bcd = (BeanComponentDefinition) componentDefinition;
                    imports.add(((GenericBeanDefinition) bcd.getBeanDefinition()).getResource().getURL().toString());
                } catch (Exception e) {

                }
            }
        }

        public Set<String> getImports() {
            return imports;
        }
    }
}
