package datawave.configuration;

import java.io.InputStream;
import java.util.Set;

import jakarta.enterprise.context.spi.CreationalContext;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.InjectionException;
import jakarta.enterprise.inject.spi.AnnotatedType;
import jakarta.enterprise.inject.spi.Extension;
import jakarta.enterprise.inject.spi.InjectionPoint;
import jakarta.enterprise.inject.spi.InjectionTarget;
import jakarta.enterprise.inject.spi.ProcessInjectionTarget;
import jakarta.xml.bind.JAXBContext;
import javax.xml.parsers.SAXParserFactory;

/**
 * A CDI portable extension to create injection targets via JAX-B deserialization. The injection point must implement {@link Configuration} and we expect to
 * find a resource on the class path matching the class' name and path. The xml resource is a jax-b serialized version of the configuration object.
 */
public class ConfigExtension implements Extension {
    @SuppressWarnings({"unused", "unchecked"})
    <T> void processInjectionTarget(@Observes ProcessInjectionTarget<T> pit) {
        final InjectionTarget<T> it = pit.getInjectionTarget();

        final AnnotatedType<T> at = pit.getAnnotatedType();
        String xmlName = at.getJavaClass().getSimpleName() + ".xml";
        InputStream xmlStream = at.getJavaClass().getResourceAsStream(xmlName);

        if (Configuration.class.isAssignableFrom(at.getJavaClass())) {
            if (xmlStream == null) {
                throw new InjectionException("No configuration XML found for " + xmlName);
            }
        } else {
            // Don't do anything if the injection target doesn't implement Configuration
            return;
        }

        try {
            SAXParserFactory factory = SAXParserFactory.newInstance();
            factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
            factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
            factory.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
            JAXBContext context = JAXBContext.newInstance(at.getJavaClass());
            final T instance = (T) context.createUnmarshaller().unmarshal(xmlStream);
            InjectionTarget<T> wrapped = new InjectionTarget<T>() {
                @Override
                public T produce(CreationalContext<T> ctx) {
                    return instance;
                }

                @Override
                public void inject(T instance, CreationalContext<T> ctx) {
                    it.inject(instance, ctx);
                }

                @Override
                public void postConstruct(T instance) {
                    it.postConstruct(instance);
                }

                @Override
                public void preDestroy(T instance) {
                    it.preDestroy(instance);
                }

                @Override
                public void dispose(T instance) {
                    it.dispose(instance);
                }

                @Override
                public Set<InjectionPoint> getInjectionPoints() {
                    return it.getInjectionPoints();
                }
            };
            pit.setInjectionTarget(wrapped);
        } catch (Exception e) {
            pit.addDefinitionError(e);
        }
    }
}
