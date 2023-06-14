package datawave.webservice.atom.jaxrs;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;

import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyWriter;
import javax.ws.rs.ext.Provider;

import org.apache.abdera.model.Categories;
import org.apache.abdera.model.Entry;
import org.apache.abdera.model.Feed;
import org.apache.abdera.model.Service;

@Provider
@Produces({"application/atomsvc+xml", "application/atomcat+xml", "application/atom+xml", "application/atom+xml;type=entry"})
public class AtomMessageBodyWriter implements MessageBodyWriter<Object> {

    @Override
    public long getSize(Object message, Class<?> clazz, Type type, Annotation[] annotations, MediaType media) {
        // -1 means size unknown
        return -1;
    }

    @Override
    public boolean isWriteable(Class<?> clazz, Type type, Annotation[] annotations, MediaType mediaType) {
        return (Entry.class.isAssignableFrom(clazz) || Feed.class.isAssignableFrom(clazz) || Service.class.isAssignableFrom(clazz)
                        || Categories.class.isAssignableFrom(clazz));
    }

    @Override
    public void writeTo(Object message, Class<?> clazz, Type type, Annotation[] annotations, MediaType media, MultivaluedMap<String,Object> httpHeaders,
                    OutputStream out) throws IOException, WebApplicationException {
        if (Entry.class.isAssignableFrom(clazz)) {
            Entry entry = (Entry) message;
            entry.writeTo(out);
        } else if (Feed.class.isAssignableFrom(clazz)) {
            Feed feed = (Feed) message;
            feed.writeTo(out);
        } else if (Service.class.isAssignableFrom(clazz)) {
            Service service = (Service) message;
            service.writeTo(out);
        } else if (Categories.class.isAssignableFrom(clazz)) {
            Categories categories = (Categories) message;
            categories.writeTo(out);
        }
    }

}
