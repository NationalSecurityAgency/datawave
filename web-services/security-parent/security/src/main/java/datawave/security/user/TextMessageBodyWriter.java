package datawave.security.user;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;

import datawave.security.DnList;
import datawave.security.authorization.DatawavePrincipal;
import datawave.user.AuthorizationsListBase;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.ext.MessageBodyWriter;
import jakarta.ws.rs.ext.Provider;

/**
 * A simple JAXB output provider that calls {@link Object#toString()} on the message object to serialize it for output.
 */
@Provider
@Produces("text/plain")
public class TextMessageBodyWriter implements MessageBodyWriter<Object> {

    public long getSize(Object ua, Class<?> c, Type type, Annotation[] annotations, MediaType media) {
        // -1 means size unknown according to the javadoc.
        return -1;
    }

    public boolean isWriteable(Class<?> c, Type type, Annotation[] annotations, MediaType media) {
        return AuthorizationsListBase.class.isAssignableFrom(c) || DatawavePrincipal.class.isAssignableFrom(c) || DnList.class.isAssignableFrom(c);
    }

    public void writeTo(Object ua, Class<?> c, Type type, Annotation[] annotations, MediaType media, MultivaluedMap<String,Object> args, OutputStream out)
                    throws IOException, WebApplicationException {
        out.write(ua.toString().getBytes());
    }

}
