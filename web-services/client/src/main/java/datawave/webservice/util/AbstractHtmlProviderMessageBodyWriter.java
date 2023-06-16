package datawave.webservice.util;

import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyWriter;
import javax.ws.rs.ext.Provider;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.nio.charset.Charset;

@Provider
@Produces(MediaType.TEXT_HTML)
public abstract class AbstractHtmlProviderMessageBodyWriter<T> implements MessageBodyWriter<T> {

    public static final Charset utf8 = Charset.forName("UTF-8");

    /*
     * (non-Javadoc)
     *
     * @see javax.ws.rs.ext.MessageBodyWriter#getSize(java.lang.Object, java.lang.Class, java.lang.reflect.Type, java.lang.annotation.Annotation[],
     * javax.ws.rs.core.MediaType)
     */
    @Override
    public long getSize(T t, Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        // Length cannot be determined in advance
        return -1;
    }

    /*
     * (non-Javadoc)
     *
     * @see javax.ws.rs.ext.MessageBodyWriter#writeTo(java.lang.Object, java.lang.Class, java.lang.reflect.Type, java.lang.annotation.Annotation[],
     * javax.ws.rs.core.MediaType, javax.ws.rs.core.MultivaluedMap, java.io.OutputStream)
     */
    @Override
    public void writeTo(T t, Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType, MultivaluedMap<String,Object> httpHeaders,
                    OutputStream entityStream) throws IOException, WebApplicationException {

        byte[] data = createHtml(t);

        entityStream.write(data, 0, data.length);
    }

    public abstract String getTitle(T t);

    public abstract String getHeadContent(T t);

    public abstract String getPageHeader(T t);

    public abstract String getMainContent(T t);

    protected byte[] createHtml(T t) {
        StringBuilder builder = new StringBuilder();
        builder.append("<html>");
        builder.append("<head>");
        builder.append("<meta http-equiv=\"content-type\" content=\"text/html; charset=UTF-8\"/>");
        builder.append("<title>");
        builder.append("DATAWAVE - ");
        builder.append(getTitle(t));
        builder.append("</title>");
        builder.append("<link rel='stylesheet' type='text/css' href='/screen.css' media='screen' />");
        builder.append(getHeadContent(t));
        builder.append("</head>");

        builder.append("<body>");
        builder.append("<h1>");
        builder.append(getPageHeader(t));
        builder.append("</h1>");

        builder.append("<div>");
        builder.append(getMainContent(t));
        builder.append("</div>");

        builder.append("<br/>");

        builder.append("</body></html>\n");
        return builder.toString().getBytes(utf8);
    }
}
