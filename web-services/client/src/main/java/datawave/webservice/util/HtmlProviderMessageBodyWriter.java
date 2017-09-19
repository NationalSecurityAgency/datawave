package datawave.webservice.util;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.nio.charset.Charset;

import datawave.webservice.HtmlProvider;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyWriter;

public class HtmlProviderMessageBodyWriter implements MessageBodyWriter<HtmlProvider> {
    
    public static final Charset utf8 = Charset.forName("UTF-8");
    
    /*
     * (non-Javadoc)
     * 
     * @see javax.ws.rs.ext.MessageBodyWriter#isWriteable(java.lang.Class, java.lang.reflect.Type, java.lang.annotation.Annotation[],
     * javax.ws.rs.core.MediaType)
     */
    @Override
    public boolean isWriteable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return HtmlProvider.class.isAssignableFrom(type);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see javax.ws.rs.ext.MessageBodyWriter#getSize(java.lang.Object, java.lang.Class, java.lang.reflect.Type, java.lang.annotation.Annotation[],
     * javax.ws.rs.core.MediaType)
     */
    @Override
    public long getSize(HtmlProvider t, Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
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
    public void writeTo(HtmlProvider t, Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType,
                    MultivaluedMap<String,Object> httpHeaders, OutputStream entityStream) throws IOException, WebApplicationException {
        
        byte[] data = createHtml(t);
        
        entityStream.write(data, 0, data.length);
    }
    
    public String getHeader() {
        return "";
    }
    
    public String getFooter() {
        return "";
    }
    
    protected byte[] createHtml(HtmlProvider t) {
        StringBuilder builder = new StringBuilder();
        builder.append("<html>");
        builder.append("<head>");
        builder.append("<meta http-equiv=\"content-type\" content=\"text/html; charset=UTF-8\"/>");
        builder.append("<title>");
        builder.append("DATAWAVE - ");
        builder.append(t.getTitle());
        builder.append("</title>");
        builder.append("<link rel='stylesheet' type='text/css' href='/screen.css' media='screen' />");
        builder.append(t.getHeadContent());
        builder.append("</head>");
        
        builder.append("<body>");
        builder.append(getHeader());
        builder.append("<h1>");
        builder.append(t.getPageHeader());
        builder.append("</h1>");
        
        builder.append("<div>");
        builder.append(t.getMainContent());
        builder.append("</div>");
        
        builder.append("<br/>");
        
        builder.append(getFooter());
        
        builder.append("</body></html>\n");
        return builder.toString().getBytes(utf8);
    }
}
