package datawave.webservice.datadictionary;

import datawave.webservice.query.result.metadata.MetadataFieldBase;
import datawave.webservice.results.datadictionary.DataDictionaryBase;
import datawave.webservice.results.datadictionary.DescriptionBase;
import datawave.webservice.results.datadictionary.DictionaryFieldBase;
import datawave.webservice.results.datadictionary.FieldsBase;

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
@Produces({"text/html"})
public class DataDictionaryHtmlProvider implements MessageBodyWriter<Object> {
    
    private static final Charset utf8 = Charset.forName("UTF-8");
    private static final String EMPTY_STR = "", SEP = ", ";
    
    @Override
    public boolean isWriteable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return (DataDictionaryBase.class.isAssignableFrom(type) || FieldsBase.class.isAssignableFrom(type));
    }
    
    @Override
    public long getSize(Object t, Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        // Length cannot be determined in advance
        return -1;
    }
    
    @Override
    public void writeTo(Object t, Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType, MultivaluedMap<String,Object> httpHeaders,
                    OutputStream out) throws IOException, WebApplicationException {
        
        byte[] data = null;
        
        if (t instanceof DataDictionaryBase) {
            data = getDataDictionaryHtml((DataDictionaryBase) t);
        } else if (t instanceof FieldsBase) {
            data = getFieldDescriptionsHtml((FieldsBase) t);
        }
        
        out.write(data, 0, data.length);
    }
    
    private byte[] getDataDictionaryHtml(DataDictionaryBase<?,? extends MetadataFieldBase> dict) {
        StringBuilder builder = new StringBuilder();
        builder.append("<html>");
        builder.append("<title>");
        builder.append("Data Dictionary");
        builder.append("</title>");
        builder.append("<body>");
        builder.append("<link rel='stylesheet' type='text/css' href='/screen.css' media='screen' />");
        builder.append("<div style=\"font-size: 14px;\">Data Dictionary</div>\n");
        builder.append("<table class=\"creds\">\n");
        
        builder.append("<tr><th>FieldName</th><th>Internal FieldName</th><th>DataType</th>");
        builder.append("<th>Index only</th><th>Forward Indexed</th><th>Reverse Indexed</th><th>Normalized</th></tr>");
        builder.append("<th>Types</th><th>Descriptions</th><th>LastUpdated</th></tr>");
        
        int x = 0;
        for (MetadataFieldBase<?,? extends DescriptionBase> f : dict.getFields()) {
            // highlight alternating rows
            if (x % 2 == 0) {
                builder.append("<tr class=\"highlight\">");
            } else {
                builder.append("<tr>");
            }
            x++;
            
            String fieldName = (null == f.getFieldName()) ? EMPTY_STR : f.getFieldName();
            String internalFieldName = (null == f.getInternalFieldName()) ? EMPTY_STR : f.getInternalFieldName();
            String datatype = (null == f.getDataType()) ? EMPTY_STR : f.getDataType();
            
            StringBuilder types = new StringBuilder();
            if (null != f.getTypes()) {
                
                for (String type : f.getTypes()) {
                    if (0 != types.length()) {
                        types.append(SEP);
                    }
                    types.append(type);
                }
            }
            
            builder.append("<td>").append(fieldName).append("</td>");
            builder.append("<td>").append(internalFieldName).append("</td>");
            builder.append("<td>").append(datatype).append("</td>");
            builder.append("<td>").append(f.isIndexOnly()).append("</td>");
            builder.append("<td>").append(f.isForwardIndexed() ? true : "").append("</td>");
            builder.append("<td>").append(f.isReverseIndexed() ? true : "").append("</td>");
            builder.append("<td>").append(f.isNormalized() ? true : "").append("</td>");
            builder.append("<td>").append(types).append("</td>");
            builder.append("<td>");
            
            boolean first = true;
            for (DescriptionBase desc : f.getDescriptions()) {
                if (!first) {
                    builder.append(", ");
                } else {
                    first = false;
                }
                builder.append(desc);
            }
            builder.append("</td>");
            builder.append("<td>").append(f.getLastUpdated()).append("</td>");
            builder.append("</tr>");
        }
        
        builder.append("</table></body></html>\n");
        return builder.toString().getBytes(utf8);
    }
    
    private byte[] getFieldDescriptionsHtml(FieldsBase<?,? extends DictionaryFieldBase,? extends DescriptionBase> descs) {
        StringBuilder builder = new StringBuilder();
        builder.append("<html>");
        builder.append("<title>");
        builder.append("Field Descriptions");
        builder.append("</title>");
        builder.append("<body>");
        builder.append("<link rel='stylesheet' type='text/css' href='/screen.css' media='screen' />");
        builder.append("<div style=\"font-size: 14px;\">Field Descriptions</div>\n");
        builder.append("<table class=\"creds\">\n");
        
        builder.append("<tr><th>Datatype</th><th>FieldName</th><th>Description</th></tr>");
        int x = 0;
        for (DictionaryFieldBase<?,? extends DescriptionBase> field : descs.getFields()) {
            for (DescriptionBase desc : field.getDescriptions()) {
                // highlight alternating rows
                if (x % 2 == 0) {
                    builder.append("<tr class=\"highlight\">");
                } else {
                    builder.append("<tr>");
                }
                x++;
                
                builder.append("<td>").append(field.getDatatype()).append("</td>");
                builder.append("<td>").append(field.getFieldName()).append("</td>");
                builder.append("<td>").append(desc.getDescription()).append("</td>");
                builder.append("</tr>");
            }
        }
        
        builder.append("</table></body></html>\n");
        return builder.toString().getBytes(utf8);
    }
    
}
