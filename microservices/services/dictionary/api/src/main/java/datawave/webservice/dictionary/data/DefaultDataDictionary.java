package datawave.webservice.dictionary.data;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import com.google.common.collect.Lists;

import datawave.webservice.HtmlProvider;
import datawave.webservice.metadata.DefaultMetadataField;
import datawave.webservice.result.TotalResultsAware;
import io.protostuff.Input;
import io.protostuff.Message;
import io.protostuff.Output;
import io.protostuff.Schema;

@XmlRootElement(name = "DefaultDataDictionary")
@XmlAccessorType(XmlAccessType.NONE)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class DefaultDataDictionary extends DataDictionaryBase<DefaultDataDictionary,DefaultMetadataField>
                implements TotalResultsAware, Message<DefaultDataDictionary>, HtmlProvider {
    
    private static final long serialVersionUID = 1L;
    private static final String TITLE = "Data Dictionary", EMPTY_STR = "", SEP = ", ";
    
    /*
     * Loads jQuery, DataTables, some CSS elements for DataTables, and executes `.dataTables()` on the HTML table in the payload.
     * 
     * Pagination on the table is turned off, we do an ascending sort on the 2nd column (field name) and a cookie is saved in the browser that will leave the
     * last sort in place upon revisit of the page.
     */
    private static final String DATA_TABLES_TEMPLATE = "<script type=''text/javascript'' src=''{0}jquery.min.js''></script>\n"
                    + "<script type=''text/javascript'' src=''{1}jquery.dataTables.min.js''></script>\n" + "<script type=''text/javascript''>\n"
                    + "$(document).ready(function() '{' $(''#myTable'').dataTable('{'\"bPaginate\": false, \"aaSorting\": [[0, \"asc\"]], \"bStateSave\": true'}'); setTimeout( function() '{' $(''#myTable'').find(\"td\").css(\"word-break\", \"break-word\"); '}', 4000); '}');\n"
                    + "</script>\n";
    
    private final String dataTablesHeader;
    
    @XmlElementWrapper(name = "MetadataFields")
    @XmlElement(name = "MetadataField")
    private List<DefaultMetadataField> fields = null;
    
    @XmlElement(name = "TotalResults")
    private Long totalResults = null;
    
    public DefaultDataDictionary() {
        this("/webjars/jquery/", "/webjars/datatables/");
    }
    
    public DefaultDataDictionary(String jqueryUri, String datatablesUri) {
        this.dataTablesHeader = MessageFormat.format(DATA_TABLES_TEMPLATE, jqueryUri, datatablesUri);
    }
    
    public DefaultDataDictionary(Collection<DefaultMetadataField> fields) {
        this();
        if (fields == null) {
            this.fields = null;
            setTotalResults(0);
        } else {
            this.fields = new ArrayList<>(fields);
            setTotalResults(this.fields.size());
            this.setHasResults(true);
        }
    }
    
    public List<DefaultMetadataField> getFields() {
        return fields == null ? null : Collections.unmodifiableList(fields);
    }
    
    public void setFields(Collection<DefaultMetadataField> fields) {
        this.fields = Lists.newArrayList(fields);
    }
    
    public static Schema<DefaultDataDictionary> getSchema() {
        return SCHEMA;
    }
    
    @Override
    public Schema<DefaultDataDictionary> cachedSchema() {
        return SCHEMA;
    }
    
    @XmlTransient
    private static final Schema<DefaultDataDictionary> SCHEMA = new Schema<DefaultDataDictionary>() {
        public DefaultDataDictionary newMessage() {
            return new DefaultDataDictionary();
        }
        
        public Class<DefaultDataDictionary> typeClass() {
            return DefaultDataDictionary.class;
        }
        
        public String messageName() {
            return DefaultDataDictionary.class.getSimpleName();
        }
        
        public String messageFullName() {
            return DefaultDataDictionary.class.getName();
        }
        
        public boolean isInitialized(DefaultDataDictionary message) {
            return true;
        }
        
        public void writeTo(Output output, DefaultDataDictionary message) throws IOException {
            if (message.totalResults != null) {
                output.writeUInt64(1, message.totalResults, false);
            }
            
            if (message.fields != null) {
                for (DefaultMetadataField field : message.fields) {
                    if (field != null)
                        output.writeObject(2, field, DefaultMetadataField.getSchema(), true);
                }
            }
        }
        
        public void mergeFrom(Input input, DefaultDataDictionary message) throws IOException {
            int number;
            while ((number = input.readFieldNumber(this)) != 0) {
                switch (number) {
                    case 1:
                        message.setTotalResults(input.readUInt64());
                        break;
                    case 2:
                        if (message.fields == null) {
                            message.fields = new ArrayList<>();
                        }
                        
                        message.fields.add(input.mergeObject(null, DefaultMetadataField.getSchema()));
                        break;
                    default:
                        input.handleUnknownField(number, this);
                        break;
                }
            }
        }
        
        public String getFieldName(int number) {
            switch (number) {
                case 1:
                    return "totalResults";
                case 2:
                    return "fields";
                default:
                    return null;
            }
        }
        
        public int getFieldNumber(String name) {
            final Integer number = fieldMap.get(name);
            return number == null ? 0 : number;
        }
        
        final java.util.HashMap<String,Integer> fieldMap = new java.util.HashMap<>();
        {
            fieldMap.put("totalResults", 1);
            fieldMap.put("fields", 2);
        }
    };
    
    @Override
    public void setTotalResults(long totalResults) {
        this.totalResults = totalResults;
    }
    
    @Override
    public long getTotalResults() {
        return this.totalResults;
    }
    
    @Override
    public String getTitle() {
        return TITLE;
    }
    
    @Override
    public String getHeadContent() {
        return dataTablesHeader;
    }
    
    @Override
    public String getPageHeader() {
        return getTitle();
    }
    
    @Override
    public String getMainContent() {
        StringBuilder builder = new StringBuilder(2048);
        builder.append("<div>");
        builder.append("<p style=\"width:60%; margin-left: auto; margin-right: auto;\">When a value is present in the forward index types, this means that a field is indexed and informs you how your ");
        builder.append("query terms will be treated (e.g. text, number, IPv4 address, etc). The same applies for the reverse index types with ");
        builder.append("the caveat that you can also query these fields using leading wildcards. Fields that are marked as 'Index only' will not ");
        builder.append("appear in a result set unless explicitly queried on. Index only fields are typically composite fields, derived from actual data, ");
        builder.append("created by the software to make querying easier.</p>");
        builder.append("</div>");
        builder.append("<table id=\"myTable\">\n");
        
        builder.append("<thead><tr><th>FieldName</th><th>Internal FieldName</th><th>DataType</th>");
        builder.append("<th>Index only</th><th>Forward Indexed</th><th>Reverse Indexed</th><th>Normalized</th><th>Types</th><th>Tokenized</th><th>Description</th><th>LastUpdated</th></tr></thead>");
        
        builder.append("<tbody>");
        for (DefaultMetadataField f : this.getFields()) {
            builder.append("<tr>");
            
            String fieldName = (null == f.getFieldName()) ? EMPTY_STR : f.getFieldName();
            String internalFieldName = (null == f.getInternalFieldName()) ? EMPTY_STR : f.getInternalFieldName();
            String datatype = (null == f.getDataType()) ? EMPTY_STR : f.getDataType();
            
            StringBuilder types = new StringBuilder();
            if (null != f.getTypes()) {
                for (String forwardIndexType : f.getTypes()) {
                    if (0 != types.length()) {
                        types.append(SEP);
                    }
                    types.append(forwardIndexType);
                }
            }
            
            builder.append("<td>").append(fieldName).append("</td>");
            builder.append("<td>").append(internalFieldName).append("</td>");
            builder.append("<td>").append(datatype).append("</td>");
            builder.append("<td>").append(f.isIndexOnly()).append("</td>");
            builder.append("<td>").append(f.isForwardIndexed() ? true : "").append("</td>");
            builder.append("<td>").append(f.isReverseIndexed() ? true : "").append("</td>");
            builder.append("<td>").append(f.getTypes() != null && f.getTypes().size() > 0 ? "true" : "false").append("</td>");
            builder.append("<td>").append(types).append("</td>");
            builder.append("<td>").append(f.isTokenized() ? true : "").append("</td>");
            builder.append("<td>");
            
            boolean first = true;
            for (DescriptionBase desc : f.getDescriptions()) {
                if (!first) {
                    builder.append(", ");
                }
                builder.append(desc.getMarkings()).append(" ").append(desc.getDescription());
                first = false;
            }
            
            builder.append("</td>");
            builder.append("<td>").append(f.getLastUpdated()).append("</td>").append("</tr>");
        }
        builder.append("</tbody>");
        
        builder.append("</table>\n");
        
        return builder.toString();
    }
    
    @Override
    public void transformFields(Consumer<DefaultMetadataField> transformer) {
        fields.forEach(transformer);
    }
    
    @Override
    public String toString() {
        return "DefaultDataDictionary{" + "fields=" + fields + ", totalResults=" + totalResults + "} " + super.toString();
    }
}
