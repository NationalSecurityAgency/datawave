package datawave.webservice.dictionary.data;

import java.io.IOException;
import java.util.List;
import java.util.Map.Entry;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

import datawave.webservice.HtmlProvider;
import datawave.webservice.result.TotalResultsAware;
import io.protostuff.Input;
import io.protostuff.Message;
import io.protostuff.Output;
import io.protostuff.Schema;

@XmlRootElement(name = "DefaultFieldsResponse")
@XmlAccessorType(XmlAccessType.NONE)
public class DefaultFields extends FieldsBase<DefaultFields,DefaultDictionaryField,DefaultDescription>
                implements TotalResultsAware, Message<DefaultFields>, HtmlProvider {
    
    private static final long serialVersionUID = 1L;
    private static final String TITLE = "Field Descriptions", EMPTY = "";
    
    @XmlElementWrapper(name = "Fields")
    @XmlElement(name = "Field")
    private List<DefaultDictionaryField> fields = Lists.newArrayList();
    
    @XmlElement(name = "TotalResults")
    private Long totalResults = 0L;
    
    public DefaultFields() {}
    
    public DefaultFields(Multimap<Entry<String,String>,DefaultDescription> descriptions) {
        this.fields = transform(descriptions);
        this.totalResults = (long) this.fields.size();
        this.setHasResults(this.totalResults > 0);
    }
    
    private List<DefaultDictionaryField> transform(Multimap<Entry<String,String>,DefaultDescription> descriptions) {
        List<DefaultDictionaryField> list = Lists.newArrayListWithCapacity(descriptions.size());
        
        for (Entry<Entry<String,String>,DefaultDescription> entry : descriptions.entries()) {
            list.add(new DefaultDictionaryField(entry.getKey().getKey(), entry.getKey().getValue(), entry.getValue()));
        }
        
        return list;
    }
    
    public List<DefaultDictionaryField> getFields() {
        return fields;
    }
    
    public void setFields(List<DefaultDictionaryField> fields) {
        this.fields = fields;
    }
    
    public void setDescriptions(Multimap<Entry<String,String>,DefaultDescription> descriptions) {
        this.fields = transform(descriptions);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see datawave.webservice.result.TotalResultsAware#setTotalResults(long)
     */
    @Override
    public void setTotalResults(long totalResults) {
        this.totalResults = totalResults;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see datawave.webservice.result.TotalResultsAware#getTotalResults()
     */
    @Override
    public long getTotalResults() {
        return this.totalResults;
    }
    
    public static Schema<DefaultFields> getSchema() {
        return SCHEMA;
    }
    
    @Override
    /*
     * (non-Javadoc)
     * 
     * @see com.dyuproject.protostuff.Message#cachedSchema()
     */
    public Schema<DefaultFields> cachedSchema() {
        return SCHEMA;
    }
    
    @XmlTransient
    private static final Schema<DefaultFields> SCHEMA = new Schema<DefaultFields>() {
        public DefaultFields newMessage() {
            return new DefaultFields();
        }
        
        public Class<DefaultFields> typeClass() {
            return DefaultFields.class;
        }
        
        public String messageName() {
            return DefaultFields.class.getSimpleName();
        }
        
        public String messageFullName() {
            return DefaultFields.class.getName();
        }
        
        public boolean isInitialized(DefaultFields message) {
            return true;
        }
        
        public void writeTo(Output output, DefaultFields message) throws IOException {
            if (message.totalResults != null) {
                output.writeUInt64(1, message.totalResults, false);
            }
            
            if (message.fields != null) {
                for (DictionaryFieldBase fd : message.fields) {
                    output.writeObject(2, (DefaultDictionaryField) fd, DefaultDictionaryField.getSchema(), true);
                }
            }
        }
        
        public void mergeFrom(Input input, DefaultFields message) throws IOException {
            int number;
            while ((number = input.readFieldNumber(this)) != 0) {
                switch (number) {
                    case 1:
                        message.setTotalResults(input.readUInt64());
                        break;
                    case 2:
                        if (message.fields == null) {
                            message.fields = Lists.newArrayList();
                        }
                        
                        message.fields.add(input.mergeObject(null, DefaultDictionaryField.getSchema()));
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
                    return "descriptions";
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
            fieldMap.put("descriptions", 2);
        }
    };
    
    @Override
    public String getTitle() {
        return TITLE;
    }
    
    @Override
    public String getPageHeader() {
        return getTitle();
    }
    
    @Override
    public String getHeadContent() {
        return EMPTY;
    }
    
    @Override
    public String getMainContent() {
        StringBuilder builder = new StringBuilder();
        
        builder.append("<table>\n");
        builder.append("<tr><th>Datatype</th><th>FieldName</th><th>Description</th></tr>");
        
        int x = 0;
        for (DefaultDictionaryField field : this.getFields()) {
            for (DefaultDescription desc : field.getDescriptions()) {
                addDescriptionRow(field, desc, x, builder);
                x++;
            }
        }
        
        builder.append("</table>\n");
        
        return builder.toString();
    }
    
    public static void addDescriptionRow(DictionaryFieldBase<?,? extends DescriptionBase> field, DescriptionBase desc, int rowNum, StringBuilder builder) {
        // highlight alternating rows
        if (rowNum % 2 == 0) {
            builder.append("<tr class=\"highlight\">");
        } else {
            builder.append("<tr>");
        }
        
        builder.append("<td>").append(field.getDatatype()).append("</td>");
        builder.append("<td>").append(field.getFieldName()).append("</td>");
        builder.append("<td>").append(desc.getDescription()).append("</td>");
        builder.append("</tr>");
    }
}
