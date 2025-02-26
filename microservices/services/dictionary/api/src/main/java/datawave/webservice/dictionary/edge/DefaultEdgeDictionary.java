package datawave.webservice.dictionary.edge;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import datawave.webservice.HtmlProvider;
import datawave.webservice.query.result.util.protostuff.FieldAccessor;
import datawave.webservice.query.result.util.protostuff.ProtostuffField;
import datawave.webservice.result.TotalResultsAware;
import io.protostuff.Input;
import io.protostuff.Message;
import io.protostuff.Output;
import io.protostuff.Schema;

@XmlRootElement(name = "EdgeDictionary")
@XmlAccessorType(XmlAccessType.NONE)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class DefaultEdgeDictionary extends EdgeDictionaryBase<DefaultEdgeDictionary,DefaultMetadata>
                implements TotalResultsAware, Message<DefaultEdgeDictionary>, HtmlProvider {
    
    private static final long serialVersionUID = 1L;
    private static final String TITLE = "Edge Dictionary", SEP = ", ";
    
    @XmlElementWrapper(name = "EdgeMetadata")
    @XmlElement(name = "Metadata")
    private List<DefaultMetadata> metadataList = null;
    
    @XmlElement(name = "TotalResults")
    private Long totalResults = null;
    
    public DefaultEdgeDictionary() {}
    
    public DefaultEdgeDictionary(Collection<DefaultMetadata> fields) {
        if (fields == null) {
            this.metadataList = null;
            setTotalResults(0);
        } else {
            this.metadataList = new LinkedList<>(fields);
            setTotalResults(this.metadataList.size());
            this.setHasResults(true);
        }
    }
    
    @Override
    public List<? extends MetadataBase<DefaultMetadata>> getMetadataList() {
        return metadataList == null ? null : Collections.unmodifiableList(metadataList);
    }
    
    public static Schema<DefaultEdgeDictionary> getSchema() {
        return SCHEMA;
    }
    
    @Override
    public Schema<DefaultEdgeDictionary> cachedSchema() {
        return SCHEMA;
    }
    
    private enum DICT_BASE implements FieldAccessor {
        METADATA(1, "metadataField"), TOTAL(2, "totalResults"), UNKNOWN(0, "UNKNOWN");
        
        final int fn;
        final String name;
        
        DICT_BASE(int fn, String name) {
            this.fn = fn;
            this.name = name;
        }
        
        public int getFieldNumber() {
            return fn;
        }
        
        public String getFieldName() {
            return name;
        }
    }
    
    private static final ProtostuffField<DICT_BASE> PFIELD = new ProtostuffField<>(DICT_BASE.class);
    
    @XmlTransient
    private static final Schema<DefaultEdgeDictionary> SCHEMA = new Schema<DefaultEdgeDictionary>() {
        public DefaultEdgeDictionary newMessage() {
            return new DefaultEdgeDictionary();
        }
        
        public Class<DefaultEdgeDictionary> typeClass() {
            return DefaultEdgeDictionary.class;
        }
        
        public String messageName() {
            return DefaultEdgeDictionary.class.getSimpleName();
        }
        
        public String messageFullName() {
            return DefaultEdgeDictionary.class.getName();
        }
        
        public boolean isInitialized(DefaultEdgeDictionary message) {
            return true;
        }
        
        public void writeTo(Output output, DefaultEdgeDictionary message) throws IOException {
            if (message.metadataList != null) {
                for (DefaultMetadata metadata : message.metadataList) {
                    output.writeObject(DICT_BASE.METADATA.getFieldNumber(), metadata, DefaultMetadata.getSchema(), true);
                }
            }
            if (message.totalResults != null) {
                output.writeUInt64(DICT_BASE.TOTAL.getFieldNumber(), message.totalResults, false);
            }
        }
        
        public void mergeFrom(Input input, DefaultEdgeDictionary message) throws IOException {
            int number;
            while ((number = input.readFieldNumber(this)) != 0) {
                switch (number) {
                    case 1:
                        message.setTotalResults(input.readUInt64());
                        break;
                    case 2:
                        if (message.metadataList == null) {
                            message.metadataList = new ArrayList<>();
                        }
                        message.metadataList.add(input.mergeObject(null, DefaultMetadata.getSchema()));
                        break;
                    default:
                        input.handleUnknownField(number, this);
                        break;
                }
            }
        }
        
        @Override
        public String getFieldName(int number) {
            DICT_BASE field = PFIELD.parseFieldNumber(number);
            if (field == DICT_BASE.UNKNOWN) {
                return null;
            }
            return field.getFieldName();
        }
        
        @Override
        public int getFieldNumber(String name) {
            DICT_BASE field = PFIELD.parseFieldName(name);
            return field.getFieldNumber();
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
        return "";
    }
    
    public String getPageHeader() {
        return getTitle();
    }
    
    @Override
    public String getMainContent() {
        StringBuilder builder = new StringBuilder(2048);
        builder.append("<div><ul>").append("<li class=\"left\">Edge Type: Defined either by Datawave Configuration files or Edge enrichment field.</li>")
                        .append("<li class=\"left\">Edge Relationship: Defined by Datawave Configuration files</li>")
                        .append("<li class=\"left\">Edge Attribute1 Source: Defined by Datawave Configuration files and optional attributes Attribute2 and Attribute3</li>")
                        .append("<li class=\"left\">Fields: List of Field Name pairs used to generate this edge type.</li>")
                        .append("<li class=\"left\">Fields Format:<pre>[Source Field, Target Field | Enrichment Field=Enrichment Field Value]</pre></li>")
                        .append("<li class=\"left\">Date: start date of edge type creation, format: yyyyMMdd</li>").append("</ul></div>");
        
        builder.append("<table id=\"myTable\" class=\"creds\">\n")
                        .append("<thead><tr><th>Edge Type</th><th>Edge Relationship</th><th>Edge Attribute1 Source</th>")
                        .append("<th>Fields</th><th>Date</th></tr></thead>");
        
        builder.append("<tbody>");
        int x = 0;
        for (MetadataBase<DefaultMetadata> metadata : this.getMetadataList()) {
            // highlight alternating rows
            if (x % 2 == 0) {
                builder.append("<tr class=\"highlight\">");
            } else {
                builder.append("<tr>");
            }
            x++;
            
            String type = metadata.getEdgeType();
            String relationship = metadata.getEdgeRelationship();
            String collect = metadata.getEdgeAttribute1Source();
            StringBuilder fieldBuilder = new StringBuilder();
            for (EventField field : metadata.getEventFields()) {
                fieldBuilder.append(field).append(SEP);
            }
            
            String fieldNames = fieldBuilder.toString().substring(0, fieldBuilder.length() - 2);
            String date = metadata.getStartDate();
            
            builder.append("<td>").append(type).append("</td>");
            builder.append("<td>").append(relationship).append("</td>");
            builder.append("<td>").append(collect).append("</td>");
            builder.append("<td>").append(fieldNames).append("</td>");
            builder.append("<td>").append(date).append("</td>");
            
            builder.append("</td>").append("</tr>");
        }
        builder.append("</tbody>");
        
        builder.append("</table>\n");
        
        return builder.toString();
    }
}
