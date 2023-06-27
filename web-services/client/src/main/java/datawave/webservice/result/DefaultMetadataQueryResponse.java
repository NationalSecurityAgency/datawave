package datawave.webservice.result;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import datawave.webservice.metadata.DefaultMetadataField;
import datawave.webservice.metadata.MetadataFieldBase;
import io.protostuff.Input;
import io.protostuff.Message;
import io.protostuff.Output;
import io.protostuff.Schema;

@XmlRootElement(name = "DefaultMetadataQueryResponse")
@XmlAccessorType(XmlAccessType.NONE)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class DefaultMetadataQueryResponse extends MetadataQueryResponseBase<DefaultMetadataQueryResponse>
                implements Serializable, TotalResultsAware, Message<DefaultMetadataQueryResponse> {
    private static final long serialVersionUID = 7040643915602975506L;

    @XmlElementWrapper(name = "MetadataFields")
    @XmlElement(name = "MetadataField")
    private List<MetadataFieldBase> fields = null;

    @XmlElement(name = "TotalResults")
    private Long totalResults = null;

    public DefaultMetadataQueryResponse() {}

    public DefaultMetadataQueryResponse(List<DefaultMetadataField> fields) {
        if (fields == null) {
            this.fields = null;
            setTotalResults(0);
        } else {
            this.fields = new ArrayList<MetadataFieldBase>(fields);
            setTotalResults(this.fields.size());
        }
    }

    public List<MetadataFieldBase> getFields() {
        return fields == null ? null : Collections.unmodifiableList(fields);
    }

    public static Schema<DefaultMetadataQueryResponse> getSchema() {
        return SCHEMA;
    }

    @Override
    public Schema<DefaultMetadataQueryResponse> cachedSchema() {
        return SCHEMA;
    }

    @XmlTransient
    private static final Schema<DefaultMetadataQueryResponse> SCHEMA = new Schema<DefaultMetadataQueryResponse>() {
        public DefaultMetadataQueryResponse newMessage() {
            return new DefaultMetadataQueryResponse();
        }

        public Class<DefaultMetadataQueryResponse> typeClass() {
            return DefaultMetadataQueryResponse.class;
        }

        public String messageName() {
            return DefaultMetadataQueryResponse.class.getSimpleName();
        }

        public String messageFullName() {
            return DefaultMetadataQueryResponse.class.getName();
        }

        public boolean isInitialized(DefaultMetadataQueryResponse message) {
            return true;
        }

        public void writeTo(Output output, DefaultMetadataQueryResponse message) throws IOException {
            if (message.totalResults != null) {
                output.writeUInt64(1, message.totalResults, false);
            }

            if (message.fields != null) {
                for (MetadataFieldBase field : message.fields) {
                    if (field != null)
                        output.writeObject(2, (DefaultMetadataField) field, DefaultMetadataField.getSchema(), true);
                }
            }
        }

        public void mergeFrom(Input input, DefaultMetadataQueryResponse message) throws IOException {
            int number;
            while ((number = input.readFieldNumber(this)) != 0) {
                switch (number) {
                    case 1:
                        message.setTotalResults(input.readUInt64());
                    case 2:
                        if (message.fields == null) {
                            message.fields = new ArrayList<MetadataFieldBase>();
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
            return number == null ? 0 : number.intValue();
        }

        final java.util.HashMap<String,Integer> fieldMap = new java.util.HashMap<String,Integer>();
        {
            fieldMap.put("totalResults", 1);
            fieldMap.put("fields", 2);
        }
    };

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
}
