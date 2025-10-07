package datawave.webservice.result.keyword;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

import datawave.webservice.query.result.event.MapSchema;
import datawave.webservice.xml.util.StringMapAdapter;
import io.protostuff.Input;
import io.protostuff.Message;
import io.protostuff.Output;
import io.protostuff.Schema;

@XmlAccessorType(XmlAccessType.NONE)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class DefaultTagCloud extends TagCloudBase<DefaultTagCloud,DefaultTagCloudEntry> implements Serializable, Message<DefaultTagCloud> {

    private static final long serialVersionUID = 6614332701390895105L;

    @XmlElement(name = "markings")
    @XmlJavaTypeAdapter(StringMapAdapter.class)
    private HashMap<String,String> markings = null;

    @XmlElement(name = "language")
    private String language = null;

    @XmlElementWrapper(name = "tags")
    @XmlElement(name = "tag")
    private List<DefaultTagCloudEntry> tags = null;

    @Override
    public Map<String,String> getMarkings() {
        if (markings != null) {
            return markings;
        } else {
            return super.getMarkings();
        }
    }

    public void setMarkings(Map<String,String> markings) {
        if (null != markings) {
            this.markings = new HashMap<>(markings);
        } else {
            this.markings = null;
        }
        super.setMarkings(this.markings);
    }

    @Override
    public String getLanguage() {
        return language;
    }

    @Override
    public void setLanguage(String language) {
        this.language = language;
    }

    public List<DefaultTagCloudEntry> getTags() {
        return tags;
    }

    public void setTags(List<DefaultTagCloudEntry> tags) {
        this.tags = tags;
    }

    @Override
    public Schema<DefaultTagCloud> cachedSchema() {
        return SCHEMA;
    }

    @XmlTransient
    private static final Schema<DefaultTagCloud> SCHEMA = new Schema<>() {
        public DefaultTagCloud newMessage() {
            return new DefaultTagCloud();
        }

        public Class<DefaultTagCloud> typeClass() {
            return DefaultTagCloud.class;
        }

        public String messageName() {
            return DefaultTagCloud.class.getSimpleName();
        }

        public String messageFullName() {
            return DefaultTagCloud.class.getName();
        }

        public boolean isInitialized(DefaultTagCloud message) {
            return true;
        }

        public void writeTo(Output output, DefaultTagCloud message) throws IOException {
            if (message.markings != null)
                output.writeObject(1, message.markings, MapSchema.SCHEMA, false);

            if (message.language != null) {
                output.writeString(2, message.language, false);
            }

            if (message.tags != null) {
                Schema<DefaultTagCloudEntry> schema = null;
                for (DefaultTagCloudEntry field : message.tags) {
                    if (field != null) {
                        if (schema == null) {
                            schema = field.cachedSchema();
                        }
                        output.writeObject(3, field, schema, true);
                    }
                }
            }
        }

        public void mergeFrom(Input input, DefaultTagCloud message) throws IOException {
            int number;
            Schema<DefaultTagCloudEntry> schema = null;
            while ((number = input.readFieldNumber(this)) != 0) {
                switch (number) {
                    case 1:
                        message.markings = new HashMap<String,String>();
                        input.mergeObject(message.markings, MapSchema.SCHEMA);
                        break;
                    case 2:
                        message.language = input.readString();
                        break;
                    case 3:
                        if (message.tags == null)
                            message.tags = new ArrayList<>();
                        if (null == schema) {
                            DefaultTagCloudEntry field = new DefaultTagCloudEntry();
                            schema = field.cachedSchema();
                        }
                        DefaultTagCloudEntry f = input.mergeObject(null, schema);
                        message.tags.add(f);
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
                    return "markings";
                case 2:
                    return "language";
                case 3:
                    return "tags";
                default:
                    return null;
            }
        }

        public int getFieldNumber(String name) {
            final Integer number = fieldMap.get(name);
            return number == null ? 0 : number;
        }

        final HashMap<String,Integer> fieldMap = new HashMap<String,Integer>();
        {
            fieldMap.put("markings", 1);
            fieldMap.put("language", 2);
            fieldMap.put("tags", 3);
        }
    };
}
