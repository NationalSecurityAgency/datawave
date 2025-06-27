package datawave.webservice.result.keyword;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlTransient;

import io.protostuff.Input;
import io.protostuff.Message;
import io.protostuff.Output;
import io.protostuff.Schema;

@XmlAccessorType(XmlAccessType.NONE)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class DefaultTagCloudEntry extends TagCloudEntryBase<DefaultTagCloudEntry> implements Serializable, Message<DefaultTagCloudEntry> {

    private static final long serialVersionUID = -1807574075079481434L;

    @XmlElement(name = "term")
    String term;
    @XmlElement(name = "score")
    double score;
    @XmlElement(name = "df")
    int frequency;
    @XmlElementWrapper(name = "sources")
    @XmlElement(name = "source")
    List<String> sources = new ArrayList<>();

    public int getFrequency() {
        return frequency;
    }

    public void setFrequency(int frequency) {
        this.frequency = frequency;
    }

    public double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
    }

    public Collection<String> getSources() {
        return sources;
    }

    public void setSources(Collection<String> sources) {
        this.sources.clear();
        this.sources.addAll(sources);
    }

    public String getTerm() {
        return term;
    }

    public void setTerm(String term) {
        this.term = term;
    }

    public Schema<DefaultTagCloudEntry> cachedSchema() {
        return SCHEMA;
    }

    @XmlTransient
    private static final Schema<DefaultTagCloudEntry> SCHEMA = new Schema<>() {

        @Override
        public DefaultTagCloudEntry newMessage() {
            return new DefaultTagCloudEntry();
        }

        @Override
        public Class<? super DefaultTagCloudEntry> typeClass() {
            return DefaultTagCloudEntry.class;
        }

        @Override
        public String messageName() {
            return DefaultTagCloudEntry.class.getSimpleName();
        }

        @Override
        public String messageFullName() {
            return DefaultTagCloudEntry.class.getName();
        }

        @Override
        public boolean isInitialized(DefaultTagCloudEntry message) {
            return true;
        }

        @Override
        public void writeTo(Output output, DefaultTagCloudEntry message) throws IOException {
            if (message.term != null)
                output.writeString(1, message.term, false);

            output.writeDouble(2, message.score, false);
            output.writeUInt32(3, message.frequency, false);

            if (message.sources != null) {
                for (String source : message.sources) {
                    output.writeString(4, source, true);
                }
            }
        }

        @Override
        public void mergeFrom(Input input, DefaultTagCloudEntry message) throws IOException {
            int number;
            while ((number = input.readFieldNumber(this)) != 0) {
                switch (number) {
                    case 1:
                        message.term = input.readString();
                        break;
                    case 2:
                        message.score = input.readDouble();
                        break;
                    case 3:
                        message.frequency = input.readUInt32();
                        break;
                    case 4:
                        if (message.sources == null) {
                            message.sources = new ArrayList<>();
                        }
                        message.sources.add(input.readString());
                        break;
                    default:
                        input.handleUnknownField(number, this);
                        break;
                }
            }
        }

        @Override
        public String getFieldName(int number) {
            switch (number) {
                case 1:
                    return "term";
                case 2:
                    return "score";
                case 3:
                    return "frequency";
                case 4:
                    return "sources";
                default:
                    return null;
            }
        }

        @Override
        public int getFieldNumber(String name) {
            final Integer number = fieldMap.get(name);
            return number == null ? 0 : number;
        }

        private final HashMap<String,Integer> fieldMap = new HashMap<String,Integer>();
        {
            fieldMap.put("term", 1);
            fieldMap.put("score", 2);
            fieldMap.put("frequency", 3);
            fieldMap.put("name", 4);
            fieldMap.put("sources", 5);
        }
    };
}
