package datawave.webservice.query.result.istat;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;

import datawave.webservice.result.BaseQueryResponse;

import io.protostuff.Input;
import io.protostuff.Message;
import io.protostuff.Output;
import io.protostuff.Schema;

@XmlRootElement(name = "IndexStatsResponse")
@XmlAccessorType(XmlAccessType.NONE)
public class IndexStatsResponse extends BaseQueryResponse implements Serializable, Message<IndexStatsResponse> {
    private static final long serialVersionUID = -9218640841543434133L;

    @XmlElementWrapper(name = "FieldStatList")
    @XmlElement(name = "fieldStats")
    private List<FieldStat> fieldStats = new LinkedList<FieldStat>();

    public void addFieldStat(FieldStat fs) {
        fieldStats.add(fs);
    }

    public List<FieldStat> getFieldStats() {
        return Collections.unmodifiableList(fieldStats);
    }

    private static final Schema<IndexStatsResponse> SCHEMA = new Schema<IndexStatsResponse>() {

        @Override
        public String messageFullName() {
            return IndexStatsResponse.class.getName();
        }

        @Override
        public String messageName() {
            return IndexStatsResponse.class.getSimpleName();
        }

        @Override
        public IndexStatsResponse newMessage() {
            return new IndexStatsResponse();
        }

        @Override
        public Class<? super IndexStatsResponse> typeClass() {
            return IndexStatsResponse.class;
        }

        @Override
        public boolean isInitialized(IndexStatsResponse arg0) {
            return true;
        }

        @Override
        public String getFieldName(int n) {
            switch (n) {
                case 1:
                    return "fieldStats";
                default:
                    return null;
            }
        }

        @Override
        public int getFieldNumber(String arg0) {
            if ("fieldStats".equals(arg0)) {
                return 1;
            } else {
                return 0;
            }
        }

        @Override
        public void mergeFrom(Input input, IndexStatsResponse other) throws IOException {
            int number;
            while ((number = input.readFieldNumber(this)) != 0) {
                switch (number) {
                    case 1:
                        other.fieldStats.add(input.mergeObject(null, FieldStat.SCHEMA));
                    default:
                        input.handleUnknownField(number, SCHEMA);
                }
            }
        }

        @Override
        public void writeTo(Output output, IndexStatsResponse message) throws IOException {
            for (FieldStat fs : message.fieldStats) {
                output.writeObject(1, fs, FieldStat.SCHEMA, true);
            }
        }
    };

    @Override
    public Schema<IndexStatsResponse> cachedSchema() {
        return SCHEMA;
    }
}
