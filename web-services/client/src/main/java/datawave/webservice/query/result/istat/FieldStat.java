package datawave.webservice.query.result.istat;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map.Entry;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;

import io.protostuff.Input;
import io.protostuff.Output;
import io.protostuff.Schema;

/**
 * The FieldStat class represents the field, the number of unique terms associated with the field, the total number of times the field was observed, and the
 * selectivity of the term (selectivity = unique / observed).
 *
 */
@XmlAccessorType(XmlAccessType.NONE)
@XmlType(propOrder = {"field", "unique", "observed", "selectivity"})
public class FieldStat implements Serializable {
    private static final long serialVersionUID = 4172426563796252101L;

    @XmlElement(name = "field")
    public String field;

    @XmlElement(name = "unique")
    public long unique;

    @XmlElement(name = "observed")
    public long observed;

    @XmlElement(name = "selectivity")
    public double selectivity;

    public static final Schema<FieldStat> SCHEMA = new Schema<FieldStat>() {

        HashMap<String,Integer> fields = new HashMap<String,Integer>();
        HashMap<Integer,String> numbers = new HashMap<Integer,String>();

        {
            fields.put("field", 1);
            fields.put("unique", 2);
            fields.put("observed", 3);
            fields.put("selectivity", 4);

            for (Entry<String,Integer> e : fields.entrySet()) {
                numbers.put(e.getValue(), e.getKey());
            }
        }

        @Override
        public String getFieldName(int number) {
            return numbers.get(number);
        }

        @Override
        public int getFieldNumber(String name) {
            return fields.containsKey(name) ? fields.get(name) : 0;
        }

        @Override
        public boolean isInitialized(FieldStat message) {
            return true;
        }

        @Override
        public FieldStat newMessage() {
            return new FieldStat();
        }

        @Override
        public String messageName() {
            return FieldStat.class.getSimpleName();
        }

        @Override
        public String messageFullName() {
            return FieldStat.class.getName();
        }

        @Override
        public Class<? super FieldStat> typeClass() {
            return FieldStat.class;
        }

        @Override
        public void mergeFrom(Input input, FieldStat message) throws IOException {
            int number;
            while ((number = input.readFieldNumber(this)) != 0) {
                switch (number) {
                    case 1:
                        message.field = input.readString();
                        break;
                    case 2:
                        message.unique = input.readUInt64();
                        break;
                    case 3:
                        message.observed = input.readUInt64();
                        break;
                    case 4:
                        message.selectivity = input.readDouble();
                        break;
                    default:
                        input.handleUnknownField(number, this);
                }
            }

        }

        @Override
        public void writeTo(Output output, FieldStat message) throws IOException {
            output.writeString(1, message.field, false);
            output.writeUInt64(2, message.unique, false);
            output.writeUInt64(3, message.observed, false);
            output.writeDouble(4, message.selectivity, false);
        }

    };
}
