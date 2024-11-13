package datawave.webservice.results.mr;

import java.io.IOException;
import java.io.Serializable;

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlTransient;

import io.protostuff.Input;
import io.protostuff.Message;
import io.protostuff.Output;
import io.protostuff.Schema;

@XmlAccessorType(XmlAccessType.NONE)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class ResultFile implements Serializable, Message<ResultFile> {

    private static final long serialVersionUID = 1L;
    @XmlAttribute(name = "fileName")
    private String fileName;
    @XmlAttribute(name = "length")
    private long length;

    public String getFileName() {
        return fileName;
    }

    public long getLength() {
        return length;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public void setLength(long length) {
        this.length = length;
    }

    @Override
    public Schema<ResultFile> cachedSchema() {
        return SCHEMA;
    }

    public static Schema<ResultFile> getSchema() {
        return SCHEMA;
    }

    @XmlTransient
    private static final Schema<ResultFile> SCHEMA = new Schema<ResultFile>() {
        // schema methods

        public ResultFile newMessage() {
            return new ResultFile();
        }

        public Class<ResultFile> typeClass() {
            return ResultFile.class;
        }

        public String messageName() {
            return ResultFile.class.getSimpleName();
        }

        public String messageFullName() {
            return ResultFile.class.getName();
        }

        public boolean isInitialized(ResultFile message) {
            return true;
        }

        public void writeTo(Output output, ResultFile message) throws IOException {

            if (message.getFileName() != null) {
                output.writeString(1, message.getFileName(), false);
            }

            output.writeUInt64(2, message.getLength(), false);
        }

        public void mergeFrom(Input input, ResultFile message) throws IOException {
            int number;
            while ((number = input.readFieldNumber(this)) != 0) {
                switch (number) {
                    case 1:
                        message.setFileName(input.readString());
                        break;
                    case 2:
                        message.setLength(input.readUInt64());
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
                    return "fileName";
                case 2:
                    return "length";
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
            fieldMap.put("fileName", 1);
            fieldMap.put("length", 2);
        }
    };

}
