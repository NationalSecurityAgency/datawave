package datawave.webservice.results.ingest.file;

import java.io.IOException;
import java.io.Serializable;
import java.util.Date;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlTransient;

import io.protostuff.Input;
import io.protostuff.Message;
import io.protostuff.Output;
import io.protostuff.Schema;

@XmlAccessorType(XmlAccessType.NONE)
public class FileDetails implements Serializable, Message<FileDetails> {
    @XmlAttribute(name = "filePath")
    private String path;
    @XmlAttribute(name = "dateReceived")
    private Date date;
    @XmlAttribute(name = "fileSize")
    private long size;

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    @Override
    public Schema<FileDetails> cachedSchema() {
        return SCHEMA;
    }

    public static Schema<FileDetails> getSchema() {
        return SCHEMA;
    }

    @XmlTransient
    private static final Schema<FileDetails> SCHEMA = new Schema<FileDetails>() {
        // schema methods

        public FileDetails newMessage() {
            return new FileDetails();
        }

        public Class<FileDetails> typeClass() {
            return FileDetails.class;
        }

        public String messageName() {
            return FileDetails.class.getSimpleName();
        }

        public String messageFullName() {
            return FileDetails.class.getName();
        }

        public boolean isInitialized(FileDetails message) {
            return true;
        }

        public void writeTo(Output output, FileDetails message) throws IOException {

            if (message.getPath() != null) {
                output.writeString(1, message.getPath(), false);
            }
            if (message.getDate() != null) {
                output.writeUInt64(2, message.getDate().getTime(), false);
            }
            output.writeUInt64(3, message.getSize(), false);
        }

        public void mergeFrom(Input input, FileDetails message) throws IOException {
            int number;
            while ((number = input.readFieldNumber(this)) != 0) {
                switch (number) {
                    case 1:
                        message.setPath(input.readString());
                        break;
                    case 2:
                        message.setDate(new Date(input.readUInt64()));
                        break;
                    case 3:
                        message.setSize(input.readUInt64());
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
                    return "filePath";
                case 2:
                    return "dateReceived";
                case 3:
                    return "fileSize";
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
            fieldMap.put("filePath", 1);
            fieldMap.put("dateReceived", 2);
            fieldMap.put("fileSize", 3);
        }
    };

}
