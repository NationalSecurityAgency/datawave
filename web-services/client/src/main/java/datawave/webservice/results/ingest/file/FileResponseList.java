package datawave.webservice.results.ingest.file;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import datawave.webservice.query.exception.QueryExceptionType;
import datawave.webservice.result.BaseResponse;
import io.protostuff.Input;
import io.protostuff.Message;
import io.protostuff.Output;
import io.protostuff.Schema;

@XmlRootElement(name = "FileResponseList")
@XmlAccessorType(XmlAccessType.NONE)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class FileResponseList extends BaseResponse implements Serializable, Message<FileResponseList> {
    private static final long serialVersionUID = 1L;

    @XmlAttribute(name = "truncated", required = false)
    protected Boolean resultsTruncated;

    @XmlElementWrapper(name = "FileResponseList")
    @XmlElement(name = "File")
    protected List<FileDetails> results = new ArrayList<FileDetails>();

    public List<FileDetails> getFiles() {
        return results;
    }

    public void setFiles(List<FileDetails> results) {
        this.results = results;
    }

    public void setResultsTruncated(Boolean resultsTruncated) {
        this.resultsTruncated = resultsTruncated;
    }

    public Boolean getResulsTruncated() {
        return resultsTruncated;
    }

    @Override
    public Schema<FileResponseList> cachedSchema() {
        return SCHEMA;
    }

    public static Schema<FileResponseList> getSchema() {
        return SCHEMA;
    }

    @XmlTransient
    private static final Schema<FileResponseList> SCHEMA = new Schema<FileResponseList>() {
        // schema methods

        public FileResponseList newMessage() {
            return new FileResponseList();
        }

        public Class<FileResponseList> typeClass() {
            return FileResponseList.class;
        }

        public String messageName() {
            return FileResponseList.class.getSimpleName();
        }

        public String messageFullName() {
            return FileResponseList.class.getName();
        }

        public boolean isInitialized(FileResponseList message) {
            return true;
        }

        public void writeTo(Output output, FileResponseList message) throws IOException {

            if (message.getFiles() != null) {
                for (FileDetails file : message.getFiles()) {
                    if (null != file) {
                        output.writeObject(1, file, FileDetails.getSchema(), true);
                    }
                }
            }

            output.writeUInt64(2, message.getOperationTimeMS(), false);

            List<String> messages = message.getMessages();
            if (messages != null) {
                for (String msg : messages) {
                    if (msg != null)
                        output.writeString(3, msg, true);
                }
            }

            List<QueryExceptionType> exceptions = message.getExceptions();
            if (exceptions != null) {
                for (QueryExceptionType exception : exceptions) {
                    if (exception != null)
                        output.writeObject(4, exception, QueryExceptionType.getSchema(), true);
                }
            }

        }

        public void mergeFrom(Input input, FileResponseList message) throws IOException {
            List<FileDetails> files = null;
            LinkedList<QueryExceptionType> exceptions = null;
            int number;
            while ((number = input.readFieldNumber(this)) != 0) {
                switch (number) {
                    case 1:
                        if (files == null)
                            files = new ArrayList<FileDetails>();
                        files.add(input.mergeObject(null, FileDetails.getSchema()));
                        break;
                    case 2:
                        message.setOperationTimeMS(input.readUInt64());
                        break;
                    case 3:
                        message.addMessage(input.readString());
                        break;
                    case 4:
                        if (exceptions == null)
                            exceptions = new LinkedList<QueryExceptionType>();
                        exceptions.add(input.mergeObject(null, QueryExceptionType.getSchema()));
                        break;
                    default:
                        input.handleUnknownField(number, this);
                        break;
                }
            }
            if (exceptions != null)
                message.setExceptions(exceptions);
            if (files != null)
                message.setFiles(files);
        }

        public String getFieldName(int number) {
            switch (number) {
                case 1:
                    return "files";
                case 2:
                    return "operationTimeMs";
                case 3:
                    return "messages";
                case 4:
                    return "exceptions";
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
            fieldMap.put("files", 1);
            fieldMap.put("operationTimeMs", 2);
            fieldMap.put("messages", 3);
            fieldMap.put("exceptions", 4);
        }
    };
}
