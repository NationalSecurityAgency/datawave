package datawave.webservice.query.result.event;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;

import javax.xml.bind.annotation.XmlAccessOrder;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorOrder;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlTransient;

import org.apache.commons.lang.builder.EqualsBuilder;

import io.protostuff.Input;
import io.protostuff.Message;
import io.protostuff.Output;
import io.protostuff.Schema;

@XmlAccessorType(XmlAccessType.NONE)
@XmlAccessorOrder(XmlAccessOrder.ALPHABETICAL)
public class Metadata implements Serializable, Message<Metadata> {

    private static final long serialVersionUID = 1L;

    @XmlElement(name = "DataType")
    private String dataType = null;

    @XmlElement(name = "InternalId")
    private String internalId = null;

    @XmlElement(name = "Table")
    private String table = null;

    @XmlElement(name = "Row")
    private String row = null;

    public String getDataType() {
        return dataType;
    }

    public String getInternalId() {
        return internalId;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public void setInternalId(String internalId) {
        this.internalId = internalId;
    }

    public String getTable() {
        return table;
    }

    public String getRow() {
        return row;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public void setRow(String row) {
        this.row = row;
    }

    @Override
    public boolean equals(Object o) {
        Metadata om = (Metadata) o;
        EqualsBuilder builder = new EqualsBuilder();
        builder.append(dataType, om.dataType);
        builder.append(internalId, om.internalId);
        builder.append(row, om.row);
        builder.append(table, om.table);
        return builder.isEquals();
    }

    @Override
    public int hashCode() {
        int result = dataType != null ? dataType.hashCode() : 0;
        result = 31 * result + (internalId != null ? internalId.hashCode() : 0);
        result = 31 * result + (table != null ? table.hashCode() : 0);
        result = 31 * result + (row != null ? row.hashCode() : 0);
        return result;
    }

    public static Schema<Metadata> getSchema() {
        return SCHEMA;
    }

    @Override
    public Schema<Metadata> cachedSchema() {
        return SCHEMA;
    }

    @XmlTransient
    private static final Schema<Metadata> SCHEMA = new Schema<Metadata>() {

        @Override
        public Metadata newMessage() {
            return new Metadata();
        }

        @Override
        public Class<? super Metadata> typeClass() {
            return Metadata.class;
        }

        @Override
        public String messageName() {
            return Metadata.class.getSimpleName();
        }

        @Override
        public String messageFullName() {
            return Metadata.class.getName();
        }

        @Override
        public boolean isInitialized(Metadata message) {
            return true;
        }

        @Override
        public void writeTo(Output output, Metadata message) throws IOException {
            if (message.dataType != null)
                output.writeString(1, message.dataType, false);
            if (message.internalId != null)
                output.writeString(2, message.internalId, false);
            if (message.table != null)
                output.writeString(3, message.table, false);
            if (message.row != null)
                output.writeString(4, message.row, false);
        }

        @Override
        public void mergeFrom(Input input, Metadata message) throws IOException {
            int number;
            while ((number = input.readFieldNumber(this)) != 0) {
                switch (number) {
                    case 1:
                        message.dataType = input.readString();
                        break;
                    case 2:
                        message.internalId = input.readString();
                        break;
                    case 3:
                        message.table = input.readString();
                        break;
                    case 4:
                        message.row = input.readString();
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
                    return "dataType";
                case 2:
                    return "internalId";
                case 3:
                    return "table";
                case 4:
                    return "row";
                default:
                    return null;
            }
        }

        @Override
        public int getFieldNumber(String name) {
            final Integer number = fieldMap.get(name);
            return number == null ? 0 : number.intValue();
        }

        private final HashMap<String,Integer> fieldMap = new HashMap<String,Integer>();
        {
            fieldMap.put("dataType", 1);
            fieldMap.put("internalId", 2);
            fieldMap.put("table", 3);
            fieldMap.put("row", 4);
        }
    };
}
