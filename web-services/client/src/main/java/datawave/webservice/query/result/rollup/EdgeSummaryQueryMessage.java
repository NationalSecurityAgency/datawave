package datawave.webservice.query.result.rollup;

import io.protostuff.Input;
import io.protostuff.Message;
import io.protostuff.Output;
import io.protostuff.Schema;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.XmlType;

/**
 * This JAXB bean represents some message returned by the iterator or query logic for a selector profile query. It can be used for conveying warnings, messages,
 * etc. back to the user. The web tier will collect these messages and present them in the default "messages" section of the response.
 *
 */
@XmlAccessorType(XmlAccessType.NONE)
@XmlType(propOrder = {"scanLimitHit", "queryLogicWarning"})
public class EdgeSummaryQueryMessage implements Serializable, Message<EdgeSummaryQueryMessage> {

    private static final long serialVersionUID = -7921311709917138744L;

    @XmlElement(name = "scanLimitHit", nillable = false)
    private boolean scanLimitHit;

    @XmlElement(name = "queryLogicWarning", nillable = false)
    private String queryLogicWarning;

    public boolean isScanLimitHit() {
        return scanLimitHit;
    }

    public void setScanLimitHit(boolean scanLimitHit) {
        this.scanLimitHit = scanLimitHit;
    }

    public String getQueryLogicWarning() {
        return queryLogicWarning;
    }

    public void setQueryLogicWarning(String queryLogicWarning) {
        this.queryLogicWarning = queryLogicWarning;
    }

    public static Schema<EdgeSummaryQueryMessage> getSchema() {
        return SCHEMA;
    }

    @Override
    public Schema<EdgeSummaryQueryMessage> cachedSchema() {
        return SCHEMA;
    }

    @XmlTransient
    private static final Schema<EdgeSummaryQueryMessage> SCHEMA = new Schema<EdgeSummaryQueryMessage>() {

        @Override
        public EdgeSummaryQueryMessage newMessage() {
            return new EdgeSummaryQueryMessage();
        }

        @Override
        public Class<? super EdgeSummaryQueryMessage> typeClass() {
            return EdgeSummaryQueryMessage.class;
        }

        @Override
        public String messageName() {
            return EdgeSummaryQueryMessage.class.getSimpleName();
        }

        @Override
        public String messageFullName() {
            return EdgeSummaryQueryMessage.class.getName();
        }

        @Override
        public boolean isInitialized(EdgeSummaryQueryMessage message) {
            return true;
        }

        @Override
        public void writeTo(Output output, EdgeSummaryQueryMessage message) throws IOException {
            output.writeBool(1, message.scanLimitHit, false);
            output.writeString(2, message.queryLogicWarning, false);
        }

        @Override
        public void mergeFrom(Input input, EdgeSummaryQueryMessage message) throws IOException {
            int number;
            while ((number = input.readFieldNumber(this)) != 0) {
                switch (number) {
                    case 1:
                        message.scanLimitHit = input.readBool();
                        break;
                    case 2:
                        message.queryLogicWarning = input.readString();
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
                    return "scanLimitHit";
                case 2:
                    return "queryLogicWarning";
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
            fieldMap.put("scanLimitHit", 1);
            fieldMap.put("queryLogicWarning", 2);
        }
    };
}
