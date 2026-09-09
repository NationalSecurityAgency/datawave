package datawave.marking;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlTransient;

import org.apache.accumulo.access.Access;
import org.apache.accumulo.access.AccessExpression;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.protostuff.Input;
import io.protostuff.Output;
import io.protostuff.Schema;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@XmlAccessorType(XmlAccessType.NONE)
@JsonAutoDetect(getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE)
public class AccessExpressionMarkings implements Markings<AccessExpression> {
    public static final Access ACCESS = Access.builder().build();

    public static final String COLUMN_VISIBILITY_KEY = "columnVisibility";

    private AccessExpression accessExpression;

    @Override
    public AccessExpression getMarkings() {
        return getAccessExpression();
    }

    @Override
    public AccessExpression toAccessExpression() {
        return accessExpression;
    }

    @Override
    public ColumnVisibility toColumnVisibility() {
        return AccessExpressionUtil.toColumnVisibility(AccessExpressionUtil.normalize(accessExpression));
    }

    @Override
    public boolean isEmpty() {
        return accessExpression == null || accessExpression.getExpression().isEmpty();
    }

    @JsonProperty(COLUMN_VISIBILITY_KEY)
    @XmlElement(name = COLUMN_VISIBILITY_KEY)
    public String getColumnVisibilityString() {
        return accessExpression != null ? accessExpression.getExpression() : null;
    }

    // required for JAXB unmarshalling
    public void setColumnVisibilityString(String ae) {
        this.accessExpression = ae != null ? ACCESS.newExpression(ae) : null;
    }

    @JsonCreator
    public static AccessExpressionMarkings create(@JsonProperty(COLUMN_VISIBILITY_KEY) String cv) {
        if (cv != null && !cv.isEmpty()) {
            return AccessExpressionMarkings.builder().columnVisibility(cv).build();
        }
        return null;
    }

    public static AccessExpressionMarkings createMarkings(ColumnVisibility cv) {
        if (null == cv) {
            return null;
        }
        return create(new String(cv.getExpression(), StandardCharsets.UTF_8));
    }

    public static AccessExpressionMarkings createMarkings(AccessExpression ae) {
        if (null == ae) {
            return null;
        }
        return create(ae.getExpression());
    }

    public static AccessExpressionMarkings createMarkings(Text cv) {
        if (null == cv) {
            return null;
        }
        return create(cv.toString());
    }

    public static AccessExpressionMarkings createMarkings(String cv) {
        if (null == cv) {
            return null;
        }
        return create(cv);
    }

    public static class AccessExpressionMarkingsBuilder {
        public AccessExpressionMarkingsBuilder columnVisibility(String cv) {
            this.accessExpression = cv != null ? ACCESS.newExpression(cv) : null;
            return this;
        }
    }

    @XmlTransient
    public static final Schema<AccessExpressionMarkings> SCHEMA = new Schema<>() {
        private final HashMap<String,Integer> fieldMap = new HashMap<>();
        {
            fieldMap.put(COLUMN_VISIBILITY_KEY, 1);
        }

        @Override
        public String getFieldName(int number) {
            if (number == 1) {
                return COLUMN_VISIBILITY_KEY;
            }
            return null;
        }

        @Override
        public int getFieldNumber(String name) {
            final Integer number = fieldMap.get(name);
            return number == null ? 0 : number;
        }

        @Override
        public boolean isInitialized(AccessExpressionMarkings message) {
            return true;
        }

        @Override
        public AccessExpressionMarkings newMessage() {
            return AccessExpressionMarkings.builder().build();
        }

        @Override
        public String messageName() {
            return AccessExpressionMarkings.class.getSimpleName();
        }

        @Override
        public String messageFullName() {
            return AccessExpressionMarkings.class.getName();
        }

        @Override
        public Class<? super AccessExpressionMarkings> typeClass() {
            return AccessExpressionMarkings.class;
        }

        @Override
        public void mergeFrom(Input input, AccessExpressionMarkings message) throws IOException {
            int number;
            while ((number = input.readFieldNumber(this)) != 0) {
                if (number == 1) {
                    String expr = input.readString();
                    if (expr != null && !expr.isEmpty()) {
                        message.accessExpression = ACCESS.newExpression(expr);
                    }
                } else {
                    input.handleUnknownField(number, this);
                }
            }
        }

        @Override
        public void writeTo(Output output, AccessExpressionMarkings message) throws IOException {
            if (message.accessExpression != null) {
                output.writeString(1, message.accessExpression.getExpression(), false);
            }
        }

    };
}
