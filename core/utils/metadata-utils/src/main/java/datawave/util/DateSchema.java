package datawave.util;

import java.io.IOException;
import java.util.Date;

import io.protostuff.Input;
import io.protostuff.Output;
import io.protostuff.Schema;

/**
 * This schema can be used by the protostuff api to serialize/deserialize a date object, internally represented as a long
 */
public class DateSchema implements Schema<Date> {
    private static final String DATE = "dateMillis";
    
    @Override
    public String getFieldName(int number) {
        switch (number) {
            case 1:
                return DATE;
            default:
                return null;
        }
    }
    
    @Override
    public int getFieldNumber(String name) {
        switch (name) {
            case DATE:
                return 1;
            default:
                return 0;
        }
    }
    
    @Override
    public boolean isInitialized(Date date) {
        return true;
    }
    
    @Override
    public Date newMessage() {
        return new Date();
    }
    
    @Override
    public String messageName() {
        return Date.class.getSimpleName();
    }
    
    @Override
    public String messageFullName() {
        return Date.class.getName();
    }
    
    @Override
    public Class<? super Date> typeClass() {
        return Date.class;
    }
    
    @Override
    public void mergeFrom(Input input, Date date) throws IOException {
        for (int number = input.readFieldNumber(this);; number = input.readFieldNumber(this)) {
            switch (number) {
                case 0:
                    return;
                case 1:
                    date.setTime(Long.parseLong(input.readString()));
                    break;
                default:
                    input.handleUnknownField(number, this);
            }
        }
    }
    
    @Override
    public void writeTo(Output output, Date date) throws IOException {
        if (date != null)
            output.writeString(1, Long.toString(date.getTime()), false);
    }
}
