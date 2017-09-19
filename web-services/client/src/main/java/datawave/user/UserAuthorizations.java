package datawave.user;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import io.protostuff.Input;
import io.protostuff.Message;
import io.protostuff.Output;
import io.protostuff.Schema;

/**
 * A simple JAXB result holder for user authorizations. This class is intended to contain a string of ACCUMULO user authorizations.
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.NONE)
public class UserAuthorizations implements Message<UserAuthorizations>, Serializable {
    private static final long serialVersionUID = 1L;
    
    @XmlElement(name = "auth")
    private TreeSet<String> auths;
    
    public UserAuthorizations() {
        auths = new TreeSet<String>();
    }
    
    public UserAuthorizations(String... auths) {
        this.auths = new TreeSet<String>(Arrays.asList(auths));
    }
    
    public UserAuthorizations(Set<String> auths) {
        this.auths = new TreeSet<String>(auths);
    }
    
    public Set<String> getUserAuthorizations() {
        return auths;
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (String auth : auths) {
            if (sb.length() > 0)
                sb.append(',');
            sb.append(auth);
        }
        return sb.toString();
    }
    
    public static Schema<UserAuthorizations> getSchema() {
        return SCHEMA;
    }
    
    @Override
    public Schema<UserAuthorizations> cachedSchema() {
        return SCHEMA;
    }
    
    @XmlTransient
    private static final Schema<UserAuthorizations> SCHEMA = new Schema<UserAuthorizations>() {
        public UserAuthorizations newMessage() {
            return new UserAuthorizations();
        }
        
        public Class<UserAuthorizations> typeClass() {
            return UserAuthorizations.class;
        }
        
        public String messageName() {
            return UserAuthorizations.class.getSimpleName();
        }
        
        public String messageFullName() {
            return UserAuthorizations.class.getName();
        }
        
        public boolean isInitialized(UserAuthorizations message) {
            return true;
        }
        
        public void writeTo(Output output, UserAuthorizations message) throws IOException {
            if (message.auths != null) {
                for (String auths : message.auths) {
                    if (auths != null)
                        output.writeString(1, auths, true);
                }
            }
        }
        
        public void mergeFrom(Input input, UserAuthorizations message) throws IOException {
            int number;
            while ((number = input.readFieldNumber(this)) != 0) {
                switch (number) {
                    case 1:
                        if (message.auths == null || message.auths == Collections.EMPTY_SET)
                            message.auths = new TreeSet<String>();
                        message.auths.add(input.readString());
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
                    return "auths";
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
            fieldMap.put("auths", 1);
        }
    };
}
