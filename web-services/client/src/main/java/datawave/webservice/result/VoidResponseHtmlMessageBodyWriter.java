package datawave.webservice.result;

import datawave.webservice.query.exception.QueryExceptionType;
import datawave.webservice.util.AbstractHtmlProviderMessageBodyWriter;

import javax.ws.rs.core.MediaType;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.List;

public class VoidResponseHtmlMessageBodyWriter extends AbstractHtmlProviderMessageBodyWriter<VoidResponse> {
    @Override
    public boolean isWriteable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return VoidResponse.class.isAssignableFrom(type);
    }

    @Override
    public String getTitle(VoidResponse voidResponse) {
        return "Void Response";
    }

    @Override
    public String getHeadContent(VoidResponse voidResponse) {
        return "";
    }

    @Override
    public String getPageHeader(VoidResponse voidResponse) {
        return VoidResponse.class.getName();
    }

    public static final String BR = "<br/>";

    @Override
    public String getMainContent(VoidResponse voidResponse) {
        StringBuilder builder = new StringBuilder();
        builder.append("<b>MESSAGES:</b>").append(BR);
        List<String> messages = voidResponse.getMessages();
        if (messages != null) {
            for (String msg : messages) {
                if (msg != null)
                    builder.append(msg).append(BR);
            }
        }

        builder.append("<b>EXCEPTIONS:</b>").append(BR);
        List<QueryExceptionType> exceptions = voidResponse.getExceptions();
        if (exceptions != null) {
            for (QueryExceptionType exception : exceptions) {
                if (exception != null)
                    builder.append(exception).append(", ").append(QueryExceptionType.getSchema()).append(BR);
            }
        }
        return builder.toString();
    }
}
