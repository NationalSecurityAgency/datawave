package datawave.microservice.http.converter.html;

import static java.nio.charset.StandardCharsets.UTF_8;

import static org.springframework.util.Assert.state;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.springframework.http.HttpOutputMessage;
import org.springframework.http.MediaType;
import org.springframework.http.converter.HttpMessageNotWritableException;
import org.springframework.lang.Nullable;

import datawave.microservice.config.web.DatawaveServerProperties;
import datawave.webservice.query.exception.QueryExceptionType;
import datawave.webservice.result.VoidResponse;

/**
 * A {@link org.springframework.http.converter.HttpMessageConverter} that writes a {@link VoidResponse} to HTML. This class does not support reading HTML and
 * converting to an {@link VoidResponse}.
 */
public class VoidResponseHttpMessageConverter extends AbstractDatawaveHttpMessageConverter<VoidResponse> {
    
    public VoidResponseHttpMessageConverter(DatawaveServerProperties datawaveServerProperties, @Nullable BannerProvider bannerProvider) {
        super(datawaveServerProperties, bannerProvider);
        setSupportedMediaTypes(Arrays.asList(MediaType.TEXT_HTML, MediaType.TEXT_PLAIN));
    }
    
    @Override
    protected void writeInternal(VoidResponse voidResponse, HttpOutputMessage outputMessage) throws IOException, HttpMessageNotWritableException {
        MediaType contentType = outputMessage.getHeaders().getContentType();
        if (contentType == null) {
            contentType = getDefaultContentType(voidResponse);
            state(contentType != null, "No content type");
        }
        
        if (MediaType.TEXT_PLAIN.isCompatibleWith(contentType)) {
            outputMessage.getBody().write(voidResponse.toString().getBytes(UTF_8));
        } else {
            super.writeInternal(voidResponse, outputMessage);
        }
    }
    
    @Override
    protected boolean supports(Class<?> clazz) {
        return VoidResponse.class.isAssignableFrom(clazz);
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
