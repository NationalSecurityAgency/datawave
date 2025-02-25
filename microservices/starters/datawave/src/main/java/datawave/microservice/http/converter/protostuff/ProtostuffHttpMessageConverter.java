package datawave.microservice.http.converter.protostuff;

import static org.springframework.util.Assert.state;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

import org.springframework.http.HttpInputMessage;
import org.springframework.http.HttpOutputMessage;
import org.springframework.http.MediaType;
import org.springframework.http.converter.AbstractHttpMessageConverter;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.http.converter.HttpMessageNotWritableException;
import org.springframework.lang.NonNull;

import io.protostuff.LinkedBuffer;
import io.protostuff.Message;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;

/**
 * An {@link org.springframework.http.converter.HttpMessageConverter} that reads/writes messages that implement the Protostuff {@link Message} interface.
 */
public class ProtostuffHttpMessageConverter extends AbstractHttpMessageConverter<Message<?>> {
    public static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;
    public static final MediaType PROTOSTUFF = new MediaType("application", "x-protostuff", DEFAULT_CHARSET);
    public static final String PROTOSTUFF_VALUE = "application/x-protostuff";
    
    private ThreadLocal<LinkedBuffer> buffer = ThreadLocal.withInitial(() -> LinkedBuffer.allocate(4096));
    
    public ProtostuffHttpMessageConverter() {
        setSupportedMediaTypes(Collections.singletonList(PROTOSTUFF));
    }
    
    @Override
    protected boolean supports(@NonNull Class<?> clazz) {
        return Message.class.isAssignableFrom(clazz);
    }
    
    @Override
    protected MediaType getDefaultContentType(Message<?> message) {
        return PROTOSTUFF;
    }
    
    @Override
    protected @NonNull Message<?> readInternal(@NonNull Class<? extends Message<?>> clazz, @NonNull HttpInputMessage inputMessage)
                    throws IOException, HttpMessageNotReadableException {
        MediaType contentType = inputMessage.getHeaders().getContentType();
        if (contentType == null) {
            contentType = PROTOSTUFF;
        }
        
        if (PROTOSTUFF.isCompatibleWith(contentType)) {
            try {
                // noinspection unchecked
                Message<Object> msg = (Message<Object>) clazz.newInstance();
                ProtostuffIOUtil.mergeFrom(inputMessage.getBody(), msg, msg.cachedSchema(), buffer.get());
                return msg;
            } catch (InstantiationException | IllegalAccessException e) {
                throw new HttpMessageNotReadableException("Unable to read protostuff message: " + e.getMessage(), e, inputMessage);
            }
        }
        
        throw new UnsupportedOperationException("Reading protostuff messages from media types other than " + PROTOSTUFF + " is not supported.");
    }
    
    @Override
    protected void writeInternal(@NonNull Message<?> message, @NonNull HttpOutputMessage outputMessage) throws IOException, HttpMessageNotWritableException {
        MediaType contentType = outputMessage.getHeaders().getContentType();
        if (contentType == null) {
            contentType = getDefaultContentType(message);
            state(contentType != null, "No content type");
        }
        
        try {
            @SuppressWarnings("unchecked")
            Schema<Object> schema = (Schema<Object>) message.cachedSchema();
            if (PROTOSTUFF.isCompatibleWith(contentType)) {
                ProtostuffIOUtil.writeTo(outputMessage.getBody(), message, schema, buffer.get());
            }
        } finally {
            buffer.get().clear();
        }
    }
}
