package datawave.webservice.util;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;

import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyWriter;
import javax.ws.rs.ext.Provider;

import com.fasterxml.jackson.core.io.IOContext;
import com.fasterxml.jackson.core.json.UTF8JsonGenerator;

import io.protostuff.JsonIOUtil;
import io.protostuff.LinkedBuffer;
import io.protostuff.Message;
import io.protostuff.ProtobufIOUtil;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.XmlIOUtil;
import io.protostuff.YamlIOUtil;
import io.protostuff.runtime.RuntimeSchema;

/**
 * A message body writer than can output using the Protostuff library. We only support serializing messages that implement {@link Message}.
 */
@Provider
@Produces({"text/yaml", "text/x-yaml", "application/x-yaml", "application/x-protobuf", "application/x-protostuff"})
public class ProtostuffMessageBodyWriter implements MessageBodyWriter<Object> {

    private LinkedBuffer buffer = LinkedBuffer.allocate(4096);

    @Override
    public long getSize(Object message, Class<?> clazz, Type type, Annotation[] annotations, MediaType media) {
        // -1 means size unknown
        return -1;
    }

    @Override
    public boolean isWriteable(Class<?> clazz, Type type, Annotation[] annotations, MediaType media) {
        return Message.class.isAssignableFrom(clazz);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void writeTo(Object message, Class<?> clazz, Type type, Annotation[] annotations, MediaType media, MultivaluedMap<String,Object> httpHeaders,
                    OutputStream out) throws IOException, WebApplicationException {

        // TODO: Figure out a method to add the proto file location in the response headers.
        // This map must be mofified before any data is written to out,
        // since at that time the response headers will be flushed.

        Schema<Object> schema = null;
        if (message instanceof Message) {
            Message<Object> msg = (Message<Object>) message;
            schema = msg.cachedSchema();
        } else {
            schema = (Schema<Object>) RuntimeSchema.getSchema(clazz);
        }

        try {
            if (MediaType.APPLICATION_XML_TYPE.equals(media) || MediaType.TEXT_XML_TYPE.equals(media)) {
                XmlIOUtil.writeTo(out, message, schema);
            } else if ("text/yaml".equals(media.toString()) || "text/x-yaml".equals(media.toString()) || "application/x-yaml".equals(media.toString())) {
                YamlIOUtil.writeTo(out, message, schema, buffer);
            } else if ("application/x-protobuf".equals(media.toString())) {
                ProtobufIOUtil.writeTo(out, message, schema, buffer);
            } else if ("application/x-protostuff".equals(media.toString())) {
                ProtostuffIOUtil.writeTo(out, message, schema, buffer);
            } else if (MediaType.APPLICATION_JSON_TYPE.equals(media)) {
                IOContext ctx = new IOContext(JsonIOUtil.DEFAULT_JSON_FACTORY._getBufferRecycler(), out, false);
                UTF8JsonGenerator generator = new UTF8JsonGenerator(ctx, JsonIOUtil.DEFAULT_JSON_FACTORY.getGeneratorFeatures(),
                                JsonIOUtil.DEFAULT_JSON_FACTORY.getCodec(), out);
                try {
                    JsonIOUtil.writeTo(generator, message, schema, false);
                } finally {
                    generator.close();
                }
            }
        } finally {
            buffer.clear();
        }
    }
}
