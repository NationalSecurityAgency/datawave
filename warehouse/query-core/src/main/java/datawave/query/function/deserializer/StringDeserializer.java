package datawave.query.function.deserializer;

import datawave.query.attributes.Document;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class StringDeserializer extends DocumentDeserializer{
    int i=0;

    public StringDeserializer(){
        System.out.println("Constructed");
    }
    @Override
    public Document deserialize(InputStream inputStream) {
        try {
            String result = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
            if (++i == 1){
                System.out.println("result is " + result);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return new Document();
    }
}


