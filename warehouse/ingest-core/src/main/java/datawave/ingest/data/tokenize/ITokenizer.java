package datawave.ingest.data.tokenize;

import java.io.IOException;

import org.apache.lucene.util.Attribute;

/**
 * Simple interface for tokenizers
 */
public interface ITokenizer {
    /**
     * 
     * @return true if a new token is available
     * @throws IOException
     *             if there is a problem
     */
    boolean incrementToken() throws IOException;
    
    /**
     * 
     * @param clazz
     *            the attribute class
     * @return true if the tokenizer has that attribute available
     */
    boolean hasAttribute(Class<? extends Attribute> clazz);
    
    /**
     * @param <A>
     *            type of Attribute
     * @param clazz
     *            the attribute class
     * @return the Attribute for the specified class
     */
    <A extends Attribute> A getAttribute(Class<A> clazz);
    
    /**
     * Free resources associated with this Tokenizer
     * 
     * @throws IOException
     *             if there is a problem
     */
    void close() throws IOException;
}
