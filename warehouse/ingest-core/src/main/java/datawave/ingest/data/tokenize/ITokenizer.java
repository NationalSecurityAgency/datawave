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
     */
    public boolean incrementToken() throws IOException;
    
    /**
     * 
     * @param clazz
     *            the attribute class
     * @return true if the tokenizer has that attribute available
     */
    public boolean hasAttribute(Class<? extends Attribute> clazz);
    
    /**
     * 
     * @param clazz
     *            the attribute class
     * @return the Attribute for the specified class
     */
    public <A extends Attribute> A getAttribute(Class<A> clazz);
    
    /**
     * Free resources associated with this Tokenizer
     */
    public void close() throws IOException;
}
