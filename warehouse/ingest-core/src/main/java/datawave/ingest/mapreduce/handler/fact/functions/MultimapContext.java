package datawave.ingest.mapreduce.handler.fact.functions;

import org.apache.commons.jexl3.JexlContext;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

/**
 *
 */
public class MultimapContext implements JexlContext {

    protected Multimap<String,Object> values;

    public MultimapContext() {

        values = ArrayListMultimap.create();
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.commons.jexl3.JexlContext#get(java.lang.String)
     */
    @Override
    public Object get(String field) {
        return values.get(field);
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.commons.jexl3.JexlContext#has(java.lang.String)
     */
    @Override
    public boolean has(String arg0) {
        return values.containsKey(arg0);
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.commons.jexl3.JexlContext#set(java.lang.String, java.lang.Object)
     */
    @Override
    public void set(String field, Object value) {
        values.put(field, value);
    }

}
