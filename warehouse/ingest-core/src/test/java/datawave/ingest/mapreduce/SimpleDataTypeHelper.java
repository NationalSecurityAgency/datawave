package datawave.ingest.mapreduce;

import com.google.common.collect.Multimap;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.ingest.IngestHelperInterface;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

/**
 * A mechanism for injecting a DataTypeHandler into unit tests. Here's how it works.
 * <p/>
 * 1) Register the fields you would like this to return using SimpleDataTypeHandler.registerFields(fields); 2) Create an instance of this using
 * SimpleDataTypeHandler.create(); 3) When helper.getFields() is invoked, it will return your registered fields.
 * <p/>
 * This uses proxying b/c implementing the whole interface was overkill for the current use case.
 */
public class SimpleDataTypeHelper implements InvocationHandler {
    
    private static Multimap<String,NormalizedContentInterface> fields;
    
    /**
     * Register a new set of fields for instances of SimpleDataTypeHelpers to return.
     *
     * @param f
     *            fields
     */
    public static void registerFields(Multimap<String,NormalizedContentInterface> f) {
        fields = f;
    }
    
    /**
     * Creates a new ingest helper
     *
     * @return ingest helper
     */
    public static IngestHelperInterface create() {
        ClassLoader cl = SimpleDataTypeHelper.class.getClassLoader();
        return (IngestHelperInterface) Proxy.newProxyInstance(cl, new Class[] {IngestHelperInterface.class}, new SimpleDataTypeHelper());
    }
    
    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        if (method.getName().equals("getEventFields")) {
            return fields;
        } else {
            throw new UnsupportedOperationException("Sorry, " + this.getClass() + " does not currently support the " + method.getName()
                            + " method. Feel free to implement it!");
        }
    }
}
