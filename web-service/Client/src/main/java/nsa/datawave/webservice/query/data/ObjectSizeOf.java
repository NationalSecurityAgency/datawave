package nsa.datawave.webservice.query.data;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashSet;
import java.util.Set;
import java.util.Stack;

import org.apache.log4j.Logger;

/**
 * A simple interface that objects can implement to return the object size.
 */
public interface ObjectSizeOf {
    /**
     * The (approximate) size of the object
     */
    long sizeInBytes();
    
    public static class ObjectInstance {
        private Object o;
        
        public ObjectInstance(Object _o) {
            this.o = _o;
        }
        
        public Object getObject() {
            return o;
        }
        
        @Override
        public int hashCode() {
            return System.identityHashCode(o);
        }
        
        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof ObjectInstance)) {
                return false;
            }
            return ((ObjectInstance) obj).o == this.o;
        }
    }
    
    public static class Sizer {
        private static final Logger log = Logger.getLogger(Sizer.class);
        public static short OBJECT_OVERHEAD = 8;
        public static short ARRAY_OVERHEAD = 12;
        public static short REFERENCE = 4;
        // The size of the basic Number constructs (and Boolean and Character) is 16: roundUp(8 + primitiveSize)
        public static short NUMBER_SIZE = 16;
        
        /**
         * Get the size of an object. Note that we want something relatively fast that gives us an order of magnitude here. The java Instrumentation agent
         * mechanism is a little too costly for general use here. This will look for the ObjectSizeOf interface and if implemented on the object will use that.
         * Otherwise it will do a simple navigation of the fields using reflection.
         * 
         * @param o
         * @return an approximation of the object size
         */
        public static long getObjectSize(Object o) {
            long estSize = getObjectSize(o, new HashSet<ObjectInstance>(), new Stack<ObjectInstance>(), true);
            return estSize;
        }
        
        public static long getObjectSize(Object o, Set<ObjectInstance> visited, Stack<ObjectInstance> stack, boolean useSizeInBytesMethod) {
            long totalSize = 0;
            stack.add(new ObjectInstance(o));
            while (!stack.isEmpty()) {
                long size = 0;
                ObjectInstance oi = stack.pop();
                o = oi.getObject();
                if (o != null && !visited.contains(oi)) {
                    visited.add(oi);
                    try {
                        if (useSizeInBytesMethod) {
                            try {
                                if (o instanceof ObjectSizeOf) {
                                    size = ((ObjectSizeOf) o).sizeInBytes();
                                } else {
                                    Method sizeInBytes = o.getClass().getMethod("sizeInBytes", (Class<?>[]) null);
                                    size = (Long) sizeInBytes.invoke(o);
                                }
                            } catch (Throwable t) {
                                // ok, lets do this the hard way...
                            }
                        }
                        if (size == 0) {
                            // the hard way...
                            // do not include Class related objects or reflection objects
                            if ((o instanceof Class) || (o instanceof ClassLoader)
                                            || (o.getClass().getPackage() != null && o.getClass().getPackage().getName().startsWith("java.lang.reflect"))) {
                                size = 0;
                            } else if (o instanceof Number || o instanceof Boolean || o instanceof Character) {
                                size = NUMBER_SIZE;
                            } else {
                                // lets do a simple sizing
                                Class<?> c = o.getClass();
                                if (c.isArray()) {
                                    size = ARRAY_OVERHEAD;
                                    int length = Array.getLength(o);
                                    if (c.getComponentType().isPrimitive()) {
                                        size += length * getPrimitiveObjectSize(c.getComponentType());
                                    } else {
                                        size += length * REFERENCE;
                                        for (int i = 0; i < length; i++) {
                                            Object element = Array.get(o, i);
                                            if (element != null) {
                                                stack.add(new ObjectInstance(element));
                                            }
                                        }
                                    }
                                } else {
                                    size += OBJECT_OVERHEAD;
                                    while (c != null) {
                                        for (Field field : c.getDeclaredFields()) {
                                            if (Modifier.isStatic(field.getModifiers())) {
                                                continue;
                                            }
                                            if (field.getType().isPrimitive()) {
                                                size += getPrimitiveObjectSize(field.getType());
                                            } else {
                                                size += REFERENCE;
                                                boolean accessible = field.isAccessible();
                                                field.setAccessible(true);
                                                try {
                                                    Object fieldObject = field.get(o);
                                                    if (fieldObject != null) {
                                                        stack.push(new ObjectInstance(fieldObject));
                                                    }
                                                } catch (Exception e) {
                                                    // cannot get to field, so ignore it in this size calculation
                                                    e.printStackTrace();
                                                }
                                                field.setAccessible(accessible);
                                            }
                                        }
                                        c = c.getSuperclass();
                                    }
                                }
                                size = roundUp(size);
                            }
                        }
                    } catch (Throwable t) {
                        log.error("Unable to determine object size for " + o);
                    }
                }
                totalSize += size;
            }
            
            return totalSize;
        }
        
        public static long roundUp(long size) {
            long extra = size % 8;
            if (extra > 0) {
                size = size + 8 - extra;
            }
            return size;
        }
        
        public static short getPrimitiveObjectSize(Class<?> primitiveType) {
            if (primitiveType.equals(int.class) || primitiveType.equals(float.class)) {
                return 4;
            } else if (primitiveType.equals(boolean.class) || primitiveType.equals(byte.class)) {
                return 1;
            } else if (primitiveType.equals(char.class) || primitiveType.equals(short.class)) {
                return 2;
            } else if (primitiveType.equals(long.class) || primitiveType.equals(double.class)) {
                return 8;
            } else { // if (primitiveType.equals(void.class)) {
                return 0;
            }
        }
    }
    
}
