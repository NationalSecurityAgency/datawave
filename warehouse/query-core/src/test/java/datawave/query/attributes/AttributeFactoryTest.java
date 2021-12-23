package datawave.query.attributes;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.NoOpType;
import datawave.data.type.NumberType;
import datawave.data.type.PointType;
import datawave.query.function.deserializer.DocumentDeserializer;
import datawave.query.function.deserializer.KryoDocumentDeserializer;
import datawave.query.function.serializer.DocumentSerializer;
import datawave.query.function.serializer.KryoDocumentSerializer;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;
import java.util.Map;

/**
 *
 */
public class AttributeFactoryTest {
    
    private static final Logger log = Logger.getLogger(AttributeFactoryTest.class);
    
    Collection<Class<?>> one = Sets.<Class<?>> newHashSet(LcNoDiacriticsType.class);
    Collection<Class<?>> two = Sets.<Class<?>> newHashSet(NoOpType.class, LcNoDiacriticsType.class);
    Collection<Class<?>> three = Sets.<Class<?>> newHashSet(NoOpType.class, LcNoDiacriticsType.class, NumberType.class);
    Collection<Class<?>> four = Sets.<Class<?>> newHashSet(LcNoDiacriticsType.class, NumberType.class);
    Collection<Class<?>> five = Sets.<Class<?>> newHashSet(NoOpType.class, NumberType.class);
    
    Collection<Class<?>> expectedOne = Sets.<Class<?>> newHashSet(LcNoDiacriticsType.class);
    Collection<Class<?>> expectedTwo = Sets.<Class<?>> newHashSet(LcNoDiacriticsType.class);
    Collection<Class<?>> expectedThree = Sets.<Class<?>> newHashSet(NumberType.class);
    Collection<Class<?>> expectedFour = Sets.<Class<?>> newHashSet(NumberType.class);
    Collection<Class<?>> expectedFive = Sets.<Class<?>> newHashSet(NumberType.class);
    
    @Test
    public void testFindersKeepers() {
        log.debug(AttributeFactory.getKeepers(one));
        log.debug(AttributeFactory.getKeepers(two));
        log.debug(AttributeFactory.getKeepers(three));
        log.debug(AttributeFactory.getKeepers(four));
        log.debug(AttributeFactory.getKeepers(five));
        
        Assert.assertEquals(AttributeFactory.getKeepers(one), expectedOne);
        Assert.assertEquals(AttributeFactory.getKeepers(two), expectedTwo);
        Assert.assertEquals(AttributeFactory.getKeepers(three), expectedThree);
        Assert.assertEquals(AttributeFactory.getKeepers(four), expectedFour);
        Assert.assertEquals(AttributeFactory.getKeepers(five), expectedFive);
        
    }


    @Test
    public void trytry() throws InstantiationException, IllegalAccessException {

        DocumentSerializer ser = new KryoDocumentSerializer();
        DocumentDeserializer deser = new KryoDocumentDeserializer();
        Key key = new Key();

        Document doc = new Document();

        for(int i=0; i < 25; i++) {
            //doc.put("blah" + i, new TypeAttribute(new NumberType("234"),key,true));
            doc.put("blah" + i, new TypeAttribute(new NoOpType("234"),key,true));
            //PointType p = new PointType();
            //p.setDelegateFromString("POINT (1 2)");
            //doc.put("blah" + i, new TypeAttribute(p,key,true));
        }
        long ts = System.currentTimeMillis();
        for(int i=0; i < 500000; i++) {
            Map.Entry<Key, Value> res = ser.apply(Maps.immutableEntry(key,doc));
            deser.apply(res);
        }
        System.out.println("that took " + (System.currentTimeMillis() - ts) + " ms " );
    }
}
