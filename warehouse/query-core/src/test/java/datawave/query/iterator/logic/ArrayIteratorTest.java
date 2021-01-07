package datawave.query.iterator.logic;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.NoSuchElementException;

public class ArrayIteratorTest {
    @Before
    public void setup() {
        
    }
    
    @Test
    public void emptyTest() {
        ArrayIterator<String> itr = new ArrayIterator<>(new String[] {});
        itr.initialize();
        
        Assert.assertEquals(false, itr.hasNext());
    }
    
    @Test
    public void sortTest() {
        ArrayIterator<String> itr = new ArrayIterator<>(new String[] {"z", "y", "x"});
        itr.initialize();
        
        Assert.assertEquals(true, itr.hasNext());
        Assert.assertEquals("x", itr.next());
        Assert.assertEquals("y", itr.next());
        Assert.assertEquals("z", itr.next());
        Assert.assertEquals(false, itr.hasNext());
    }
    
    @Test
    public void hasNextIdempotentTest() {
        ArrayIterator<String> itr = new ArrayIterator<>(new String[] {"a", "b"});
        itr.initialize();
        
        Assert.assertEquals(true, itr.hasNext());
        Assert.assertEquals(true, itr.hasNext());
        Assert.assertEquals(true, itr.hasNext());
        Assert.assertEquals(true, itr.hasNext());
        
        Assert.assertEquals("a", itr.next());
        
        Assert.assertEquals(true, itr.hasNext());
        Assert.assertEquals(true, itr.hasNext());
        Assert.assertEquals(true, itr.hasNext());
        Assert.assertEquals(true, itr.hasNext());
        
        Assert.assertEquals("b", itr.next());
        
        Assert.assertEquals(false, itr.hasNext());
        Assert.assertEquals(false, itr.hasNext());
        Assert.assertEquals(false, itr.hasNext());
        Assert.assertEquals(false, itr.hasNext());
    }
    
    @Test(expected = NoSuchElementException.class)
    public void nextBoundaryTest() {
        ArrayIterator<String> itr = new ArrayIterator<>(new String[] {"a", "b"});
        itr.initialize();
        
        Assert.assertEquals("a", itr.next());
        Assert.assertEquals("b", itr.next());
        Assert.assertEquals(null, itr.next());
    }
    
    @Test(expected = IllegalStateException.class)
    public void moveBackwardsTest() {
        ArrayIterator<String> itr = new ArrayIterator<>(new String[] {"a", "b", "c"});
        itr.initialize();
        
        itr.next();
        itr.next();
        
        Assert.assertEquals("a", itr.move("a"));
    }
    
    @Test
    public void moveFirstTest() {
        ArrayIterator<String> itr = new ArrayIterator<>(new String[] {"a", "b", "c"});
        itr.initialize();
        
        Assert.assertEquals("a", itr.move("a"));
    }
    
    @Test
    public void moveNextTest() {
        ArrayIterator<String> itr = new ArrayIterator<>(new String[] {"a", "b", "c"});
        itr.initialize();
        
        itr.next();
        
        Assert.assertEquals("b", itr.move("b"));
    }
    
    @Test
    public void moveLastTest() {
        ArrayIterator<String> itr = new ArrayIterator<>(new String[] {"a", "b", "c"});
        itr.initialize();
        
        itr.next();
        
        Assert.assertEquals("c", itr.move("c"));
    }
    
    @Test
    public void moveEmptyTest() {
        ArrayIterator<String> itr = new ArrayIterator<>(new String[] {"a", "b", "c"});
        itr.initialize();
        
        itr.next();
        
        Assert.assertEquals(null, itr.move("d"));
    }
    
    @Test
    public void peekTest() {
        ArrayIterator<String> itr = new ArrayIterator<>(new String[] {"a", "b", "c"});
        itr.initialize();
        
        Assert.assertEquals("a", itr.peek());
        Assert.assertEquals("a", itr.peek());
        Assert.assertEquals("a", itr.peek());
        
        itr.next();
        
        Assert.assertEquals("b", itr.peek());
        Assert.assertEquals("b", itr.peek());
        Assert.assertEquals("b", itr.peek());
        
        itr.next();
        
        Assert.assertEquals("c", itr.peek());
        Assert.assertEquals("c", itr.peek());
        Assert.assertEquals("c", itr.peek());
        
        itr.next();
        
        Assert.assertEquals(null, itr.peek());
        Assert.assertEquals(null, itr.peek());
        Assert.assertEquals(null, itr.peek());
    }
}
