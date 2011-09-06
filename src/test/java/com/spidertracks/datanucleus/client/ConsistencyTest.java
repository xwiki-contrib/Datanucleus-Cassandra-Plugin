package com.spidertracks.datanucleus.client;

import static org.junit.Assert.assertEquals;

import org.apache.cassandra.thrift.ConsistencyLevel;
import org.junit.Test;

/**
 * Tests basic consistency ops
 * @author Todd Nine
 *
 */
public class ConsistencyTest {

    @Test
    public void testSet() throws InterruptedException{
        Thread one = new Thread(new TestThread(100, ConsistencyLevel.ONE));
        
        Thread two = new Thread(new TestThread(100, ConsistencyLevel.LOCAL_QUORUM));
        
        Thread three = new Thread(new TestThread(100, ConsistencyLevel.ALL));
        
        one.run();
        two.run();
        three.run();
        
        one.join();
        two.join();
        three.join();
        
        
        
    }
    
    
    private class TestThread implements Runnable{

        private int count;
        private ConsistencyLevel level;
        
        public TestThread(int count, ConsistencyLevel level){
            this.count = count;
            this.level = level;
        }
        
        @Override
        public void run() {
            Consistency.set(level);
            
            for(int i = 0; i < count; i ++){
                assertEquals(level, Consistency.get());
                
            }
            
        }
        
    }
}
