/**********************************************************************
Copyright (c) 2010 Todd Nine. All rights reserved.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Contributors :
    ...
 ***********************************************************************/

package com.spidertracks.datanucleus.basic;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.UUID;

import javax.jdo.PersistenceManager;

import org.junit.Test;

import com.spidertracks.datanucleus.CassandraTest;
import com.spidertracks.datanucleus.model.CompositeEntity;
import com.spidertracks.datanucleus.model.CompositeKey;
import com.spidertracks.datanucleus.model.LexicalUUIDEntity;
import com.spidertracks.datanucleus.model.LongEntity;

/**
 * @author Todd Nine
 * 
 * TODO test 2 primitives A and B with bidirectional link and defaultFetchGroup="true"
 */

public class ByteMapperTest extends CassandraTest {

    @Test
    public void testLongEntityLoad() throws Exception {

        PersistenceManager pm = pmf.getPersistenceManager();

        LongEntity object = new LongEntity();
        object.setTestVal("testVal");
        object.setId(100l);
        
        
        // now save our object
        pm.makePersistent(object);

        // don't want it to come from the cache, get a new pm
        PersistenceManager pm2 = pmf.getPersistenceManager();

        // now retrieve a copy
        LongEntity stored = (LongEntity) pm2.getObjectById(
                LongEntity.class, object.getId());

        // make sure they're not the same instance, we want a new one from the
        // data source
        assertFalse(object == stored);

        assertEquals(object.getId(), stored.getId());

        assertEquals(object.getTestVal(), stored.getTestVal());


    }

    
    @Test
    public void testLexicalUUIDEntityLoad() throws Exception {

        PersistenceManager pm = pmf.getPersistenceManager();

        LexicalUUIDEntity object = new LexicalUUIDEntity();
        object.setTestVal("testVal");
        object.setId(UUID.randomUUID());
        
        
        // now save our object
        pm.makePersistent(object);

        // don't want it to come from the cache, get a new pm
        PersistenceManager pm2 = pmf.getPersistenceManager();

        // now retrieve a copy
        LexicalUUIDEntity stored = (LexicalUUIDEntity) pm2.getObjectById(
                LexicalUUIDEntity.class, object.getId());

        // make sure they're not the same instance, we want a new one from the
        // data source
        assertFalse(object == stored);

        assertEquals(object.getId(), stored.getId());

        assertEquals(object.getTestVal(), stored.getTestVal());


    }
    

    @Test
    public void testCompositeEntityLoad() throws Exception {

        PersistenceManager pm = pmf.getPersistenceManager();

        
        CompositeKey key = new CompositeKey();
        key.setFirst(0);
        key.setSecond(100);
        key.setThird(300);
        
        CompositeEntity object = new CompositeEntity();
        object.setTestVal("testVal");
        object.setId(key);
        
        
        
        // now save our object
        pm.makePersistent(object);

        // don't want it to come from the cache, get a new pm
        PersistenceManager pm2 = pmf.getPersistenceManager();

        // now retrieve a copy
        CompositeEntity stored = (CompositeEntity) pm2.getObjectById(
                CompositeEntity.class, object.getId());

        // make sure they're not the same instance, we want a new one from the
        // data source
        assertFalse(object == stored);

        assertEquals(object.getId(), stored.getId());

        assertEquals(object.getTestVal(), stored.getTestVal());


    }


}
