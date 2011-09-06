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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Date;

import javax.jdo.JDODataStoreException;
import javax.jdo.JDOException;
import javax.jdo.PersistenceManager;

import org.datanucleus.exceptions.NucleusDataStoreException;
import org.junit.Ignore;
import org.junit.Test;

import com.eaio.uuid.UUID;
import com.spidertracks.datanucleus.CassandraTest;
import com.spidertracks.datanucleus.basic.converter.EnumConverter;
import com.spidertracks.datanucleus.basic.model.EmbeddedObject;
import com.spidertracks.datanucleus.basic.model.EnumEntity;
import com.spidertracks.datanucleus.basic.model.EnumValues;
import com.spidertracks.datanucleus.basic.model.PrimitiveObject;
import com.spidertracks.datanucleus.basic.model.PrimitiveObjectSubclass;
import com.spidertracks.datanucleus.basic.model.UnitData;
import com.spidertracks.datanucleus.basic.model.UnitDataKey;
import com.spidertracks.datanucleus.basic.model.UnitDataNoConverter;
import com.spidertracks.datanucleus.basic.model.UnitDataNoConverterKey;
import com.spidertracks.datanucleus.convert.ByteConverterContext;
import com.spidertracks.datanucleus.identity.ByteAware;

/**
 * @author Todd Nine
 * 
 *         TODO test 2 primitives A and B with bidirectional link and
 *         defaultFetchGroup="true"
 */

public class PrimitiveTest extends CassandraTest {

    @Test
    public void testBasicPeristAndLoad() throws Exception {

        PersistenceManager pm = pmf.getPersistenceManager();

        PrimitiveObject object = new PrimitiveObject();
        object.setTestByte((byte) 0xf1);
        object.setTestBool(true);
        object.setTestChar('t');
        object.setTestDouble(100.10);
        object.setTestFloat((float) 200.20);
        object.setTestInt(40);
        object.setTestLong(200);
        object.setTestShort((short) 5);
        object.setTestString("foobar");

        // now save our object
        pm.makePersistent(object);

        // don't want it to come from the cache, get a new pm
        PersistenceManager pm2 = pmf.getPersistenceManager();

        // now retrieve a copy
        PrimitiveObject stored = (PrimitiveObject) pm2.getObjectById(
                PrimitiveObject.class, object.getId());

        // make sure they're not the same instance, we want a new one from the
        // data source
        assertFalse(object == stored);

        assertEquals(object.getId(), stored.getId());

        assertEquals(object.getTestByte(), stored.getTestByte());

        assertEquals(object.isTestBool(), stored.isTestBool());

        assertEquals(object.getTestChar(), stored.getTestChar());

        assertEquals(object.getTestDouble(), stored.getTestDouble(), 0);

        assertEquals(object.getTestFloat(), stored.getTestFloat(), 0);

        assertEquals(object.getTestInt(), stored.getTestInt());

        assertEquals(object.getTestLong(), stored.getTestLong());

        assertEquals(object.getTestString(), stored.getTestString());

    }

    /**
     * Tests an object is serialized as bytes properly
     */
    @Test
    public void testCompositeKey() {

        UnitData unitData = new UnitData();
        
        UnitDataKey key = new UnitDataKey(new Date(), "123456");
        
        unitData.setKey(key);

        PersistenceManager pm = pmf.getPersistenceManager();

        pm.makePersistent(unitData);

        PersistenceManager pm2 = pmf.getPersistenceManager();

        // now retrieve a copy
        UnitData stored = (UnitData) pm2.getObjectById(UnitData.class,
                new UnitDataKey(key.getCreatedDate(), key.getUnitId()));

        assertEquals(unitData, stored);

    }

    /**
     * Tests an object is serialized as bytes properly
     */
    @Test
    public void testCompositeKeyNoConverter() {

        UnitDataNoConverter data = new UnitDataNoConverter();
        
        UnitDataNoConverterKey key = new UnitDataNoConverterKey(new Date(), "123456");
        data.setKey(key);
        
        PersistenceManager pm = pmf.getPersistenceManager();

        try {
            pm.makePersistent(data);
        } catch (Exception e) {
            
            Throwable current = e;
            
            while(!(current instanceof NucleusDataStoreException) && current != null){
                current = current.getCause();
            }
            
            assertNotNull(current);
            
            assertEquals(String.format("You cannot use the default serializer on a key.  See the %s to defined your own converter or %s interface to use custom keys",
                    ByteConverterContext.class.getName(), ByteAware.class.getName()), current.getMessage());
            
            return;
            
        }
        
        fail("An exception should have been thrown");

    }

    /**
     * Tests an object is serialized as bytes properly
     */
    @Test
    public void testByteArray() {


        UnitData unitData = new UnitData();
        
        UnitDataKey key = new UnitDataKey(new Date(), "123456");
        
        unitData.setKey(key);


        unitData.setData(new byte[] { 0x0F, 0x0A });

        PersistenceManager pm = pmf.getPersistenceManager();

        pm.makePersistent(unitData);

        PersistenceManager pm2 = pmf.getPersistenceManager();

        // now retrieve a copy
        UnitData stored = (UnitData) pm2.getObjectById(UnitData.class,
                new UnitDataKey(key.getCreatedDate(), key.getUnitId()));

        assertEquals(unitData, stored);

        assertArrayEquals(unitData.getData(), stored.getData());

    }

    /**
     * Tests an object is serialized as bytes properly
     */
    @Test(expected = JDOException.class)
    public void testEmbeddedObject() {

        EmbeddedObject object = new EmbeddedObject();

        pmf.getPersistenceManager().makePersistent(object);

    }

    /**
     * Tests an object is serialized as string types properly
     */
    public void testObjectToStringConverterOnObject() {

        EnumEntity saved = new EnumEntity();

        saved.setFirst(EnumValues.ONE);

        saved.setSecond(EnumValues.TWO);

        // check our converter has never been invoked
        assertEquals(0, EnumConverter.getFromCount());

        assertEquals(0, EnumConverter.getToCount());

        pmf.getPersistenceManager().makePersistent(saved);

        EnumEntity returned = pmf.getPersistenceManager().getObjectById(
                EnumEntity.class, saved.getId());

        assertEquals(saved, returned);

        assertEquals(saved.getFirst(), returned.getFirst());

        assertEquals(saved.getSecond(), returned.getSecond());

        // now check our counters are correct If they aren't the user's
        // converter plugins weren't invoked
        assertTrue(EnumConverter.getFromCount() >= 2);

        assertTrue(EnumConverter.getToCount() >= 2);

    }

    @Test(expected = JDODataStoreException.class)
    public void testDelete() throws Exception {

        PersistenceManager pm = pmf.getPersistenceManager();

        PrimitiveObject object = new PrimitiveObject();
        object.setTestByte((byte) 0xf1);
        object.setTestBool(true);
        object.setTestChar('t');

        // now save our object
        pm.makePersistent(object);

        // don't want it to come from the cache, get a new pm
        PersistenceManager pm2 = pmf.getPersistenceManager();

        // now retrieve a copy
        PrimitiveObject stored = (PrimitiveObject) pm2.getObjectById(
                PrimitiveObject.class, object.getId());

        // make sure they're not the same instance, we want a new one from the
        // data source
        assertFalse(object == stored);

        assertEquals(object.getId(), stored.getId());

        assertEquals(object.getTestByte(), stored.getTestByte());

        assertEquals(object.isTestBool(), stored.isTestBool());

        assertEquals(object.getTestChar(), stored.getTestChar());

        assertEquals(object.getTestDouble(), stored.getTestDouble(), 0);

        assertEquals(object.getTestFloat(), stored.getTestFloat(), 0);

        assertEquals(object.getTestInt(), stored.getTestInt());

        assertEquals(object.getTestLong(), stored.getTestLong());

        assertEquals(object.getTestString(), stored.getTestString());

        // now delete our object
        pm2.deletePersistent(stored);

        PersistenceManager pm3 = pmf.getPersistenceManager();

        // should throw an exception
        pm3.getObjectById(
                PrimitiveObject.class, object.getId());

    }

    // @Test
    @Ignore("Waiting to hear back from Andy at datanuclues.  Not sure if this is a valid test")
    public void subClassReturned() {

        PrimitiveObject primitive = new PrimitiveObject();

        primitive.setTestByte((byte) 0xf1);
        primitive.setTestBool(true);
        primitive.setTestChar('t');
        primitive.setTestDouble(100.10);
        primitive.setTestFloat((float) 200.20);
        primitive.setTestInt(40);
        primitive.setTestLong(200);
        primitive.setTestShort((short) 5);
        primitive.setTestString("foobar");

        PrimitiveObjectSubclass subclass = new PrimitiveObjectSubclass();

        subclass.setTestByte((byte) 0xf1);
        subclass.setTestBool(true);
        subclass.setTestChar('t');
        subclass.setTestDouble(100.10);
        subclass.setTestFloat((float) 200.20);
        subclass.setTestInt(40);
        subclass.setTestLong(200);
        subclass.setTestShort((short) 5);
        subclass.setTestString("foobar");
        subclass.setSubClassString("subclassString");

        PersistenceManager pm = pmf.getPersistenceManager();

        pm.makePersistent(primitive);
        pm.makePersistent(subclass);

        UUID primitiveId = primitive.getId();
        UUID subclassId = subclass.getId();

        PersistenceManager pm2 = pmf.getPersistenceManager();

        PrimitiveObject subclassInstance = pm2.getObjectById(
                PrimitiveObject.class, subclassId);

        boolean correctInstance = subclassInstance instanceof PrimitiveObjectSubclass;

        assertTrue(correctInstance);

        PrimitiveObject instance = pm2.getObjectById(PrimitiveObject.class,
                primitiveId);

        correctInstance = instance instanceof PrimitiveObject;

        assertTrue(correctInstance);

    }

}
