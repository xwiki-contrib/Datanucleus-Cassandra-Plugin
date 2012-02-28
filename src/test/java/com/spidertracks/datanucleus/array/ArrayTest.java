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

package com.spidertracks.datanucleus.array;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import javax.jdo.JDODataStoreException;
import javax.jdo.PersistenceManager;
import javax.jdo.Transaction;

import org.datanucleus.exceptions.NucleusObjectNotFoundException;
import org.junit.Test;

import com.eaio.uuid.UUID;
import com.spidertracks.datanucleus.CassandraTest;
import com.spidertracks.datanucleus.array.model.CardArray;
import com.spidertracks.datanucleus.array.model.PackArray;
import com.spidertracks.datanucleus.collection.model.Card;
import com.spidertracks.datanucleus.collection.model.Pack;

/**
 * @author Todd Nine
 * 
 */

public class ArrayTest extends CassandraTest {

    @Test
    public void testBasicPeristAndLoadOneToManyArray() throws Exception {

        PackArray pack = new PackArray();

        CardArray aceSpades = new CardArray();
        aceSpades.setName("Ace of Spades");
        pack.getCards()[0] = (aceSpades);

        CardArray jackHearts = new CardArray();
        jackHearts.setName("Jack of Hearts");
        pack.getCards()[1] = jackHearts;

        pmf.getPersistenceManager().makePersistent(pack);

        PackArray saved = pmf.getPersistenceManager().getObjectById(
                PackArray.class, pack.getId());

        assertEquals(pack, saved);

        assertNotNull(saved.getCards());

        assertTrue(saved.getCards()[0].equals(aceSpades));

        assertTrue(saved.getCards()[1].equals(jackHearts));

    }

    @Test
    public void testDeleteMap() throws Exception {

        PackArray pack = new PackArray();

        CardArray aceSpades = new CardArray();
        aceSpades.setName("Ace of Spades");
        pack.getCards()[0] = (aceSpades);

        CardArray jackHearts = new CardArray();
        jackHearts.setName("Jack of Hearts");
        pack.getCards()[1] = jackHearts;

        final PersistenceManager pm1 = pmf.getPersistenceManager();
        pm1.makePersistent(pack);

        PersistenceManager pm = pmf.getPersistenceManager();
        PackArray saved = pm.getObjectById(PackArray.class, pack.getId());

        assertEquals(pack, saved);

        assertNotNull(saved.getCards());

        assertTrue(saved.getCards()[0].equals(aceSpades));

        assertTrue(saved.getCards()[1].equals(jackHearts));
        
        UUID packId = pack.getId();
        UUID aceId = aceSpades.getId();
        UUID jackId = jackHearts.getId();


        pm.deletePersistent(saved);

    
        boolean deleted = false;

        try {
            pmf.getPersistenceManager().getObjectById(Pack.class, packId);
        } catch (JDODataStoreException n) {
            deleted = n.getCause() instanceof NucleusObjectNotFoundException;
        }

        assertTrue(deleted);

        deleted = false;

        // now check the cards are gone as well
        try {
            pmf.getPersistenceManager().getObjectById(Card.class, aceId);
        } catch (JDODataStoreException n) {
            deleted = n.getCause() instanceof NucleusObjectNotFoundException;
        }

        assertTrue(deleted);

        deleted = false;
        try {
            pmf.getPersistenceManager().getObjectById(Card.class, jackId);

        } catch (JDODataStoreException n) {
            deleted = n.getCause() instanceof NucleusObjectNotFoundException;
        }

        assertTrue(deleted);

        deleted = false;

    }

}
