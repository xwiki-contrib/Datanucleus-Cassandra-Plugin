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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import javax.jdo.JDODataStoreException;
import javax.jdo.PersistenceManager;
import javax.jdo.Transaction;

import org.datanucleus.exceptions.NucleusObjectNotFoundException;
import org.junit.Test;

import com.eaio.uuid.UUID;
import com.spidertracks.datanucleus.CassandraTest;
import com.spidertracks.datanucleus.basic.model.InvitationToken;
import com.spidertracks.datanucleus.basic.model.InvitedPerson;

/**
 * Tests for 2 objects that are bi-directionally dependent and default fetch group deleting properly
 * 
 * @author Todd Nine
 * 
 */
public class CascadeTest extends CassandraTest {




    /**
     * We should never hit 30 seconds unless we're stuck in endless recursion
     * @throws Exception
     */
    @Test
    public void testDelete() throws Exception {
        InvitedPerson person = new InvitedPerson();
        person.setFirstName("firstName");
        person.setLastName("lastName");
        person.setLoginCount(10);
        
        InvitationToken token = new InvitationToken();
        token.setToken("testtoken");
        
        person.setToken(token);
        token.setPerson(person);
        
        //now persist them.
        
        pmf.getPersistenceManager().makePersistent(person);
        
        
        UUID personId = person.getId();
        String tokenKey = token.getToken();
        
        PersistenceManager pm = pmf.getPersistenceManager();
        Transaction trans = pm.currentTransaction();
        trans.begin();
        
        InvitedPerson saved = pm.getObjectById(InvitedPerson.class, personId);
        
        assertNotNull(saved);
        assertNotNull(saved.getToken());
        
        
        pm.deletePersistent(saved);
        trans.commit();
        
        boolean deleted = false;

        try {
            pmf.getPersistenceManager().getObjectById(InvitedPerson.class, personId);
        } catch (JDODataStoreException n) {
            deleted = n.getCause() instanceof NucleusObjectNotFoundException;
        }

        assertTrue(deleted);

        deleted = false;

        // now check the cards are gone as well
        try {
            pmf.getPersistenceManager().getObjectById(InvitationToken.class, tokenKey);
        } catch (JDODataStoreException n) {
            deleted = n.getCause() instanceof NucleusObjectNotFoundException;
        }

        assertTrue(deleted);

        
        
        
    }


}
