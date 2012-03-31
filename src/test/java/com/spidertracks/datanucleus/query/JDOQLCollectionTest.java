/**********************************************************************
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
package com.spidertracks.datanucleus.query;

import java.util.List;

import javax.jdo.PersistenceManager;
import javax.jdo.Query;
import javax.jdo.Transaction;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.spidertracks.datanucleus.CassandraTest;
import com.spidertracks.datanucleus.collection.model.Pack;
import com.spidertracks.datanucleus.collection.model.Card;
import com.spidertracks.datanucleus.collection.model.Case;
import com.spidertracks.datanucleus.collection.model.Beer;
import com.spidertracks.datanucleus.model.BaseEntity;

public class JDOQLCollectionTest extends CassandraTest
{
    private PersistenceManager setupPm;

    @Before
    public void setUp() throws Exception
    {
        this.setupPm = pmf.getPersistenceManager();
    }

    @After
    public void tearDown() throws Exception {
        final Transaction tx = setupPm.currentTransaction();
        tx.begin();

        setupPm.newQuery(Pack.class).deletePersistentAll();
        setupPm.newQuery(Case.class).deletePersistentAll();

        tx.commit();
    }

    @Test
    public void testContains()
    {
        final Card aceOfSpades = new Card("aceOfSpades");
        final Card queenOfHearts = new Card("queenOfHearts");
        final Card joker = new Card("joker");
        final Card blackjack = new Card("blackjack");

        final Pack pack1 = new Pack();
        pack1.addCard(aceOfSpades);
        pack1.addCard(queenOfHearts);

        final Pack pack2 = new Pack();
        pack2.addCard(joker);
        pack2.addCard(blackjack);

        Transaction tx = setupPm.currentTransaction();
        tx.begin();
        setupPm.makePersistent(pack1);
        setupPm.makePersistent(pack2);
        tx.commit();

        final PersistenceManager pm = pmf.getPersistenceManager();
        tx = pm.currentTransaction();
        tx.begin();

        final Query query =
            pm.newQuery("SELECT FROM com.spidertracks.datanucleus.collection.model.Pack "
                      + "WHERE cards.contains(card) && card.name == \"blackjack\" "
                      + "VARIABLES com.spidertracks.datanucleus.collection.model.Card card");

        final List<Pack> results = (List<Pack>) query.execute();

        Assert.assertEquals(1, results.size());
        Assert.assertTrue(results.get(0).equals(pack2));

        tx.commit();
    }

    @Test
    public void testContainsSuperclassTable()
    {
        final Beer ale = new Beer("ale");
        final Beer lager = new Beer("lager");
        final Beer pilsner = new Beer("pilsner");
        final Beer stout = new Beer("stout");

        final Case dark = new Case();
        dark.addBeer(lager);
        dark.addBeer(stout);

        final Case light = new Case();
        light.addBeer(ale);
        light.addBeer(pilsner);

        Transaction tx = setupPm.currentTransaction();
        tx.begin();
        setupPm.makePersistent(dark);
        setupPm.makePersistent(light);
        tx.commit();

        final PersistenceManager pm = pmf.getPersistenceManager();
        tx = pm.currentTransaction();
        tx.begin();

        final Query query =
            pm.newQuery("SELECT FROM com.spidertracks.datanucleus.collection.model.Case "
                      + "WHERE beers.contains(b) && b.name == \"lager\" "
                      + "VARIABLES com.spidertracks.datanucleus.collection.model.Beer b");

        final List<Case> results = (List<Case>) query.execute();

        Assert.assertEquals(1, results.size());
        Assert.assertTrue(results.get(0).equals(dark));

        tx.commit();
    }
}
