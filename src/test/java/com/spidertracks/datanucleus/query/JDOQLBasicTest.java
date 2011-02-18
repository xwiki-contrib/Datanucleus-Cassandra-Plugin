/**********************************************************************
Copyright (c) 2008 Erik Bengtson and others. All rights reserved.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Calendar;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import javax.jdo.JDODataStoreException;
import javax.jdo.PersistenceManager;
import javax.jdo.Query;
import javax.jdo.Transaction;

import org.apache.cassandra.thrift.ConsistencyLevel;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.spidertracks.datanucleus.CassandraTest;
import com.spidertracks.datanucleus.basic.inheritance.casefour.Search;
import com.spidertracks.datanucleus.basic.inheritance.casefour.SearchOne;
import com.spidertracks.datanucleus.basic.inheritance.casefour.SearchThree;
import com.spidertracks.datanucleus.basic.inheritance.casefour.SearchTwo;
import com.spidertracks.datanucleus.basic.inheritance.recursive.Child;
import com.spidertracks.datanucleus.basic.inheritance.recursive.ChildTwo;
import com.spidertracks.datanucleus.basic.inheritance.recursive.GrandChildTwoOne;
import com.spidertracks.datanucleus.basic.inheritance.recursive.GrandChildTwoTwo;
import com.spidertracks.datanucleus.basic.model.InvitationToken;
import com.spidertracks.datanucleus.basic.model.Person;
import com.spidertracks.datanucleus.basic.model.PrimitiveObject;
import com.spidertracks.datanucleus.client.Consistency;

public class JDOQLBasicTest extends CassandraTest {

	Object[] id = new Object[3];

	private PrimitiveObject object1;
	private PrimitiveObject object2;
	private PrimitiveObject object3;
	private PersistenceManager setupPm;

	private Person p1;
	private Person p2;
	private Person p3;
	private Person p4;
	private Person p5;

	@Before
	public void setUp() throws Exception {
		
	
		setupPm = pmf.getPersistenceManager();

		Transaction tx = setupPm.currentTransaction();
		tx.begin();

		object1 = new PrimitiveObject();
		object1.setTestByte((byte) 0xf1);
		object1.setTestBool(true);
		object1.setTestChar('1');
		object1.setTestDouble(100.10);
		object1.setTestFloat((float) 100.10);
		object1.setTestInt(10);
		object1.setTestLong(100);
		object1.setTestShort((short) 1);
		object1.setTestString("one");

		setupPm.makePersistent(object1);

		object2 = new PrimitiveObject();
		object2.setTestByte((byte) 0xf1);
		object2.setTestBool(true);
		object2.setTestChar('2');
		object2.setTestDouble(200.20);
		object2.setTestFloat((float) 200.20);
		object2.setTestInt(20);
		object2.setTestLong(200);
		object2.setTestShort((short) 2);
		object2.setTestString("two");

		setupPm.makePersistent(object2);

		object3 = new PrimitiveObject();
		object3.setTestByte((byte) 0xf1);
		object3.setTestBool(true);
		object3.setTestChar('3');
		object3.setTestDouble(300.30);
		object3.setTestFloat((float) 300.30);
		object3.setTestInt(30);
		object3.setTestLong(300);
		object3.setTestShort((short) 3);
		object3.setTestString("three");

		setupPm.makePersistent(object3);

		Calendar cal = Calendar.getInstance();
		cal.add(Calendar.DATE, -20);

		p1 = new Person();
		p1.setEmail("p1@test.com");
		p1.setFirstName("firstName1");
		p1.setLastName("lastName1");
		p1.setLastLogin(cal.getTime());

		cal.add(Calendar.DATE, 2);

		p2 = new Person();
		p2.setEmail("p2@test.com");
		p2.setFirstName("firstName1");
		p2.setLastName("secondName1");
		p2.setLastLogin(cal.getTime());

		cal.add(Calendar.DATE, 2);

		p3 = new Person();
		p3.setEmail("p3@test.com");
		p3.setFirstName("firstName1");
		p3.setLastName("secondName2");
		p3.setLastLogin(cal.getTime());

		cal.add(Calendar.DATE, 2);

		p4 = new Person();
		p4.setEmail("p4@test.com");
		p4.setFirstName("firstName2");
		p4.setLastName("secondName2");
		p4.setLastLogin(cal.getTime());

		cal.add(Calendar.DATE, 2);

		p5 = new Person();
		p5.setEmail("p5@test.com");
		p5.setFirstName("firstName3");
		p5.setLastName("secondName3");
		p5.setLastLogin(cal.getTime());

		// now persist everything

		setupPm.makePersistent(p1);
		setupPm.makePersistent(p2);
		setupPm.makePersistent(p3);
		setupPm.makePersistent(p4);
		setupPm.makePersistent(p5);

		tx.commit();

	}

	@After
	public void tearDown() throws Exception {
		Transaction tx = setupPm.currentTransaction();
		tx.begin();

		setupPm.deletePersistent(object1);
		setupPm.deletePersistent(object2);
		setupPm.deletePersistent(object3);

		setupPm.deletePersistent(p1);
		setupPm.deletePersistent(p2);
		setupPm.deletePersistent(p3);
		setupPm.deletePersistent(p4);
		setupPm.deletePersistent(p5);

		tx.commit();

	}

	/**
	 * Runs basic query extent
	 */
	@SuppressWarnings("rawtypes")
	@Test
	public void testExtent() {
		PersistenceManager pm = pmf.getPersistenceManager();
		Transaction tx = pm.currentTransaction();
		try {
			tx.begin();
			Iterator it = pm.getExtent(PrimitiveObject.class).iterator();
			assertTrue(it.hasNext());
			it.next();
			assertTrue(it.hasNext());
			it.next();
			assertTrue(it.hasNext());
			tx.commit();
		} finally {
			if (tx.isActive()) {
				tx.rollback();
			}
			pm.close();
		}
	}

	/**
	 * Runs basic query
	 */
	@SuppressWarnings("rawtypes")
	public void testBasicQuery() {
		PersistenceManager pm = pmf.getPersistenceManager();
		Transaction tx = pm.currentTransaction();
		try {
			tx.begin();
			Collection c = (Collection) pm.newQuery(PrimitiveObject.class)
					.execute();
			assertEquals(3, c.size());
			tx.commit();

		} finally {
			if (tx.isActive()) {
				tx.rollback();
			}
			pm.close();
		}
	}

	/**
	 * ordering
	 */
	@SuppressWarnings("rawtypes")
	@Test
	public void testOrdering() {
		PersistenceManager pm = pmf.getPersistenceManager();
		Transaction tx = pm.currentTransaction();
		try {
			tx.begin();
			Query q = pm.newQuery(PrimitiveObject.class);
			q.setOrdering("testString DESC, testDouble");
			Collection c = (Collection) q.execute();
			assertTrue(c.size() >= 3);
			Iterator it = c.iterator();
			assertEquals("two", ((PrimitiveObject) it.next()).getTestString());
			assertEquals("three", ((PrimitiveObject) it.next()).getTestString());
			assertEquals("one", ((PrimitiveObject) it.next()).getTestString());
			tx.commit();
		} finally {
			if (tx.isActive()) {
				tx.rollback();
			}
			pm.close();
		}
	}

	/**
	 * result test
	 */
	@SuppressWarnings("rawtypes")
	@Test
	public void testFilter() {
		PersistenceManager pm = pmf.getPersistenceManager();
		Transaction tx = pm.currentTransaction();
		try {
			tx.begin();
			Query q = pm.newQuery(PrimitiveObject.class);
			q.setFilter("testString == 'one'");
			Collection c = (Collection) q.execute();
			assertEquals(1, c.size());
			Iterator it = c.iterator();
			assertEquals("one", ((PrimitiveObject) it.next()).getTestString());
			tx.commit();
		} finally {
			if (tx.isActive()) {
				tx.rollback();
			}
			pm.close();
		}
	}

	/**
	 * Test query with parameters (NUCCORE-205)
	 */
	@SuppressWarnings("rawtypes")
	@Test
	public void testFilterWithParameters() {
		PersistenceManager pm = pmf.getPersistenceManager();
		try {
			// declare query an run with the first parameter
			pm.currentTransaction().begin();
			Query q = pm.newQuery(PrimitiveObject.class);
			q.setFilter("testString == s1");
			q.declareParameters("java.lang.String s1");
			Collection c = (Collection) q.execute("one");
			assertEquals(1, c.size());
			Iterator it = c.iterator();
			assertEquals("one", ((PrimitiveObject) it.next()).getTestString());
			pm.currentTransaction().commit();

			// declare same query an run with another parameter
			pm.currentTransaction().begin();
			q = pm.newQuery(PrimitiveObject.class);
			q.setFilter("testString == s1");
			q.declareParameters("java.lang.String s1");
			c = (Collection) q.execute("xyz");
			assertEquals(0, c.size());
			pm.currentTransaction().commit();

		} finally {
			if (pm.currentTransaction().isActive()) {
				pm.currentTransaction().rollback();
			}
			pm.close();
		}
	}

	/**
	 * Query returning an object with relation fields, testing the contents of
	 * the relation fields.
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void testEqual() {
		// now perform our select. We want everyone with firstname =
		// "firstName1"
		PersistenceManager pm = pmf.getPersistenceManager();

		Query query = pm.newQuery(Person.class);
		query.setFilter("firstName == :fN");

		List<Person> results = (List<Person>) query.execute(p1.getFirstName());
		// check we got p1, p2 and p3.

		assertEquals(3, results.size());

		assertTrue(results.contains(p1));
		assertTrue(results.contains(p2));
		assertTrue(results.contains(p3));

	}

	@SuppressWarnings("unchecked")
	@Test
	public void testEqualStringId() throws Exception {

		// now perform our select. We want everyone with firstname =
		// "firstName1"
		PersistenceManager pm = pmf.getPersistenceManager();
		Transaction trans = pm.currentTransaction();
		trans.begin();

		InvitationToken token = new InvitationToken();
		token.setToken("testKey");
		token.setTestString("testIndexedString");

		pm.makePersistent(token);

		trans.commit();

		List<InvitationToken> results = (List<InvitationToken>) pm.newQuery(
				InvitationToken.class).execute();

		assertTrue(results.contains(token));

	}

	/**
	 * Query returning an object with relation fields, testing the contents of
	 * the relation fields.
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void testEqualNoValue() {
		// now perform our select. We want everyone with firstname =
		// "firstName1"
		PersistenceManager pm = pmf.getPersistenceManager();

		Query query = pm.newQuery(Person.class);
		query.setFilter("firstName == :fN ");

		// p1 firstName == p1-p3 and p4 lastName == p3 and p4
		List<Person> results = (List<Person>) query.execute("foobar");

		// check we got p1, p2 and p3 and p4

		assertEquals(0, results.size());

	}

	/**
	 * Query returning an object with relation fields, testing the contents of
	 * the relation fields.
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void testRetrieveAnd() {

		// now perform our select. We want everyone with firstname =
		// "firstName1"
		PersistenceManager pm = pmf.getPersistenceManager();

		Query query = pm.newQuery(Person.class);
		query.setFilter("firstName == :fN && lastName == :lN");

		List<Person> results = (List<Person>) query.execute(p2.getFirstName(),
				p2.getLastName());

		// check we got p1, p2 and p3.

		assertEquals(1, results.size());

		assertTrue(results.contains(p2));

	}

	/**
	 * Query returning an object with relation fields, testing the contents of
	 * the relation fields.
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void testRetrieveOR() {
		// now perform our select. We want everyone with firstname =
		// "firstName1"
		PersistenceManager pm = pmf.getPersistenceManager();

		Query query = pm.newQuery(Person.class);
		query.setFilter("firstName == :fN || lastName == :lN");

		// p1 firstName == p1-p3 and p4 lastName == p3 and p4
		List<Person> results = (List<Person>) query.execute(p1.getFirstName(),
				p4.getLastName());

		// check we got p1, p2 and p3 and p4

		assertEquals(4, results.size());

		assertTrue(results.contains(p1));
		assertTrue(results.contains(p2));
		assertTrue(results.contains(p3));
		assertTrue(results.contains(p4));

	}

	/**
	 * Query returning an object with relation fields, testing the contents of
	 * the relation fields.
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void testRetrieveGreaterThanEqual() {

		// now perform our select. We want everyone with firstname =
		// "firstName1"
		PersistenceManager pm = pmf.getPersistenceManager();

		Query query = pm.newQuery(Person.class);
		query.setFilter("lastLogin >= :loginDate && lastName == :secondName");

		// should be p3 p4 and p5
		List<Person> results = (List<Person>) query.execute(p3.getLastLogin(),
				"secondName2");

		// check we got p1, p2 and p3 and p4

		assertEquals(2, results.size());

		assertTrue(results.contains(p3));
		assertTrue(results.contains(p4));

	}

	/**
	 * Query returning an object with relation fields, testing the contents of
	 * the relation fields.
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void testRetrieveGreaterThan() {

		// now perform our select. We want everyone with firstname =
		// "firstName1"
		PersistenceManager pm = pmf.getPersistenceManager();

		Query query = pm.newQuery(Person.class);
		query.setFilter("lastLogin > :loginDate && lastName == :secondName");

		// should be p4 and p5
		List<Person> results = (List<Person>) query.execute(p3.getLastLogin(),
				"secondName2");

		assertEquals(1, results.size());

		assertTrue(results.contains(p4));

	}

	/**
	 * Query returning an object with relation fields, testing the contents of
	 * the relation fields.
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void testRetrieveLessThanEqual() {

		// now perform our select. We want everyone with firstname =
		// "firstName1"
		PersistenceManager pm = pmf.getPersistenceManager();

		Query query = pm.newQuery(Person.class);
		query.setFilter("lastLogin <= :loginDate && firstName == :fName");

		// should be p1 p2 p3
		List<Person> results = (List<Person>) query.execute(p3.getLastLogin(),
				"firstName1");

		// check we got p1, p2 and p3

		assertEquals(3, results.size());

		assertTrue(results.contains(p1));
		assertTrue(results.contains(p2));
		assertTrue(results.contains(p3));

	}

	/**
	 * Query returning an object with relation fields, testing the contents of
	 * the relation fields.
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void testRetrieveLessThanEqualOrder() {

		// now perform our select. We want everyone with firstname =
		// "firstName1"
		PersistenceManager pm = pmf.getPersistenceManager();

		Query query = pm.newQuery(Person.class);
		query.setFilter("lastLogin <= :loginDate && firstName == :fName");
		query.setOrdering("lastLogin DESC");

		// should be p1 p2 p3
		List<Person> results = (List<Person>) query.execute(p3.getLastLogin(),
				"firstName1");

		// check we got p1, p2 and p3

		assertEquals(3, results.size());

		assertTrue(results.get(0).equals(p3));
		assertTrue(results.get(1).equals(p2));
		assertTrue(results.get(2).equals(p1));

	}

	/**
	 * Query returning an object with relation fields, testing the contents of
	 * the relation fields.
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void testRetrieveLessThanEqualOrderRange() {

		// now perform our select. We want everyone with firstname =
		// "firstName1"
		PersistenceManager pm = pmf.getPersistenceManager();

		Query query = pm.newQuery(Person.class);
		query.setFilter("lastLogin <= :loginDate && firstName == :fName");
		query.setOrdering("lastLogin DESC");
		query.setRange(1, 3);

		// should be p1 p2 p3
		List<Person> results = (List<Person>) query.execute(p3.getLastLogin(),
				"firstName1");

		// check we got p1, p2 and p3

		assertEquals(2, results.size());

		assertTrue(results.get(0).equals(p2));
		assertTrue(results.get(1).equals(p1));

	}

	/**
	 * Query returning an object with relation fields, testing the contents of
	 * the relation fields.
	 */
	@SuppressWarnings({ "unchecked", "unused" })
	@Test
	public void testRetrieveGreaterLessThanEqualNoOrderRange() {

		// now perform our select. We want everyone with firstname =
		// "firstName1"
		PersistenceManager pm = pmf.getPersistenceManager();

		Query query = pm.newQuery(Person.class);
		query.setFilter("lastLogin <= :loginDate && firstName == :fName");
		query.setRange(1, 3);

		// should be p1 p2 p3
		try {
			List<Person> results = (List<Person>) query.execute(
					p3.getLastLogin(), "firstName1");

		} catch (JDODataStoreException ndse) {
			if (ndse.getCause() instanceof NucleusDataStoreException) {
				return;
			}
		}

		fail("Should have thrown an exception.  You can't perform ranges without ordering");

		// check we got p1, p2 and p3

	}

	/**
	 * Query returning an object with relation fields, testing the contents of
	 * the relation fields.
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void testRetrieveLessThan() {
		PersistenceManager pm = pmf.getPersistenceManager();

		Query query = pm.newQuery(Person.class);
		query.setFilter("lastLogin < :loginDate && firstName == :fName");

		// should be p1 and p2
		List<Person> results = (List<Person>) query.execute(p3.getLastLogin(),
				"firstName1");

		// check we got p1, p2 and p3 and p4

		assertEquals(2, results.size());

		assertTrue(results.contains(p1));
		assertTrue(results.contains(p2));

	}

	/**
	 * Query returning an object with relation fields, testing the contents of
	 * the relation fields.
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void testRetrieveLessThanNoValue() {
		PersistenceManager pm = pmf.getPersistenceManager();

		Query query = pm.newQuery(Person.class);
		query.setFilter("lastLogin < :loginDate  && firstName == :fName");

		// should be p1 and p2
		List<Person> results = (List<Person>) query.execute(p1.getLastLogin(),
				"firstName1");

		// check we got p1, p2 and p3 and p4

		assertEquals(0, results.size());

	}

	/**
	 * Tests that when a field is common on 2 subclasses, the correct subclass
	 * is returned
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void testSharedFieldSubclass() {
		
		
		PersistenceManager pm = pmf.getPersistenceManager();
		Transaction trans = pm.currentTransaction();
		trans.begin();

		SearchOne one = new SearchOne();
		one.setSearchField("search");
		one.setSearchOne("searchOne");

		SearchTwo two = new SearchTwo();
		two.setSearchField("search");
		two.setSearchTwo("searchThree");

		SearchThree three = new SearchThree();
		three.setSearchField("search");
		three.setSearchThree("searchThree");

		pm.makePersistent(one);
		pm.makePersistent(two);
		pm.makePersistent(three);

		trans.commit();
		pm.close();

		pm = pmf.getPersistenceManager();

		Query query = pm.newQuery(SearchOne.class);
		query.setFilter("searchField == :search");
		query.setIgnoreCache(true);

		// now query on the subclass
		List<SearchOne> resultsOne = (List<SearchOne>) query.execute("search");

		assertEquals(1, resultsOne.size());

		assertTrue(resultsOne.contains(one));

		query = pm.newQuery(SearchTwo.class);
		query.setFilter("searchField == :search");
		query.setIgnoreCache(true);

		// now query on the subclass
		List<SearchTwo> resultsTwo = (List<SearchTwo>) query.execute("search");

		assertEquals(1, resultsTwo.size());

		assertTrue(resultsTwo.contains(two));

		query = pm.newQuery(SearchThree.class);
		query.setFilter("searchField == :search");
		query.setIgnoreCache(true);

		// now query on the subclass
		List<SearchThree> resultsThree = (List<SearchThree>) query
				.execute("search");

		assertEquals(1, resultsTwo.size());

		assertTrue(resultsThree.contains(three));

		// query for all subclasses

		query = pm.newQuery(Search.class);
		query.setFilter("searchField == :search");
		query.setIgnoreCache(true);

		// should be p3-p5 with login date then p5 based on contains with Name,
		// and p4 with firstname
		List<Search> results = (List<Search>) query.execute("search");

		// check we got p1, p2 and p3 and p5

		assertEquals(3, results.size());

		assertTrue(results.contains(one));
		assertTrue(results.contains(two));
		assertTrue(results.contains(three));

	}

	/**
	 * Tests that when a field is common on 2 subclasses, the correct subclass
	 * is returned. TODO this is failing. It seems that cassandra is only
	 * returning 1 row for all secondary indexes
	 * 
	 * @throws InterruptedException
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void testMultipleChildren() throws InterruptedException {
		Consistency.setDefault(ConsistencyLevel.QUORUM);
		
		PersistenceManager pm = pmf.getPersistenceManager();
		Transaction trans = pm.currentTransaction();
		trans.begin();

		Child childOne = new Child();
		childOne.setParentField("test");

		ChildTwo childTwo = new ChildTwo();
		childTwo.setParentField("test");

		GrandChildTwoOne gcTwoOne = new GrandChildTwoOne();
		gcTwoOne.setParentField("test");

		GrandChildTwoTwo gcTwoTwo = new GrandChildTwoTwo();
		gcTwoTwo.setParentField("test");

		pm.makePersistent(childOne);
		
		printAllRows("RecursiveParent");
		
		pm.makePersistent(childTwo);
		
		printAllRows("RecursiveParent");
		
		pm.makePersistent(gcTwoOne);
		
		printAllRows("RecursiveParent");
		
		pm.makePersistent(gcTwoTwo);

		printAllRows("RecursiveParent");
		
		
		trans.commit();
		pm.close();

		pm = pmf.getPersistenceManager();
		
		
		//verify the data in storage.
		

	
		

		Query query = pm.newQuery(ChildTwo.class);
		query.setFilter("parentField == :searchVal");
		query.setIgnoreCache(true);

		// now query on the subclass
		List<ChildTwo> results = (List<ChildTwo>) query.execute("test");
		
//		Thread.sleep(100000 * 100000);

		assertEquals(3, results.size());

		assertTrue(results.contains(childTwo));
		assertTrue(results.contains(gcTwoOne));
		assertTrue(results.contains(gcTwoTwo));

	

	}

}