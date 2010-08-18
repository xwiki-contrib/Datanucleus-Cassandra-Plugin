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
package com.spidertracks.datanucleus.basic.inheritance;

import static org.junit.Assert.assertEquals;

import javax.jdo.PersistenceManager;

import org.junit.Test;

import com.spidertracks.datanucleus.CassandraTest;
import com.spidertracks.datanucleus.basic.inheritance.caseone.Child;
import com.spidertracks.datanucleus.basic.inheritance.caseone.GrandChildOne;
import com.spidertracks.datanucleus.basic.inheritance.caseone.GrandChildTwo;
import com.spidertracks.datanucleus.basic.inheritance.casethree.ChildThree;
import com.spidertracks.datanucleus.basic.inheritance.casethree.GrandChildThreeOne;
import com.spidertracks.datanucleus.basic.inheritance.casethree.GrandChildThreeTwo;
import com.spidertracks.datanucleus.basic.inheritance.casetwo.ChildTwo;
import com.spidertracks.datanucleus.basic.inheritance.casetwo.GrandChildTwoOne;
import com.spidertracks.datanucleus.basic.inheritance.casetwo.GrandChildTwoTwo;

/**
 * Tests for 2 objects that are bi-directionally dependent and default fetch
 * group deleting properly
 * 
 * @author Todd Nine
 * 
 */
public class InheritanceTest extends CassandraTest {

	/**
	 * Test retrieval works with subclass and superclass mix
	 * 
	 * @throws Exception
	 */
	@Test
	public void testQueryChildReturnsSubclass() throws Exception {

		GrandChildOne first = new GrandChildOne();
		first.setChildField("cf-gc1");
		first.setGrandChildOneField("gcf-gc1");
		first.setParentField("pf-gc1");

		GrandChildTwo second = new GrandChildTwo();
		second.setChildField("cf-gc2");
		second.setGrandChildTwoField("gcf-gc2");
		second.setParentField("pf-gc2");

		Child third = new Child();
		third.setChildField("cf-c1");
		third.setParentField("pf-c1");

		PersistenceManager pm = pmf.getPersistenceManager();
		pm.makePersistent(first);
		pm.makePersistent(second);
		pm.makePersistent(third);

		// now retrieve to instances of "child" should return 2 subclasses

		Child savedFirst = pm.getObjectById(Child.class, first.getId());

		assertEquals(first, savedFirst);

		Child savedSecond = pm.getObjectById(Child.class, second.getId());

		assertEquals(second, savedSecond);

		Child savedThird = pm.getObjectById(Child.class, third.getId());

		assertEquals(third, savedThird);

	}

	/**
	 * Test retrieval works when each class has it's own CF
	 * 
	 * @throws Exception
	 */
	@Test
	public void testQueryChildReturnsSubclassOwnCF() throws Exception {

		GrandChildTwoOne first = new GrandChildTwoOne();
		first.setChildField("cf-gc1");
		first.setGrandChildOneField("gcf-gc1");
		first.setParentField("pf-gc1");

		GrandChildTwoTwo second = new GrandChildTwoTwo();
		second.setChildField("cf-gc2");
		second.setGrandChildOneField("gcf-gc2");
		second.setParentField("pf-gc2");

		ChildTwo third = new ChildTwo();
		third.setChildField("cf-c1");
		third.setParentField("pf-c1");

		PersistenceManager pm = pmf.getPersistenceManager();
		pm.makePersistent(first);
		pm.makePersistent(second);
		pm.makePersistent(third);

		// now retrieve to instances of "child" should return 2 subclasses

		ChildTwo savedFirst = pm.getObjectById(ChildTwo.class, first.getId());

		assertEquals(first, savedFirst);

		ChildTwo savedSecond = pm.getObjectById(ChildTwo.class, second.getId());

		assertEquals(second, savedSecond);

		ChildTwo savedThird = pm.getObjectById(ChildTwo.class, third.getId());

		assertEquals(third, savedThird);

	}
	
	/**
	 * Test retrieval works when everything is stored in abstract parent class cf
	 * 
	 * @throws Exception
	 */
	@Test
	public void testQueryChildReturnsSuperclassCF() throws Exception {

		GrandChildThreeOne first = new GrandChildThreeOne();
		first.setChildField("cf-gc1");
		first.setGrandChildOneField("gcf-gc1");
		first.setParentField("pf-gc1");

		GrandChildThreeTwo second = new GrandChildThreeTwo();
		second.setChildField("cf-gc2");
		second.setGrandChildOneField("gcf-gc2");
		second.setParentField("pf-gc2");

		ChildThree third = new ChildThree();
		third.setChildField("cf-c1");
		third.setParentField("pf-c1");

		PersistenceManager pm = pmf.getPersistenceManager();
		pm.makePersistent(first);
		pm.makePersistent(second);
		pm.makePersistent(third);

		// now retrieve to instances of "child" should return 2 subclasses

		ChildThree savedFirst = pm.getObjectById(ChildThree.class, first.getId());

		assertEquals(first, savedFirst);

		ChildThree savedSecond = pm.getObjectById(ChildThree.class, second.getId());

		assertEquals(second, savedSecond);

		ChildThree savedThird = pm.getObjectById(ChildThree.class, third.getId());

		assertEquals(third, savedThird);

	}


}
