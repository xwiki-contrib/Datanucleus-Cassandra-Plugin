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
package com.spidertracks.datanucleus.basic.inheritance.recursive;

import javax.jdo.annotations.Discriminator;
import javax.jdo.annotations.DiscriminatorStrategy;
import javax.jdo.annotations.Inheritance;
import javax.jdo.annotations.InheritanceStrategy;
import javax.jdo.annotations.PersistenceCapable;
import javax.jdo.annotations.Persistent;

/**
 * Should persist in it's own table
 * 
 * @author Todd Nine
 * 
 */
@PersistenceCapable
@Inheritance(strategy = InheritanceStrategy.SUPERCLASS_TABLE)
@Discriminator(strategy = DiscriminatorStrategy.VALUE_MAP, value = "GrandchildTwoTwo")
public class GrandChildTwoTwo extends ChildTwo {

	@Persistent
	private String grandChildTwoField;

	/**
	 * @return the grandChildOneField
	 */
	public String getGrandChildTwoField() {
		return grandChildTwoField;
	}

	/**
	 * @param grandChildOneField
	 *            the grandChildOneField to set
	 */
	public void setGrandChildTwoField(String grandChildOneField) {
		this.grandChildTwoField = grandChildOneField;
	}
}
