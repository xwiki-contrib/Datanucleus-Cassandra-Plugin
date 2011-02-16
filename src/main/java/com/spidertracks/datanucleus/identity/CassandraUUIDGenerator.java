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

Contributors : Todd Nine
 ***********************************************************************/
package com.spidertracks.datanucleus.identity;

import java.util.Properties;

import org.datanucleus.store.valuegenerator.AbstractGenerator;
import org.datanucleus.store.valuegenerator.ValueGenerationBlock;

import com.eaio.uuid.UUID;


/**
 * @author Todd Nine
 * 
 */
public class CassandraUUIDGenerator extends AbstractGenerator {

	/**
	 * 
	 * @param name
	 * @param props
	 */
	public CassandraUUIDGenerator(String name, Properties props) {
		
		super(name, props);
	}

	/**
	 * Method to reserve "size" ValueGenerations to the ValueGenerationBlock.
	 * 
	 * @param size
	 *            The block size
	 * @return The reserved block
	 */
	public ValueGenerationBlock reserveBlock(long size) {
		
		
		UUID[] ids = new UUID[(int) size];
		for (int i = 0; i < size; i++) {
			ids[i] = getIdentifier();
		}
		
		return new ValueGenerationBlock(ids);
	}

	/**
	 * Create a UUID identifier.
	 * 
	 * @return The identifier
	 */
	protected UUID getIdentifier()
    {
		 return new com.eaio.uuid.UUID();

    }
	
}
