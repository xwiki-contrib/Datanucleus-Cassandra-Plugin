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
package com.spidertracks.datanucleus.convert;
import static com.spidertracks.datanucleus.convert.ConverterUtils.check;

import java.nio.ByteBuffer;

import org.scale7.cassandra.pelops.ColumnFamilyManager;

/**
 * @author Todd Nine
 *
 */
public class CharacterConverter implements ByteConverter {

	private static final int SIZE = Character.SIZE / Byte.SIZE;
	
	@Override
	public Character getObject(ByteBuffer buff, ByteConverterContext context) {
		if(buff == null || buff.remaining() < SIZE){
			return null;
		}
		return buff.getChar();
	}

	@Override
	public ByteBuffer writeBytes(Object value, ByteBuffer buff, ByteConverterContext context) {
		ByteBuffer returned = check(buff, SIZE);
		
		return returned.putChar((Character) value);
	}

	@Override
	public String getComparatorType() {
		return ColumnFamilyManager.CFDEF_COMPARATOR_BYTES;
	}

	

	

}
