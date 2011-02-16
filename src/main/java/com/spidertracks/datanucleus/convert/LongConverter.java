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
public class LongConverter implements ByteConverter {

	private static final int SIZE = Long.SIZE / Byte.SIZE;

	@Override
	public Object getObject(ByteBuffer buffer, ByteConverterContext context) {
		if (buffer == null || buffer.remaining() < SIZE) {
			return null;
		}

		return buffer.getLong();
	}

	@Override
	public ByteBuffer writeBytes(Object value, ByteBuffer buffer, ByteConverterContext context) {
		if (value == null) {
			return buffer;
		}

		ByteBuffer returned = check(buffer, SIZE);

		return returned.putLong((Long) value);
	}

	@Override
	public String getComparatorType() {
		return ColumnFamilyManager.CFDEF_COMPARATOR_LONG;
	}

}
