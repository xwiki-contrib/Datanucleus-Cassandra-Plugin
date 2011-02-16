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

import com.spidertracks.datanucleus.serialization.Serializer;

/**
 * Wrapper around our serlializer instance
 * 
 * @author Todd Nine
 * 
 */
public class SerializerWrapperConverter implements ByteConverter {
	private Serializer serializer;

	public SerializerWrapperConverter(Serializer serializer) {
		this.serializer = serializer;
	}

	@Override
	public Object getObject(ByteBuffer buffer, ByteConverterContext context) {
		if (buffer == null) {
			return null;
		}

		// TODO this be required to copy
		byte[] data = new byte[buffer.limit() - buffer.position()];

		buffer.get(data);

		return serializer.getObject(data);

	}

	@Override
	public ByteBuffer writeBytes(Object value, ByteBuffer buffer, ByteConverterContext context) {

		byte[] serialized = serializer.getBytes(value);

		ByteBuffer returned = check(buffer, serialized.length);

		return returned.put(serialized);

	}


	@Override
	public String getComparatorType() {
		return ColumnFamilyManager.CFDEF_COMPARATOR_BYTES;
	}

}