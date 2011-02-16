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

import java.nio.ByteBuffer;

import org.datanucleus.store.types.ObjectStringConverter;
import org.scale7.cassandra.pelops.ColumnFamilyManager;

/**
 * Encapsulates both a long converter and a DN long converter in the same
 * instance
 * 
 * @author Todd Nine
 * 
 */
public class ObjectStringWrapperConverter implements ByteConverter {

	private ObjectStringConverter dnStringConverter;
	private ByteConverter stringConverter;

	public ObjectStringWrapperConverter(ObjectStringConverter dnLongConverter,
			ByteConverter stringConverter) {
		this.dnStringConverter = dnLongConverter;
		this.stringConverter = stringConverter;
	}

	@Override
	public String getComparatorType() {
		return ColumnFamilyManager.CFDEF_COMPARATOR_UTF8;
	}

	@Override
	public Object getObject(ByteBuffer buffer, ByteConverterContext context) {
		if (buffer == null) {
			return null;
		}
		return dnStringConverter.toObject((String) stringConverter
				.getObject(buffer, context));
	}

	@Override
	public ByteBuffer writeBytes(Object value, ByteBuffer buffer, ByteConverterContext context) {
		String stringVal = dnStringConverter.toString(value);

		return stringConverter.writeBytes(stringVal, buffer, context);
	}


}
