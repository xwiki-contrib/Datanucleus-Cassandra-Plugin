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

import org.datanucleus.exceptions.NucleusDataStoreException;

import com.spidertracks.datanucleus.identity.ByteAware;

/**
 * ByteAware wrapper that will create new instances of keys and return them
 * 
 * @author Todd Nine
 * 
 */
public class ByteAwareConverter implements ByteConverter {

	private Class<?> targetClass;

	public ByteAwareConverter(Class<?> targetClass) {
		this.targetClass = targetClass;
	}

	@Override
	public Object getObject(ByteBuffer buffer, ByteConverterContext context) {

		ByteAware instance = createInstance();

		instance.parseBytes(buffer, context);

		return instance;

	}

	@Override
	public ByteBuffer writeBytes(Object value, ByteBuffer buffer, ByteConverterContext context) {

		return ((ByteAware) value).writeBytes(buffer, context);

	}

	@Override
	public String getComparatorType() {
		return createInstance().getComparatorType();
	}

	private ByteAware createInstance() {
		try {
			return (ByteAware) targetClass.newInstance();
		} catch (Exception e) {
			throw new NucleusDataStoreException(
					String.format(
							"Unable to create new instance of class %s.  Please make sure it has a no arg constructor",
							targetClass.getName()));
		}
	}

}
