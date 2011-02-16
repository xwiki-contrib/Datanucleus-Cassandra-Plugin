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
package com.spidertracks.datanucleus.serialization;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.datanucleus.store.types.sco.SCO;

import com.thoughtworks.xstream.XStream;

/**
 * Serializer that serializes objects to JSON
 * 
 * @author Todd Nine
 * 
 */
public class XStreamSerializer implements Serializer {

	public XStreamSerializer() {

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.spidertracks.datanucleus.serialization.Serializer#getBytes(java.lang
	 * .Object)
	 */
	@Override
	public byte[] getBytes(Object value) {
		try {

			// we have to unwrap SCO instances, otherwise serialization blows up
			if (value instanceof SCO) {
				value = ((SCO) value).getValue();
			}

			ByteArrayOutputStream output = new ByteArrayOutputStream();

			XStream xstream = new XStream();

			xstream.toXML(value, output);

			output.flush();

			byte[] result = output.toByteArray();

			output.close();

			return result;
		} catch (IOException e) {
			throw new RuntimeException("Unable to serialize to json", e);
		}

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.spidertracks.datanucleus.serialization.Serializer#getObject(byte[])
	 */
	@SuppressWarnings("unchecked")
	@Override
	public <T> T getObject(byte[] bytes) {

		try {

			ByteArrayInputStream input = new ByteArrayInputStream(bytes);

			XStream xstream = new XStream();

			T result = (T) xstream.fromXML(input);

			input.close();

			return result;

		} catch (Exception e) {
			throw new RuntimeException("Unable to de-serialize xml to object.  Make sure you used this converter to persist the object", e);
		}

	}

	@Override
	public int size(Object value) {
		//no object can be larger than 2048 bytes
		return 2048;
	}

}
