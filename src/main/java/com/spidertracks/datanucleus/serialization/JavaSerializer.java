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
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * @author Todd Nine
 *
 */
public class JavaSerializer implements Serializer {


	/* (non-Javadoc)
	 * @see com.spidertracks.datanucleus.serialization.Serializer#getBytes(java.lang.Object)
	 */
	@Override
	public byte[] getBytes(Object value) {
		try {
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(bos);
			oos.writeObject(value);
			oos.flush();

			byte[] bytes = bos.toByteArray();

			oos.close();
			bos.close();

			return bytes;
		} catch (Exception e) {
			throw new RuntimeException("Unable to serialize to object", e);
		}
	}

	/* (non-Javadoc)
	 * @see com.spidertracks.datanucleus.serialization.Serializer#getObject(byte[])
	 */
	@SuppressWarnings("unchecked")
	@Override
	public <T> T getObject(byte[] bytes) {
		try {
			ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
			ObjectInputStream ois;

			ois = new ObjectInputStream(bis);

			T serialized = (T) ois.readObject();
			ois.close();
			bis.close();

			return serialized;
		} catch (Exception e) {
			throw new RuntimeException("Unable to de-serialize to object", e);
		}
	}

	@Override
	public int size(Object value) {
		//no object can be larger than 2048 bytes
		return 2048;
	}

}
