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

/**
 * Simple interface to allow users to create their own serialization schemes
 * @author Todd Nine
 *
 */
public interface Serializer {

	/**
	 * Get the bytes for the serialized object
	 * @param value
	 * @return
	 */
	public byte[] getBytes(Object value);
	
	
	/**
	 * Get the object for the bytes
	 * @param <T>
	 * @param bytes
	 * @return
	 */
	public <T> T getObject(byte[] bytes);
	
	/**
	 * Return the maximum number of bytes this object could have when serialized
	 */
	public int size(Object value);
	
	
}
