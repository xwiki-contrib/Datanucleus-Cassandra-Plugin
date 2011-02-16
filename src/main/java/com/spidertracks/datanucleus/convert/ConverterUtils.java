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
import java.util.Arrays;

/**
 * @author Todd Nine
 * 
 */
public class ConverterUtils  {

	/**
	 * Check if the buffer has the remaining capacity to hold the number
	 * of bytes.  If not, create a new buffer with the required size and return it
	 * @param buffer
	 * @param size
	 * @return
	 */
	public static ByteBuffer check(ByteBuffer buffer, int size) {
		
		if(buffer == null){
			ByteBuffer newBuffer =  ByteBuffer.allocate(size);
			newBuffer.mark();
			return newBuffer;
		}
		
		if (buffer.remaining() < size) {

			int position = buffer.position();

			ByteBuffer newBuffer = ByteBuffer.allocate(position + size + 1);
			newBuffer.mark();

			buffer.reset();

			newBuffer.put(Arrays.copyOfRange(buffer.array(), 0, position));

			return newBuffer;

		}

		return buffer;
	}

}
