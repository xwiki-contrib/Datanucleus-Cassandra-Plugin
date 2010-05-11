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
package org.datanucleus.store.cassandra;

/**
 * This interface defines all operations required to get the time to set to column operations.  
 * 
 * @author Todd Nine
 *
 */
public interface ColumnTimestamp {

	/**
	 * Get the time in milliseconds to use when applying time stamps to columns.
	 * This is invoked once per persistence manager operation to ensure that all columns have the same time stamp when a persistent object is modified.
	 * @return
	 */
	public long getTime();
}
