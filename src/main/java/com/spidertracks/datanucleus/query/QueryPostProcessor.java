/**********************************************************************
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 ***********************************************************************/
package com.spidertracks.datanucleus.query;

import java.util.Collection;
import java.util.Map;


/**
 * A mechanism which can postprocess a query which Cassandra could not completely handle
 * and so, returned more results than were desired or returned them in the wrong order.
 *
 * @version $Id$
 * @since 1.1.1-0.7.0
 */
interface QueryPostProcessor
{
    /**
     * Run a postprocessing of a query against a set of result candidates.
     *
     * @param candidates the result candidates to run the query against.
     * @param parameters the query parameters if this was a parameterized query.
     * @return a postprocessed (paired down and perhaps reorganized) version of candidates.
     */
    Collection<?> run(final Collection<?> candidates, final Map parameters);
}
