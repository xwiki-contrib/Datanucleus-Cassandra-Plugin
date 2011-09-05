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
 ***********************************************************************/
package com.spidertracks.datanucleus.query;


import java.util.Collection;
import java.util.Map;

import org.datanucleus.query.evaluator.JPQLEvaluator;
import org.datanucleus.query.evaluator.JavaQueryEvaluator;
import org.datanucleus.store.ExecutionContext;
import org.datanucleus.store.query.AbstractJPQLQuery;
import org.datanucleus.util.NucleusLogger;


/**
 * A query in JPQL query language.
 *
 * @version $Id$
 * @since 1.1.1-0.7.0
 */
public class JPQLQuery extends AbstractJPQLQuery
{
    /** Serialization number. */
    private static final long serialVersionUID = 1L;

    /** The type of query, used by the logger. */
    private static final String QUERY_TYPE = "JPQL";

    /**
     * Constructs a new query instance that uses the given persistence manager.
     * 
     * @param ec the associated ExecutiongContext for this query.
     */
    public JPQLQuery(final ExecutionContext ec) {
        this(ec, (JPQLQuery) null);
    }

    /**
     * Constructs a new query instance having the same criteria as the given query.
     * 
     * @param ec The Executing Manager
     * @param q The query from which to copy criteria.
     */
    public JPQLQuery(final ExecutionContext ec, final JPQLQuery q) {
        super(ec, q);
    }

    /**
     * Constructor for a JPQL query where the query is specified using the "Single-String" format.
     * 
     * @param ec The execution context
     * @param query The query string
     */
    public JPQLQuery(final ExecutionContext ec, final String query) {
        super(ec, query);
    }

    @Override
    protected Object performExecute(Map parameters)
    {
        long startTime = System.currentTimeMillis();
        if (NucleusLogger.QUERY.isDebugEnabled()) {
            NucleusLogger.QUERY.debug(LOCALISER.msg("0121121", QUERY_TYPE,
                getSingleStringQuery(), null));
        }

        final Object result =
            QueryHelper.executeQuery(parameters, this, new JPQLQueryPostProcessor(this));

        if (NucleusLogger.QUERY.isDebugEnabled()) {
            NucleusLogger.QUERY.debug(LOCALISER.msg("021074", QUERY_TYPE, ""
                    + (System.currentTimeMillis() - startTime)));
        }

        return result;
    }

    /**
     * A postprocessor for JPQL queries.
     */
    private static class JPQLQueryPostProcessor implements QueryPostProcessor
    {
        /** The query to postprocess. */
        private final JPQLQuery query;

        /**
         * The Constructor.
         *
         * @param query the query to postprocess.
         */
        public JPQLQueryPostProcessor(final JPQLQuery query)
        {
            this.query = query;
        }

        @Override
        public Collection<?> run(final Collection<?> candidates, final Map parameters)
        {
            final JavaQueryEvaluator evaluator =
                new JPQLEvaluator(this.query,
                                  candidates,
                                  this.query.getCompilation(),
                                  parameters,
                                  query.getObjectManager().getClassLoaderResolver());

            return evaluator.execute(true, true, true, true, true);
        }
    }
}
