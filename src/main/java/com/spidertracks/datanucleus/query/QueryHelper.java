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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.jdo.identity.SingleFieldIdentity;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.KeyRange;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.query.expression.Expression;
import org.datanucleus.store.ExecutionContext;
import org.datanucleus.store.query.Query;
import org.datanucleus.util.ClassUtils;
import org.scale7.cassandra.pelops.Bytes;
import org.scale7.cassandra.pelops.Pelops;
import org.scale7.cassandra.pelops.Selector;
import com.spidertracks.datanucleus.CassandraStoreManager;
import com.spidertracks.datanucleus.client.Consistency;
import com.spidertracks.datanucleus.convert.ByteConverterContext;
import com.spidertracks.datanucleus.query.runtime.Columns;
import com.spidertracks.datanucleus.query.runtime.Operand;
import com.spidertracks.datanucleus.utils.MetaDataUtils;


/**
 * A utility class for running queries against Cassandra.
 *
 * @version $Id$
 * @since 1.1.1-0.7.0
 */
final class QueryHelper
{
    /** The default maximum number of entries to return in a query. */
    private static final int DEFAULT_MAX = 1000;

    /**
     * Private default constructor.
     * since it's a utility class.
     */
    private QueryHelper()
    {
        // Private default constructor.
    }

    /**
     * Run a query and get the objects which result.
     *
     * @param parameters the query parameters if this is a parameterized query.
     * @param query the query to run.
     * @param postProcessor the thing to use to postprocess the query if there are parts of it
     *                      which Cassandra cannot handle.
     * @return all of the objects in the store which match the query up to limited by DEFAULT_MAX.
     */
    static Collection<?> executeQuery(final Map parameters,
                                      final Query query,
                                      final QueryPostProcessor postProcessor)
    {
        final ExecutionContext context = query.getObjectManager();

        final ClassLoaderResolver resolver = context.getClassLoaderResolver();

        final Class<?> candidateClass = query.getCandidateClass();

        // The metadata for the class or interface selected.
        final AbstractClassMetaData acmd =
            context.getMetaDataManager().getMetaDataForClass(candidateClass.getName(), resolver);

        // The name of the identity column.
        final Bytes idColumnBytes = MetaDataUtils.getIdentityColumn(acmd);

        // The class discriminator column if one is used, otherwise null.
        final Bytes discriminatorColumn = getDiscriminatorColumnName(acmd);

        // Names of columns necessary to select in order to have a unique key.
        final Bytes[] selectColumns;
        if (discriminatorColumn != null) {
            selectColumns = new Bytes[] {idColumnBytes, discriminatorColumn};
        } else {
            selectColumns = new Bytes[] {idColumnBytes};
        }

        final CassandraStoreManager storeManager =
            (CassandraStoreManager) context.getStoreManager();

        final ByteConverterContext byteConverter = storeManager.getByteConverterContext();

        int range = DEFAULT_MAX;
        if (query.getRange() != null) {
            if (query.getOrdering() == null) {
                throw new NucleusDataStoreException(
                        "You cannot invoke a range without an ordering expression against "
                      + "Cassandra. Results will be randomly ordered from Cassandra and need "
                      + "order to page");
            }

            range = (int) query.getRangeToExcl();
        }

        final CassandraQueryExpressionEvaluator evaluator =
            new CassandraQueryExpressionEvaluator(acmd, range, byteConverter, parameters);

        final Expression filter = query.getCompilation().getExprFilter();

        // If true, the query is not against any indexed columns
        // so we must select all rows and post filter.
        boolean nonIndexedQuery = isQueryNonIndexed(filter, evaluator, candidateClass);

        // If a query was specified, and there are indexed fields in the query,
        // perform a filter with secondary cassandra indexes.
        final Set<Columns> candidateKeys;
        if (filter != null && !nonIndexedQuery)
        {
            candidateKeys = runQuery(filter, evaluator, acmd, context, selectColumns);
        } else {
            // Otherwise just get every object of the given type and, if there was a query
            // which Cassandra simply couldn't handle, sort it out later.
            candidateKeys = getAll(storeManager.getPoolName(),
                                   MetaDataUtils.getColumnFamily(acmd),
                                   selectColumns,
                                   range);
        }

        final List<?> results = getObjectsOfCandidateType(candidateKeys,
                                                          context,
                                                          candidateClass,
                                                          query.isSubclasses(),
                                                          idColumnBytes,
                                                          discriminatorColumn,
                                                          byteConverter);

        // If this has an order by clause, a group by clause, or if we had to select all
        // entries of the given type because there were no expressions which cassandra could handle
        // then we have to postprocess the query.
        if (query.getOrdering() != null || query.getGrouping() != null || nonIndexedQuery)
        {
            return postProcessor.run(results, parameters);
        }

        return results;
    }

    /**
     * Load the actual objects from the keys.
     *
     * @param keys a set of the columns needed to find the correct class, if the class is a subclass
     *             then it should contain the identity and the discriminator, otherwise just the
     *             identity.
     * @param context the DataNucleus ExecutionContext.
     * @param candidateClass the class of object to get objects of.
     * @param subclasses whether to look in the store database to determine the class of the object.
     * @param identityColumn the name of the identity column.
     * @param descriminatorColumn the name of the descriminator column.
     * @param byteConverter the ByteConverterContext for deserializing the objects.
     * @return a list of persistable objects for each of the keys.
     */
    private static List<?> getObjectsOfCandidateType(final Set<Columns> keys,
                                                     final ExecutionContext context,
                                                     final Class<?> candidateClass,
                                                     final boolean subclasses,
                                                     final Bytes identityColumn,
                                                     final Bytes descriminatorColumn,
                                                     final ByteConverterContext byteConverter)
    {
        final ClassLoaderResolver resolver = context.getClassLoaderResolver();

        // The metadata for the class or interface selected.
        final AbstractClassMetaData acmd =
            context.getMetaDataManager().getMetaDataForClass(candidateClass.getName(), resolver);

        final List<Object> results = new ArrayList<Object>(keys.size());

        for (final Columns idBytes : keys) {

            // If this is subclassed with a discriminator then the class
            // which we want to return is the subclass, not the superclass.
            final Class<?> targetClass;
            if (descriminatorColumn != null) {

                final String descriminatorValue =
                    byteConverter.getString(idBytes.getColumnValue(descriminatorColumn));

                final String className =
                    org.datanucleus.metadata.MetaDataUtils.getClassNameFromDiscriminatorValue(
                        descriminatorValue, acmd.getDiscriminatorMetaData(), context);

                targetClass = resolver.classForName(className);
            } else {
                targetClass = candidateClass;
            }

            final Object identity = byteConverter.getObjectIdentity(
                context, targetClass, idBytes.getColumnValue(identityColumn));

            // Not a valid subclass, don't return it as a candidate
            if (!(identity instanceof SingleFieldIdentity)) {
                throw new NucleusDataStoreException("Only single field identities are supported");
            }

            final String idClassName = ((SingleFieldIdentity) identity).getTargetClassName();
            if (!ClassUtils.typesAreCompatible(candidateClass, idClassName, resolver))
            {
                throw new NucleusDataStoreException("The stored class's identity is for a class "
                                                    + "which is not the same nor a subclass of the "
                                                    + "candidate class to be selected. This should "
                                                    + "not happen.\nIdentity class name: "
                                                    + idClassName + "\nTarget class name: "
                                                    + targetClass.getName());
            }

            final Object returned =
                context.findObject(identity, true, subclasses, candidateClass.getName());

            if (returned == null) {
                // TODO: Remove this, this should never happen.
                throw new NucleusDataStoreException("findObject silently returned null!");
            }

            results.add(returned);
        }

        return results;
    }

    /**
     * Get all of a specified column from a given column family.
     * Used ranges to set the max amount.
     * 
     * @param poolName the name of the cassandra pool to query against.
     * @param cfName the name of the column family.
     * @param selectColumns the columns to get.
     * @param maxSize the maximum number of entries to return.
     * @return a set of the requested columns from all of the entries in the column family or
     *         from the maxSize, whichever is fewer.
     */
    private static Set<Columns> getAll(final String poolName,
                                       final String cfName,
                                       final Bytes[] selectColumns,
                                       final int maxSize)
    {

        Set<Columns> candidateKeys = new HashSet<Columns>();

        KeyRange range = new KeyRange();
        range.setStart_key(new byte[] {});
        range.setEnd_key(new byte[] {});
        range.setCount(maxSize);

        final Map<Bytes, List<Column>> results;
        try {
            results = Pelops.createSelector(poolName).getColumnsFromRows(
                    cfName, range, Selector.newColumnsPredicate(selectColumns),
                    Consistency.get());
        } catch (Exception e) {
            throw new NucleusException("Error scanning rows", e);
        }

        for (Entry<Bytes, List<Column>> entry : results.entrySet()) {

            if (entry.getValue().size() == 0) {
                continue;
            }

            final Columns cols = new Columns(entry.getKey());

            for (Column currentCol : entry.getValue()) {
                cols.addResult(currentCol);
            }

            candidateKeys.add(cols);
        }

        return candidateKeys;
    }

    /**
     * Check whether the filter has only non indexed fields.
     * A filter which only filters against non-indexed fields cannot be run against Cassandra
     * since cassandra can only select a slice from fields with a secondary index.
     * If this returns true then the query must select all of the given type the postprocess them.
     *
     * @param filter the "where" expression which filters what will be selected.
     * @param evaluator the object which converts the query into a stack of expressions which
     *                  can be run against Cassandra.
     * @param candidateClass the class of object to be returned.
     * @return true if none of the fields in the query filter are indexed.
     */
    private static boolean isQueryNonIndexed(final Expression filter,
                                             final CassandraQueryExpressionEvaluator evaluator,
                                             final Class<?> candidateClass)
    {
        final AnnotationEvaluator ae = new AnnotationEvaluator(candidateClass);
        final List<String> annotationlist = ae.getAnnotatedFields("javax.jdo.annotations.Index");
        final List<String> expressionlist = evaluator.getPrimaryExpressions(filter);

        if (annotationlist == null || expressionlist == null) {
            return true;
        }

        for (final String ex : expressionlist) {
            if (annotationlist.indexOf(ex) != -1) {
                return false;
            }
        }

        return true;
    }

    /**
     * Perform the query against Cassandra.
     *
     * @param filter the "where" expression of the query.
     * @param evaluator the mechanism for evaluating the filter into a stack of primative
     *                  operands which can be built into a Cassandra CQL query.
     * @param acmd metadata about the class ot interface being selected in the query.
     * @param context the DataNucleus ExecutionContext.
     * @param selectColumns the names of the columns which will be selected by this query.
     *                      If an entry matches the filter but does not have a column by the name
     *                      of one of selectColumns, it will not be returned.
     * @return a set of results each containing the row key and a subset of the columns in that row
     *         as named by selectColumns.
     */
    private static Set<Columns> runQuery(final Expression filter,
                                         final CassandraQueryExpressionEvaluator evaluator,
                                         final AbstractClassMetaData acmd,
                                         final ExecutionContext context,
                                         final Bytes[] selectColumns)
    {
        final CassandraStoreManager storeManager =
            ((CassandraStoreManager) context.getStoreManager());

        Operand opTree = (Operand) filter.evaluate(evaluator);

        // there's a discriminator so be sure to include it
        if (acmd.hasDiscriminatorStrategy()) {
            final Bytes descriminiatorCol =
                MetaDataUtils.getDiscriminatorColumnName(acmd.getDiscriminatorMetaData());

            final List<Bytes> descriminatorValues =
                MetaDataUtils.getDescriminatorValues(acmd.getFullClassName(),
                                                     context.getClassLoaderResolver(),
                                                     context,
                                                     storeManager.getByteConverterContext());

            opTree = opTree.optimizeDescriminator(descriminiatorCol, descriminatorValues);
        }
        // perform a query rewrite to take into account descriminator values
        opTree.performQuery(storeManager.getPoolName(),
                            MetaDataUtils.getColumnFamily(acmd),
                            selectColumns);

        return opTree.getCandidateKeys();
    }

    /**
     * @param acmd metadata about the class ot interface being selected in the query.
     * @return the name of the discriminator column if one is defined, otherwise null.
     */
    private static Bytes getDiscriminatorColumnName(final AbstractClassMetaData acmd)
    {
        if (acmd.hasDiscriminatorStrategy()) {
            return com.spidertracks.datanucleus.utils.MetaDataUtils.getDiscriminatorColumnName(
                acmd.getDiscriminatorMetaData());
        } else {
            return null;
        }
    }
}
