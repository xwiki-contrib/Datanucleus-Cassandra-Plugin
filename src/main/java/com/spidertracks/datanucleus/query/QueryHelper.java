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
import org.datanucleus.query.expression.DyadicExpression;
import org.datanucleus.query.expression.Expression;
import org.datanucleus.query.expression.Literal;
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
import com.spidertracks.datanucleus.query.runtime.EqualityOperand;
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

            // TODO: Ordering in Cassandra.
            //range = (int) query.getRangeToExcl();
        }

        final CassandraQueryExpressionEvaluator evaluator = new CassandraQueryExpressionEvaluator(
            acmd, range, byteConverter, parameters, candidateClass);

        final Expression filter = query.getCompilation().getExprFilter();

System.out.println("Running Query: [ " + filter + " ]");

        // If a query was specified, and there are indexed fields in the query,
        // perform a filter with secondary cassandra indexes.
        final Set<Columns> candidateKeys =
            runQuery(filter, evaluator, acmd, context, selectColumns, range);

        final List<?> results = getObjectsOfCandidateType(candidateKeys,
                                                          context,
                                                          candidateClass,
                                                          query.isSubclasses(),
                                                          idColumnBytes,
                                                          discriminatorColumn,
                                                          byteConverter);

        return postProcessor.run(results, parameters);
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
            if (!ClassUtils.typesAreCompatible(targetClass, idClassName, resolver))
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
                // This should never happen.
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
                                       final AbstractClassMetaData acmd,
                                       final Bytes[] selectColumns,
                                       final int maxSize)
    {
        final String cfName = MetaDataUtils.getColumnFamily(acmd);

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
                                         final Bytes[] selectColumns,
                                         final int maxResults)
    {
        final CassandraStoreManager storeManager =
            ((CassandraStoreManager) context.getStoreManager());

        Operand opTree = null;
        try {
            if (filter != null) {
                opTree = (Operand) filter.evaluate(evaluator);
            } else {
                opTree = new EqualityOperand(maxResults);
            }

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
        } catch (Exception e) {
            // TODO: handle queries containing strange expressions properly
            // rather than pushing everything off on the in-memory handler.
            opTree = new EqualityOperand(maxResults);
        }

        if (!opTree.isIndexed()) {
System.out.println("Returning all entries from : [" + MetaDataUtils.getColumnFamily(acmd) + "]");
            // just get all keys.
            return getAll(((CassandraStoreManager) context.getStoreManager()).getPoolName(),
                          acmd,
                          selectColumns,
                          maxResults);
        }

System.out.println("Query: [" + opTree.toString() + "]");
        try {
            opTree.performQuery(storeManager.getPoolName(),
                                MetaDataUtils.getColumnFamily(acmd),
                                selectColumns);
        } catch (NucleusException e) {
            throw new NucleusException("Failed to run query [" + opTree.toString() + "]", e);
        }
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
