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

Contributors : Pedro Gomes and Universidade do Minho.
             : Todd Nine
 ***********************************************************************/
package com.spidertracks.datanucleus;

import static com.spidertracks.datanucleus.utils.MetaDataUtils.getColumnFamily;
import static com.spidertracks.datanucleus.utils.MetaDataUtils.getDiscriminatorColumnName;
import static com.spidertracks.datanucleus.utils.MetaDataUtils.getFetchColumnList;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.thrift.Column;
import org.datanucleus.ClassLoaderResolver;
import org.datanucleus.api.ApiAdapter;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.datanucleus.exceptions.NucleusException;
import org.datanucleus.exceptions.NucleusObjectNotFoundException;
import org.datanucleus.metadata.AbstractClassMetaData;
import org.datanucleus.metadata.AbstractMemberMetaData;
import org.datanucleus.metadata.DiscriminatorMetaData;
import org.datanucleus.metadata.DiscriminatorStrategy;
import org.datanucleus.metadata.Relation;
import org.datanucleus.store.AbstractPersistenceHandler;
import org.datanucleus.store.ExecutionContext;
import org.datanucleus.store.ObjectProvider;
import org.scale7.cassandra.pelops.Bytes;
import org.scale7.cassandra.pelops.Mutator;
import org.scale7.cassandra.pelops.Pelops;
import org.scale7.cassandra.pelops.Selector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.spidertracks.datanucleus.client.Consistency;
import com.spidertracks.datanucleus.convert.ByteConverterContext;
import com.spidertracks.datanucleus.mutate.BatchMutationManager;
import com.spidertracks.datanucleus.mutate.ExecutionContextDelete;

/**
 * Persistence handler for our DN plugin
 * 
 * @author Todd Nine
 * 
 */
public class CassandraPersistenceHandler extends AbstractPersistenceHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraPersistenceHandler.class);

    private CassandraStoreManager manager;
    private BatchMutationManager batchManager;
    private ByteConverterContext byteContext;

    public CassandraPersistenceHandler(CassandraStoreManager manager) {
        this.manager = manager;
        this.batchManager = new BatchMutationManager(manager);
        this.byteContext = manager.getByteConverterContext();
    }

    @Override
    public void close() {

    }

    @Override
    public void deleteObject(ObjectProvider op) {

        Bytes key = byteContext.getRowKey(op);

        String columnFamily = getColumnFamily(op.getClassMetaData());

        ExecutionContext ec = op.getExecutionContext();

        ExecutionContextDelete delete = this.batchManager.beginDelete(ec, op);

        // we've already visited this object, do nothing
        if (!delete.addDeletion(op, key, columnFamily)) {
            return;
        }

        // delete our dependent objects as well.
        AbstractClassMetaData metaData = op.getClassMetaData();

        int[] fields = metaData.getAllMemberPositions();

        for (int current : fields) {
            AbstractMemberMetaData fieldMetaData = metaData
                    .getMetaDataForManagedMemberAtAbsolutePosition(current);

            // if we're a collection, delete each element
            // recurse to delete this object if it's marked as dependent
            if (fieldMetaData.isDependent()
                    || (fieldMetaData.getCollection() != null && fieldMetaData
                            .getCollection().isDependentElement())) {

                // here we have the field value
                Object value = op.provideField(current);

                if (value == null) {
                    continue;
                }

                ClassLoaderResolver clr = ec.getClassLoaderResolver();

                int relationType = fieldMetaData.getRelationType(clr);

                // check if this is a relationship

                if (relationType == Relation.ONE_TO_ONE_BI
                        || relationType == Relation.ONE_TO_ONE_UNI || relationType == Relation.MANY_TO_ONE_BI) {
                    // Persistable object - persist the related object and
                    // store the
                    // identity in the cell

                    ec.deleteObjectInternal(value);
                }

                else if (relationType == Relation.MANY_TO_MANY_BI
                        || relationType == Relation.ONE_TO_MANY_BI || relationType == Relation.ONE_TO_MANY_UNI) {
                    // Collection/Map/Array

                    if (fieldMetaData.hasCollection()) {

                        for (Object element : (Collection<?>) value) {
                            // delete the object
                            ec.deleteObjectInternal(element);
                        }

                    } else if (fieldMetaData.hasMap()) {
                        ApiAdapter adapter = ec.getApiAdapter();

                        Map<?, ?> map = ((Map<?, ?>) value);
                        Object mapValue;

                        // get each element and persist it.
                        for (Object mapKey : map.keySet()) {

                            mapValue = map.get(mapKey);

                            // handle the case if our key is a persistent
                            // class
                            // itself
                            if (adapter.isPersistable(mapKey)) {
                                ec.deleteObjectInternal(mapKey);

                            }
                            // persist the value if it can be persisted
                            if (adapter.isPersistable(mapValue)) {
                                ec.deleteObjectInternal(mapValue);
                            }

                        }

                    } else if (fieldMetaData.hasArray()
                            && fieldMetaData.isDependent()) {
                        Object persisted = null;

                        for (int i = 0; i < Array.getLength(value); i++) {
                            // persist the object
                            persisted = Array.get(value, i);
                            ec.deleteObjectInternal(persisted);
                        }
                    }

                }

            }

        }

        try {
            this.batchManager.endDelete(ec);

        } catch (NucleusException ne) {
            throw ne;
        } catch (Exception e) {
            throw new NucleusDataStoreException(e.getMessage(), e);
        }
    }

    @Override
    public void fetchObject(ObjectProvider op, int[] fieldNumbers) {
        AbstractClassMetaData metaData = op.getClassMetaData();

        Bytes key = byteContext.getRowKey(op);
        String columnFamily = getColumnFamily(metaData);

        Selector selector = Pelops.createSelector(manager.getPoolName());

        List<Column> columns = selector.getColumnsFromRow(columnFamily, key,
                getFetchColumnList(metaData, fieldNumbers), Consistency.get());

        // nothing to do
        if (columns == null || columns.size() == 0) {
            // check if the pk field was requested. If so, throw an
            // exception b/c the object doesn't exist
            pksearched(metaData, fieldNumbers);


        }

        CassandraFetchFieldManager manager = new CassandraFetchFieldManager(
                columns, op, columnFamily, key, selector);

        op.replaceFields(fieldNumbers, manager);

    }

    /**
     * Checks if a pk field was requested to be loaded. If it is null a
     * NucleusObjectNotFoundException is thrown because we only call this with 0
     * column results
     * 
     * @param metaData
     * @param requestedFields
     */
    private void pksearched(AbstractClassMetaData metaData,
            int[] requestedFields) {

        int[] pkPositions = metaData.getPKMemberPositions();

        for (int pkPosition : pkPositions) {
            for (int requestedField : requestedFields) {
                // our pk was a requested field, throw an exception b/c we
                // didn't find anything
                if (requestedField == pkPosition) {
                    throw new NucleusObjectNotFoundException();
                }
            }
        }
    }

    @Override
    public Object findObject(ExecutionContext ec, Object id) {
        return null;
    }

    @Override
    public void insertObject(ObjectProvider op) {
        // just delegate to update. They both perform the same logic
        updateObject(op, op.getClassMetaData().getAllMemberPositions());

    }

    @Override
    public void locateObject(ObjectProvider op) {
        fetchObject(op, op.getClassMetaData().getAllMemberPositions());

    }

    @Override
    public void updateObject(ObjectProvider op, int[] fieldNumbers) {
        this.manager.assertReadOnlyForUpdateOfObject(op);

        AbstractClassMetaData metaData = op.getClassMetaData();

        ExecutionContext ec = op.getExecutionContext();

        // signal a write is about to start
        Mutator mutator = this.batchManager.beginWrite(ec).getMutator();
        Selector selector = Pelops.createSelector(manager.getPoolName());


        Bytes key = byteContext.getRowKey(op);
        String columnFamily = getColumnFamily(metaData);

        // Write our all our primary object data
        CassandraInsertFieldManager manager = new CassandraInsertFieldManager(
                selector, mutator, op, columnFamily, key);

        op.provideFields(metaData.getAllMemberPositions(), manager);

        // if we have a discriminator, write the value
        if (metaData.hasDiscriminatorStrategy()) {
            final DiscriminatorMetaData discriminator = metaData.getDiscriminatorMetaData();

            Bytes colName = getDiscriminatorColumnName(discriminator);

            // DN doesn't provide discrminator value if the strategy is CLASS_NAME.
            final String value = (discriminator.getStrategy() == DiscriminatorStrategy.CLASS_NAME)
                ? metaData.getFullClassName()
                : discriminator.getValue();

            LOGGER.debug("Object [{}] has a discriminator, it is [{}].", key.toUTF8(), value);
            
            Bytes byteValue = byteContext.getBytes(value);
            
            mutator.writeColumn(columnFamily, key, mutator.newColumn(colName, byteValue));
        } else {
            LOGGER.debug("Object [{}] has no discriminator.", key.toUTF8());
        }

        try {

            this.batchManager.endWrite(ec);

        } catch (NucleusException ne) {
            throw ne;
        } catch (Exception e) {

            throw new NucleusDataStoreException(e.getMessage(), e);
        }

    }

}
