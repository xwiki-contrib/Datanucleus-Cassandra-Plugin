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

import static com.spidertracks.datanucleus.utils.MetaDataUtils.getColumnName;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.HashMap;
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
import org.datanucleus.metadata.Relation;
import org.datanucleus.store.ExecutionContext;
import org.datanucleus.store.ObjectProvider;
import org.datanucleus.store.fieldmanager.AbstractFieldManager;
import org.datanucleus.store.types.sco.SCOUtils;
import org.scale7.cassandra.pelops.Bytes;
import org.scale7.cassandra.pelops.Selector;

import com.spidertracks.datanucleus.collection.CassEntry;
import com.spidertracks.datanucleus.collection.ReadCollection;
import com.spidertracks.datanucleus.collection.ReadMap;
import com.spidertracks.datanucleus.convert.ByteConverterContext;

/**
 * @author Todd Nine
 * 
 */
public class CassandraFetchFieldManager extends AbstractFieldManager {

    private Map<Bytes, Bytes> columns;
    private AbstractClassMetaData metaData;
    private ObjectProvider objectProvider;
    private ByteConverterContext byteContext;
    private ExecutionContext context;
    private ClassLoaderResolver clr;
    private String columnFamily;
    private Bytes rowKey;
    private Selector selector;

    /**
     * @param columns
     * @param metaData
     */
    public CassandraFetchFieldManager(List<Column> columns, ObjectProvider op,
            String columnFamily, Bytes rowKey, Selector selector) {
        super();

        this.objectProvider = op;
        this.metaData = op.getClassMetaData();
        this.context = op.getExecutionContext();
        this.clr = this.context.getClassLoaderResolver();
        this.byteContext = ((CassandraStoreManager) context.getStoreManager())
                .getByteConverterContext();
        this.columnFamily = columnFamily;
        this.rowKey = rowKey;
        this.selector = selector;

        // rather than iterate over every field call for O(n) it's faster to
        // take our O(n) hit up front then perform an O(1) lookup. Sorting and
        // searching is O(n log (n)) sort plus log n search
        this.columns = new HashMap<Bytes, Bytes>();

        for (Column column : columns) {
            this.columns.put(Bytes.fromBytes(column.getName()),
                    Bytes.fromBytes(column.getValue()));
        }

    }

    @Override
    public boolean fetchBooleanField(int fieldNumber) {

        try {

            Bytes columnName = getColumnName(metaData, fieldNumber);
            Bytes value = this.columns.get(columnName);

            return (Boolean) byteContext.getBoolean(value);

        } catch (Exception e) {
            throw new NucleusException(e.getMessage(), e);
        }
    }

    @Override
    public byte fetchByteField(int fieldNumber) {
        try {

            Bytes columnName = getColumnName(metaData, fieldNumber);
            Bytes value = this.columns.get(columnName);

            return value.toByte();

        } catch (Exception e) {
            throw new NucleusException(e.getMessage(), e);
        }
    }

    @Override
    public char fetchCharField(int fieldNumber) {
        try {

            Bytes columnName = getColumnName(metaData, fieldNumber);
            Bytes value = this.columns.get(columnName);

            return (Character) byteContext.getCharacter(value);

        } catch (Exception e) {
            throw new NucleusException(e.getMessage(), e);
        }
    }

    @Override
    public double fetchDoubleField(int fieldNumber) {
        try {

            Bytes columnName = getColumnName(metaData, fieldNumber);
            Bytes value = this.columns.get(columnName);

            return (Double) byteContext.getDouble(value);

        } catch (Exception e) {
            throw new NucleusException(e.getMessage(), e);
        }
    }

    @Override
    public float fetchFloatField(int fieldNumber) {
        try {

            Bytes columnName = getColumnName(metaData, fieldNumber);
            Bytes value = this.columns.get(columnName);

            return (Float) byteContext.getFloat(value);

        } catch (Exception e) {
            throw new NucleusException(e.getMessage(), e);
        }
    }

    @Override
    public int fetchIntField(int fieldNumber) {
        try {

            Bytes columnName = getColumnName(metaData, fieldNumber);
            Bytes column = this.columns.get(columnName);

            return (Integer) byteContext.getInteger(column);

        } catch (Exception e) {
            throw new NucleusException(e.getMessage(), e);
        }
    }

    @Override
    public long fetchLongField(int fieldNumber) {
        try {

            Bytes columnName = getColumnName(metaData, fieldNumber);
            Bytes column = this.columns.get(columnName);

            return (Long) byteContext.getLong(column);

        } catch (Exception e) {
            throw new NucleusException(e.getMessage(), e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object fetchObjectField(int fieldNumber) {
        try {

            Bytes columnName = getColumnName(metaData, fieldNumber);
            Bytes column = columns.get(columnName);

            AbstractMemberMetaData fieldMetaData = this.metaData
                    .getMetaDataForManagedMemberAtAbsolutePosition(fieldNumber);

            int relationType = fieldMetaData.getRelationType(clr);

            if (relationType == Relation.ONE_TO_ONE_BI
                    || relationType == Relation.ONE_TO_ONE_UNI
                    || relationType == Relation.MANY_TO_ONE_BI) {

                // Persistable object
                if (fieldMetaData.isEmbedded()) {
                    // TODO Handle embedded objects
                    throw new NucleusDataStoreException(
                            "Embedded objects are currently unimplemented.");
                }

                // No object defined
                if (column == null) {
                    return null;
                }

                Object identity = byteContext.getObjectIdentity(context,
                        fieldMetaData.getType(), column);
                try {

                    Object object = context.findObject(identity, false, true,

                    fieldMetaData.getTypeName());

                    return objectProvider.wrapSCOField(fieldNumber, object,
                            false, false, true);
                } catch (NucleusObjectNotFoundException nonfe) {
                    // swallow. TODO remove the lazy reference if record is over
                    // tombstone time
                }

            } else if (relationType == Relation.MANY_TO_MANY_BI
                    || relationType == Relation.ONE_TO_MANY_BI
                    || relationType == Relation.ONE_TO_MANY_UNI) {

                if (Collection.class.isAssignableFrom(fieldMetaData.getType())) {

                    Collection<Object> coll;
                    Class<?> elementClass = clr.classForName(fieldMetaData
                            .getCollection().getElementType());

                    try {
                        Class<?> instanceType = SCOUtils
                                .getContainerInstanceType(
                                        fieldMetaData.getType(),
                                        fieldMetaData.getOrderMetaData() != null);
                        coll = (Collection<Object>) instanceType.newInstance();
                    } catch (Exception e) {
                        throw new NucleusDataStoreException(e.getMessage(), e);
                    }

                    // loop through the super columns

                    // get our list of Strings

                    ReadCollection columnFetcher = new ReadCollection(selector, 
                            byteContext, columnFamily, rowKey, columnName,
                            context, elementClass);

                    // TODO use context.getFetchPlan().getFetchSize()
                    columnFetcher.fetchColumns(100, null);

                    for (Object key : columnFetcher) {

                        try {
                            Object element = context.findObject(key, false,
                                    true, fieldMetaData.getTypeName());

                            coll.add(element);
                        } catch (NucleusObjectNotFoundException nonfe) {
                            // swallow. TODO remove the lazy reference if record
                            // is over tombstone time
                        }
                    }

                    if (coll.size() == 0) {
                        return null;
                    }

                    return objectProvider.wrapSCOField(fieldNumber, coll,
                            false, false, true);
                } else if (Map.class.isAssignableFrom(fieldMetaData.getType())) {

                    Map<Object, Object> map;

                    try {
                        Class<?> instanceType = SCOUtils
                                .getContainerInstanceType(
                                        fieldMetaData.getType(),
                                        fieldMetaData.getOrderMetaData() != null);
                        map = (Map<Object, Object>) instanceType.newInstance();
                    } catch (Exception e) {
                        throw new NucleusDataStoreException(e.getMessage(), e);
                    }

                    ApiAdapter adapter = objectProvider.getExecutionContext()
                            .getApiAdapter();

                    Class<?> keyClass = clr.classForName(fieldMetaData.getMap()
                            .getKeyType());
                    Class<?> valueClass = clr.classForName(fieldMetaData
                            .getMap().getValueType());

                    Class<?> storedKeyClass = keyClass;
                    Class<?> storedValueClass = valueClass;

                    boolean pcKey = adapter.isPersistable(keyClass);
                    boolean pcValue = adapter.isPersistable(valueClass);

                    if (pcKey) {
                        storedKeyClass = byteContext.getKeyClass(
                                context,
                                fieldMetaData.getMap().getKeyClassMetaData(clr,
                                        context.getMetaDataManager()));
                    }

                    if (pcValue) {
                        storedValueClass = byteContext.getKeyClass(
                                context,
                                fieldMetaData.getMap().getValueClassMetaData(
                                        clr, context.getMetaDataManager()));
                    }

                    // TODO use context.getFetchPlan().getFetchSize()
                    ReadMap mapReader = new ReadMap(selector, byteContext, columnFamily,
                            rowKey, columnName, storedKeyClass,
                            storedValueClass);
                    mapReader.fetchColumns(100, null);

                    for (CassEntry entry : mapReader) {

                        try {
                            Object key = null;

                            if (pcKey) {

                                key = context.findObject(
                                        context.newObjectId(keyClass,
                                                entry.getKey()), false, true,

                                        fieldMetaData.getTypeName());
                            } else {
                                key = entry.getKey();
                            }

                            Object value = null;

                            if (pcValue) {
                                value = context.findObject(
                                        context.newObjectId(valueClass,
                                                entry.getValue()), false, true,

                                        fieldMetaData.getTypeName());
                            } else {
                                value = entry.getValue();
                            }

                            map.put(key, value);
                        } catch (NucleusObjectNotFoundException nonfe) {
                            // swallow. TODO remove the lazy reference if record
                            // is over
                            // tombstone time
                        }

                    }

                    if (map.size() == 0) {
                        return null;
                    }

                    return objectProvider.wrapSCOField(fieldNumber, map, false,
                            false, true);

                } else if (fieldMetaData.getType().isArray()) {

                    ReadMap mapReader = new ReadMap(selector, byteContext, columnFamily,
                            rowKey, columnName, Integer.class,
                            byteContext.getKeyClass(context, metaData));
                    mapReader.fetchColumns(100, null);

                    int columns = mapReader.getColumnCount();

                    // TODO make this size more
                    Object array = Array.newInstance(fieldMetaData.getType()
                            .getComponentType(), columns);

                    Class<?> elementClass = clr.classForName(fieldMetaData
                            .getArray().getElementType());

                    for (int i = 0; mapReader.hasNext(); i++) {

                        CassEntry entry = mapReader.next();

                        Object id = context.newObjectId(elementClass,
                                entry.getValue());

                        Object element = context.findObject(id, false, true, fieldMetaData.getTypeName());

                        Array.set(array, (Integer) entry.getKey(), element);
                    }

                    return objectProvider.wrapSCOField(fieldNumber, array,
                            false, false, true);
                }

            }

            // No object defined
            if (column == null) {
                return null;
            }

            return byteContext.getObject(column, this.metaData, fieldNumber);

        } catch (Exception e) {
            throw new NucleusException(e.getMessage(), e);
        }
    }

    @Override
    public short fetchShortField(int fieldNumber) {
        try {

            Bytes columnName = getColumnName(metaData, fieldNumber);
            Bytes column = this.columns.get(columnName);

            return (Short) byteContext.getShort(column);

        } catch (Exception e) {
            throw new NucleusException(e.getMessage(), e);
        }
    }

    @Override
    public String fetchStringField(int fieldNumber) {
        try {

            Bytes columnName = getColumnName(metaData, fieldNumber);
            Bytes column = this.columns.get(columnName);

            return (String) byteContext.getString(column);

        } catch (Exception e) {
            throw new NucleusException(e.getMessage(), e);
        }
    }

}
