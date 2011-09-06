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
package com.spidertracks.datanucleus.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.IndexClause;
import org.apache.cassandra.thrift.IndexExpression;
import org.apache.cassandra.thrift.IndexOperator;
import org.apache.cassandra.thrift.IndexType;
import org.apache.cassandra.thrift.KsDef;
import org.apache.commons.codec.binary.Hex;
import org.datanucleus.exceptions.NucleusDataStoreException;
import org.junit.Ignore;
import org.junit.Test;
import org.scale7.cassandra.pelops.Bytes;
import org.scale7.cassandra.pelops.Cluster;
import org.scale7.cassandra.pelops.ColumnFamilyManager;
import org.scale7.cassandra.pelops.KeyspaceManager;
import org.scale7.cassandra.pelops.Mutator;
import org.scale7.cassandra.pelops.Pelops;
import org.scale7.cassandra.pelops.Selector;

import com.spidertracks.datanucleus.CassandraTest;
import com.spidertracks.datanucleus.utils.ClusterUtils;

/**
 * Added test to ensure pelops indexing is working correctly
 * @author Todd Nine
 * 
 */
@Ignore
public class PelopsIndexingTest extends CassandraTest {

    private static final String POOL = "Pool";
    
    private static final String KEYSPACE = "PelopsKeyspace";
    
    private static final String CF = "testingcf";

    private static final String COL1 = "col1";

    private static final String COL1_INDEX = COL1 + "_index";

    private static final String COL2 = "col2";

    private static final String COL2_INDEX = COL2 + "_index";
    
    @Test
    public void indexCreationAndQuery() throws Exception {
        Cluster cluster = new Cluster("localhost", 19160);

        KeyspaceManager keyspaceManager = new KeyspaceManager(
                ClusterUtils.getFirstAvailableNode(cluster));

    
        KsDef keyspaceDefinition = new KsDef(KEYSPACE,
                KeyspaceManager.KSDEF_STRATEGY_SIMPLE, 1,
                new ArrayList<CfDef>());

        try {
            keyspaceManager.addKeyspace(keyspaceDefinition);
        } catch (Exception e) {
            throw new NucleusDataStoreException("Not supported", e);
        }

        
        CfDef columnFamily = new CfDef(KEYSPACE, CF);
        columnFamily
                .setComparator_type(ColumnFamilyManager.CFDEF_COMPARATOR_BYTES);

        ColumnFamilyManager manager = Pelops.createColumnFamilyManager(cluster,
                KEYSPACE);

        manager.addColumnFamily(columnFamily);
        
        
        KsDef returned = keyspaceManager.getKeyspaceSchema(KEYSPACE);
        
        
        columnFamily = returned.getCf_defs().get(0);
        
        
        //now migrate again
        
        List<ColumnDef> columns = new ArrayList<ColumnDef>();
        
        ColumnDef def = new ColumnDef();

        def.setName(Bytes.fromUTF8(COL1).getBytes());
        def.setValidation_class("UTF8Type");

        def.setIndex_name(COL1_INDEX);
        def.setIndex_type(IndexType.KEYS);

        columns.add(def);
        
        
        def = new ColumnDef();

        def.setName(Bytes.fromUTF8(COL2).getBytes());
        def.setValidation_class("UTF8Type");

        def.setIndex_name(COL2_INDEX);
        def.setIndex_type(IndexType.KEYS);

        columns.add(def);
        
        columnFamily.setColumn_metadata(columns);
        
        manager.updateColumnFamily(columnFamily);
        
        
        
        
        Pelops.addPool(POOL, cluster, KEYSPACE);
        
        
        
        Mutator mutator = Pelops.createMutator(POOL);
        
        Bytes rowKey1 = Bytes.fromUTF8("row1");
        Bytes rowKey2 = Bytes.fromUTF8("row2");
        
        
        Bytes col1Value = Bytes.fromUTF8("value");
        
        Bytes col2Row1 = Bytes.fromUTF8("value21");
        
        Bytes col2Row2 = Bytes.fromUTF8("value22");
        
        
        mutator.writeColumn(CF, rowKey1, mutator.newColumn(COL1, col1Value));
        mutator.writeColumn(CF, rowKey1, mutator.newColumn(COL2, col2Row1));
        
        mutator.writeColumn(CF, rowKey2, mutator.newColumn(COL1, col1Value));
        mutator.writeColumn(CF, rowKey2, mutator.newColumn(COL2, col2Row2));
        
        
        mutator.execute(ConsistencyLevel.QUORUM);
        
        //now select what we need
        
        Selector selector = Pelops.createSelector(POOL);
        
        IndexClause clause = new IndexClause();
        clause.setStart_key(new byte[] {});
        clause.setCount(100);
        
        IndexExpression exp1 = new IndexExpression();
        exp1.setColumn_name(Bytes.fromUTF8(COL1).getBytes());
        exp1.setOp(IndexOperator.EQ);
        exp1.setValue(col1Value.getBytes());
        
        clause.addToExpressions(exp1);
        
        
        IndexExpression exp2 = new IndexExpression();
        exp2.setColumn_name(Bytes.fromUTF8(COL2).getBytes());
        exp2.setOp(IndexOperator.EQ);
        exp2.setValue(col2Row1.getBytes());
        
        clause.addToExpressions(exp2);
        
        
        
        Map<Bytes, List<Column>> results = selector.getIndexedColumns(CF, clause, Selector.newColumnsPredicate(COL2, COL2, false, 10), ConsistencyLevel.QUORUM);
        
        
        assertEquals(1, results.size());
        
        assertTrue(results.containsKey(rowKey1));
        
        List<Column> cols = results.get(rowKey1);
        
        assertEquals(1, cols.size());
        
        assertEquals(col2Row1, Bytes.fromByteArray(cols.get(0).getValue())) ;
        
        
        
        //get the second set
        
        clause = new IndexClause();
        clause.setStart_key(new byte[] {});
        clause.setCount(100);
        
        exp1 = new IndexExpression();
        exp1.setColumn_name(Bytes.fromUTF8(COL1).getBytes());
        exp1.setOp(IndexOperator.EQ);
        exp1.setValue(col1Value.getBytes());
        
        clause.addToExpressions(exp1);
        
        
        exp2 = new IndexExpression();
        exp2.setColumn_name(Bytes.fromUTF8(COL2).getBytes());
        exp2.setOp(IndexOperator.EQ);
        exp2.setValue(col2Row2.getBytes());
        
        clause.addToExpressions(exp2);
        
        
        
        results = selector.getIndexedColumns(CF, clause, Selector.newColumnsPredicate(COL2, COL2, false, 10), ConsistencyLevel.QUORUM);
        
        
        assertEquals(1, results.size());
        
        assertTrue(results.containsKey(rowKey2));
        
        cols = results.get(rowKey2);
        
        assertEquals(1, cols.size());
        
        assertEquals(col2Row2, Bytes.fromByteArray(cols.get(0).getValue())) ;
    
        
        

    }
    
    
    @Test
    public void getAll() throws Exception {
        Cluster cluster = new Cluster("localhost", 9160);
        
        Pelops.addPool(POOL, cluster, KEYSPACE);
        
        Selector selector = Pelops.createSelector(POOL);
        
        Map<Bytes, List<Column>> values = selector.getColumnsFromRows("Parent", Selector.newKeyRange(Bytes.EMPTY, Bytes.EMPTY, 100), Selector.newColumnsPredicateAll(false), ConsistencyLevel.QUORUM);
        
        
        for(Bytes key: values.keySet()){
            System.out.println(String.format("Key: %s", new String(Hex.encodeHex(key.toByteArray()))));
            
            
            for(Column col: values.get(key)){
                System.out.println(String.format("\t name: %s value: %s", new String(Hex.encodeHex(col.getName())), new String(Hex.encodeHex(col.getValue()))));
            }
            
        }
    }

}
