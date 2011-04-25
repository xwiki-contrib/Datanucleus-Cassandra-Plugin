Cassandra Datanucleus Plugin
============================

NOTE:  This plugin is now deprecated.  It has limitations in indexing, data migration, and transactions that cannot be addressed without a significant rewrite.  The requests for a JPA implementation were significantly higher than requests for this plugin.  As a result, a new version utilizing JPA has been created here.

https://github.com/riptano/hector-jpa

This is currently in production use, and is recommended for new projects.  The JPA is still in beta release phase, though it is generally stable.


Features
--------

* Basic CRUD
* Graph persistence of all loaded objects
* Cascading deletes of all dependent objects
* Subclass retrieval and persistence
* Basic secondary indexing with simple terms.  && || < <= > >= and == are supported.
* In memory ordering and paging.  Paging is not supporting without an ordering clause
* Custom conversions of your own classes via either an interface or a conversion
* Automatic creation of column families and indexes based on annotations and JDO configuration


See all unit/integration tests for details on how to annotate objects, as well as correctly query them.

Defining repositories
---------------------

Add the following repository to your pom.xml

    <repository>
        <id>maven.spidertracks.com</id>
        <name>Scale7 Maven Repo</name>
        <url>https://github.com/tnine/m2repo/raw/master</url>
        <snapshots>
            <enabled>true</enabled>
        </snapshots>
    </repository>
    
    
Add this dependency for the latest release build

	<dependency>
		<groupId>com.spidertracks.datanucleus</groupId>
		<artifactId>cassandra-jdo</artifactId>
		<version>1.1.0-0.7.0</version>
	</dependency>





Usage
=====

Developing Unit/Integration Tests
----------------------------------

Include the "test-jar" in your test dependency scope.  Extend or duplicate com.spidertracks.datanucleus.CassandraTest to create an embedded test.
Copy log4j.properties and cassandra.yaml from src/test/resources to your project's test resources.


Keyspace and CF creation
------------------------

Currently secondary indexing is automatically created for the columns of field that are annotated with the @Index annotation
if the auto table create is enabled in your dn configuration.  See the src/test/resources/META-INF/jdoconfig.xml for an example.
Note that in a production environment you SHOULD NOT set datanucleus.autoCreateSchema="true".  This uses the simple replication
strategy with a replication factor of 1.  This is ideal for rapid development on a local machine, but not for a production environment.



Querying
--------

All queries must contain at least one == operand.  For instance, this is a valid query.

	"follower == :email && lastupdate > :maxUse"

This is valid as well

	"follower == :email || follower == :email2"
	
This is an invalid query

    "follower == :email || lastUpdate > :maxUse"

Currently as of 0.7.0, Cassandra cannot support OR operations.  As a result all left and right expressions of OR ops in a query are performed independently
and the candidate results are unioned into a single result set.

Ordering and Paging
-------------------

Currently in memory ordering and paging are supported.  Since cassandra will return results in any order when using an || operand in your query
you cannot guarantee order.  Therefore ordering must be used when paging is used.  Be aware that all values from 0 to your defined start index
are loaded and sorted before being ignored.  This can be quite memory intensive.  You may get better performance by modifying the range your
query runs over.

Consistency
-----------

Currently no mechanism exists for attaching additional properties to JDO transaction.  As a workaround, you use a ThreadLocal resource to define the Consistency level. 
This is done the following way.

	Consistency.set(ConsistencyLevel.QUORUM)
	
Every operation that communicates with Cassandra will use this consistency level.  This class will not reset when the transaction completes.  As such you should be when
using it to set the Consistency level for the operation you're about to perform.  Otherwise a thread in a thread pool could reuse a consistency level from a previous operation.
See the Spring Integration section for utilities that make this easier if you are a user of the Spring framework.
	


Inheritance
-----------

All inheritance requires a discriminator strategy if there more than 1 concrete class is in the inheritance tree.
This is due to a limitation of the plugin.  Note that a CF per concrete class is much less efficient than
storing all subclasses in the parent CF.  

Storing the subclasses in the parent CF requires O(2) reads.  

1. One to read the class type and instantiate the instance. O(1)
2. Read the columns and populate the object. O(1)

Storing the subclass in its own table requires O(n+1) reads where n is the number of children from the queried class

1. Recursively read all CFs in the inheritance structure until we find a result O(n)
2. Read the columns and populate the object O(1) 

KNOWN ISSUE!!
Querying requires a full inheritance tree at execution time.  This class is used to determine subclasses.

http://www.datanucleus.org/javadocs/core/latest/org/datanucleus/metadata/MetaDataManager.html#getSubclassesForClass(java.lang.String, boolean)

This class is only aware of PC objects that have been loaded at runtime.  For instance, if you have the following class structure.

-> = extends
A
B -> A
C -> A

And you query on class A, no subclasses will be returned.  As a work around, in your DAO, simply instantiate all possible sub classes.  This will resolve the issue.  I'm working with DN to resolve this issue. 

public MyDao(){
	new A();
	new B();
	new C();
}


Byte Mapping
-----------

All conversion to bytes for storage is now performed by called the com.spidertracks.datanucleus.convert.ByteConverterContext.  All fields including row keys are converted via this method.  
See the javadoc for details.  There are 2 options to map types to bytes.

ByteConverter:

If you wish to de-couple your entities from how they are persisted, you may implement your own byte converter and add it to the conversion properties file.  This file will be loaded on startup,
and will override any defaults.  By default all Java primitives are supported as well as java.util.UUID and com.eaio.uuid.UUID.  

ByteAware:

If implement this interface in an object that will be written to a column, the appropriate methods are invoked for creating a ByteBuffer that represents the underlying object.  Take care when writing
to the Buffer that rewind() is invoked before returning.  Otherwise no bytes will be written.  See com.spidertracks.datanucleus.basic.model.UnitDataKey in the test sources for an example.

Byte conversions are determined in the following order, then cached for future use.

1. Lookup an existing ByteConverter.  If one exists, use it.
2. Check if the class implements ByteAware.  If so, instantiate a wrapper ByteConverter for the class type and cache it for future use.
3. Check if the class has a Datanucleus ObjectLong converter. If so, wrap it with an ObjectLongWrapperConverter which will convert the object to a Long, then use the Long's ByteConverter
4. Check if the class has a Datanucleus ObjectString converter. If so, wrap it with an ObjectStringWrapperConverter which will convert the object to a String, then use the String's ByteConverter
5. If none of the above apply.  Use the serializer defined in the JDO config to serialize the object to bytes.


Spring Integration
------------------

I have created a utility project to enable easier integration with spring using Cassandra's ConsistencyLevel and annotations.  You utilize this functionality by including
this project as a dependency and following the example.  <http://github.com/tnine/Datanucleus-Cassandra-Spring>

Building
--------

You no longer need to manually install the cassandra dependencies when building.  They will be installed and deployed automatically by maven.


Reporting Bugs
--------------

If you find a bug.  Please fork the code, create a test case, and submit a pull request with the issue.  This will allow me to quickly determine the cause and resolve the issue.

Changes
-------

This will be the final change to the underlying storage mechanism of sets.  Collections now use a <fieldname>0x00<value> format, and maps use a <fieldname>0x00<key>:<value> storage format
This will be the format that will ultimately be used for paging proxies.

Roadmap
-------

1. Upgrade as Pelops client improves
2. Make paging/iteration possible on large associative sets.  The storage changes to support large sets is complete, however the paging size is not.

Special Thanks
--------------

Thanks to Dominic and Dan of s7 for making [Pelops]("http://http://github.com/s7/scale7-pelops") and rapidly responding to pull notifications and feature improvements.  Without their
elegant client, developing this plugin would have been significantly more difficult.




