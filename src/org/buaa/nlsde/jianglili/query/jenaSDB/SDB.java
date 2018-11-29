package org.buaa.nlsde.jianglili.query.jenaSDB;

import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.sdb.SDBException;
import org.apache.jena.sdb.SDBFactory;
import org.apache.jena.sdb.Store;
import org.apache.jena.sdb.StoreDesc;
import org.apache.jena.sdb.shared.Access;
import org.apache.jena.sdb.sql.JDBC;
import org.apache.jena.sdb.sql.SDBConnection;
import org.apache.jena.sdb.store.DatabaseType;
import org.apache.jena.sdb.store.DatasetStore;
import org.apache.jena.sdb.store.LayoutType;
import org.apache.jena.sdb.store.StoreFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import static org.apache.jena.atlas.logging.LogCtl.setLog4j;

/**
 * Created by jianglili on 2017/2/2.
 */
public class SDB {
    public static void main(String[] args) {

            Store store = SDBFactory.connectStore("E:\\benchmarks\\LUBM\\count\\sdbcount\\sdb-mysql-innodb-lubm1.ttl") ;
            Model model = SDBFactory.connectDefaultModel(store) ;

            StmtIterator sIter = model.listStatements() ;
            for ( ; sIter.hasNext() ; )
            {
                Statement stmt = sIter.nextStatement() ;
                System.out.println(stmt) ;
            }
            sIter.close() ;
            store.close() ;

    }

    /** Example of the usual way to connect store and issue a query.
     *  A description of the connection and store is read from file "sdb.ttl".
     *  Use and password come from environment variables SDB_USER and SDB_PASSWORD.
     */
    static public void main2(String...argv)
    {
        String queryString = "SELECT * { ?s ?p ?o }" ;
        Query query = QueryFactory.create(queryString) ;
        Store store = SDBFactory.connectStore("sdb.ttl") ;

        // Must be a DatasetStore to trigger the SDB query engine.
        // Creating a graph from the Store, and adding it to a general
        // purpose dataset will not necesarily exploit full SQL generation.
        // The right answers will be obtained but slowly.

        Dataset ds = DatasetStore.create(store) ;
        QueryExecution qe = QueryExecutionFactory.create(query, ds) ;
        try {
            ResultSet rs = qe.execSelect() ;
            ResultSetFormatter.out(rs) ;
        } finally { qe.close() ; }

        // Close the SDB conenction which also closes the underlying JDBC connection.
        store.getConnection().close() ;
        store.close() ;
    }

    /** Connect to a store using API calls. */
    static public void main3(String...argv)
    {
        String queryString = "SELECT * { ?s ?p ?o }" ;
        Query query = QueryFactory.create(queryString) ;

        StoreDesc storeDesc = new StoreDesc(LayoutType.LayoutTripleNodesHash, DatabaseType.Derby) ;

        JDBC.loadDriverDerby() ;    // Multiple choices for Derby - load the embedded driver
        String jdbcURL = "jdbc:derby:DB/SDB2";

        // Passing null for user and password causes them to be extracted
        // from the environment variables SDB_USER and SDB_PASSWORD
        SDBConnection conn = new SDBConnection(jdbcURL, null, null) ;

        // Make store from connection and store description.
        Store store = SDBFactory.connectStore(conn, storeDesc) ;

        Dataset ds = DatasetStore.create(store) ;
        QueryExecution qe = QueryExecutionFactory.create(query, ds) ;
        try {
            ResultSet rs = qe.execSelect() ;
            ResultSetFormatter.out(rs) ;
        } finally { qe.close() ; }
        store.close() ;
    }

    /** Example of use with the Jena API for models. */
    static public void main4(String...argv)
    {
        Store store = StoreFactory.create("sdb.ttl") ;
        Model model = SDBFactory.connectDefaultModel(store) ;

        StmtIterator sIter = model.listStatements() ;
        for ( ; sIter.hasNext() ; )
        {
            Statement stmt = sIter.nextStatement() ;
            System.out.println(stmt) ;
        }
        sIter.close() ;
        store.close() ;
    }

    static { setLog4j() ; }

    public static void main5(String...argv)
    {
        String jdbcURL = String.format("jdbc:mysql:%s", "DB/test2-hash") ;
        JDBC.loadDriverDerby() ;

        // Setup - make the JDBC connection and read the store description once.
        Connection jdbc = makeConnection(jdbcURL) ;
        //StoreDesc storeDesc = StoreDesc.read("sdb-store.ttl") ;

        // Make a store description without any connection information.
        StoreDesc storeDesc = new StoreDesc(LayoutType.LayoutTripleNodesHash,
                DatabaseType.MySQL) ;

        // Make some calls to the store, using the same JDBC connection and store description.
        System.out.println("Subjects: ") ;
        query("SELECT DISTINCT ?s { ?s ?p ?o }", storeDesc, jdbc) ;
        System.out.println("Predicates: ") ;
        query("SELECT DISTINCT ?p { ?s ?p ?o }", storeDesc, jdbc) ;
        System.out.println("Objects: ") ;
        query("SELECT DISTINCT ?o { ?s ?p ?o }", storeDesc, jdbc) ;
    }

    public static void query(String queryString, StoreDesc storeDesc, Connection jdbcConnection)
    {
        Query query = QueryFactory.create(queryString) ;

        SDBConnection conn = new SDBConnection(jdbcConnection) ;

        Store store = StoreFactory.create(storeDesc, conn) ;

        Dataset ds = DatasetStore.create(store) ;
        QueryExecution qe = QueryExecutionFactory.create(query, ds) ;
        try {
            ResultSet rs = qe.execSelect() ;
            ResultSetFormatter.out(rs) ;
        } finally { qe.close() ; }
        // Does not close the JDBC connection.
        // Do not call : store.getConnection().close() , which does close the underlying connection.
        store.close() ;
    }

    public static Connection makeConnection(String jdbcURL)
    {
        try {
            return DriverManager.getConnection(jdbcURL,
                    Access.getUser(),
                    Access.getPassword()) ;
        } catch (SQLException ex)
        {
            throw new SDBException("SQL Exception while connecting to database: "+jdbcURL+" : "+ex.getMessage()) ;
        }
    }
}
