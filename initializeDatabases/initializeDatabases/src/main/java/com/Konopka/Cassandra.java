package com.Konopka;

import com.datastax.driver.core.*;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;

public class Cassandra {

    private static Cassandra instance;
    private static Connection sqlConn;
    private static Cluster cluster;
    private static Session session;



    private Cassandra(){
        sqlConn  = PostgreSql.getConnection();
        connectCassandra();
        CreateKeySpace();
        createTablesAndInsertData();
    }

    private static void connectCassandra(){
        cluster = Cluster.builder()
                .addContactPoint("localhost")
                .withPort(9042)
                .build();
        session = cluster.connect("cassandraSpeedTestDb");
        System.out.println("Połączono z Cassandra!");
    }


    public static Cassandra getInstance(){
        if(instance == null){
            instance = new Cassandra();
        }
        return instance;
    }


    public static void CreateKeySpace(){
        session.execute("CREATE KEYSPACE IF NOT EXISTS cassandraSpeedTestDb " +
                "WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};");
    }

    public static void createTablesAndInsertData(){
        createCustomers();
        insertCustomers();
        createBranches();
        insertBranches();
        createCategories();
        insertCategories();
        createProductsByCategory();
        insertProductsByCategory();
        createOrderDetailsByOrder();
        insertOrderDetailsByOrder();
        createOrdersByBranch();
        insertOrdersByBranch();
    }


//    cassandra:
//  1. customers
//	2. branches
//	3. categories
//	4. products_by_category
//	5. order_details_by_order
//	6. orders_by_branch


    public static void createCustomers(){
        session.execute("""
        CREATE TABLE IF NOT EXISTS cassandraSpeedTestDb.customers (
            USERID text PRIMARY KEY,
            USERNAME_ text,
            NAMESURNAME text,
            STATUS_ text,
            USERGENDER text,
            USERBIRTHDATE date,
            REGION text,
            CITY text,
            TOWN text,
            DISTRICT text,
            ADDRESS text
        );
    """);
    }




    public static void insertCustomers() {
        try {
            Statement stmt = sqlConn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT * FROM customers_eng;");

            PreparedStatement ps = session.prepare(
                    "INSERT INTO customers (USERID, USERNAME_, NAMESURNAME, STATUS_, USERGENDER, USERBIRTHDATE, REGION, CITY, TOWN, DISTRICT, ADDRESS) " +
                            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
            );

            while (rs.next()) {
                BoundStatement bound = ps.bind(
                        rs.getString("USERID"),
                        rs.getString("USERNAME_"),
                        rs.getString("NAMESURNAME"),
                        String.valueOf(rs.getBoolean("STATUS_")),
                        rs.getString("USERGENDER"),
                        LocalDate.fromMillisSinceEpoch(rs.getDate("USERBIRTHDATE").getTime()),
                        rs.getString("REGION"),
                        rs.getString("CITY"),
                        rs.getString("TOWN"),
                        rs.getString("DISTRICT"),
                        rs.getString("ADDRESSTEXT")
                );
                session.execute(bound);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }




    public static void createBranches() {
        session.execute("""
        CREATE TABLE IF NOT EXISTS cassandraSpeedTestDb.branches (
            BRANCH_ID text,
            REGION text,
            CITY text,
            TOWN text,
            BRANCH_TOWN text,
            LAT double,
            LON double,
            PRIMARY KEY (BRANCH_ID)
        );
    """);
    }

    public static void insertBranches() {
        try {
            Statement stmt = sqlConn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT * FROM branches;");

            PreparedStatement ps = session.prepare(
                    "INSERT INTO branches (BRANCH_ID, REGION, CITY, TOWN, BRANCH_TOWN, LAT, LON) " +
                            "VALUES (?, ?, ?, ?, ?, ?, ?)"
            );

            while (rs.next()) {
                BoundStatement bound = ps.bind(
                        rs.getString("BRANCH_ID"),
                        rs.getString("REGION"),
                        rs.getString("CITY"),
                        rs.getString("TOWN"),
                        rs.getString("BRANCH_TOWN"),
                        rs.getDouble("LAT"),
                        rs.getDouble("LON")
                );
                session.execute(bound);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }




    public static void createCategories() {
        session.execute("""
        CREATE TABLE IF NOT EXISTS cassandraSpeedTestDb.categories (
           ITEMID int,
           CATEGORY1 text,
           CATEGORY1_ID text,
           CATEGORY2 text,
           CATEGORY2_ID text,
           CATEGORY3 text,
           CATEGORY3_ID text,
           CATEGORY4 text,
           CATEGORY4_ID text,
           BRAND text,
           ITEMCODE text,
           ITEMNAME text,
           PRIMARY KEY (ITEMID)
        );
    """);
    }



    public static void insertCategories() {
        try {
            Statement stmt = sqlConn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT * FROM categories_eng;");

            PreparedStatement ps = session.prepare(
                    "INSERT INTO categories (ITEMID, CATEGORY1, CATEGORY1_ID, CATEGORY2, CATEGORY2_ID, CATEGORY3, CATEGORY3_ID, CATEGORY4, CATEGORY4_ID, BRAND, ITEMCODE, ITEMNAME) " +
                            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
            );

            while (rs.next()) {
                BoundStatement bound = ps.bind(
                        rs.getInt("ITEMID"),
                        rs.getString("CATEGORY1"),
                        rs.getString("CATEGORY1_ID"),
                        rs.getString("CATEGORY2"),
                        rs.getString("CATEGORY2_ID"),
                        rs.getString("CATEGORY3"),
                        rs.getString("CATEGORY3_ID"),
                        rs.getString("CATEGORY4"),
                        rs.getString("CATEGORY4_ID"),
                        rs.getString("BRAND"),
                        rs.getString("ITEMCODE"),
                        rs.getString("ITEMNAME")
                );
                session.execute(bound);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


    public static void createProductsByCategory(){
        session.execute("""
        CREATE TABLE IF NOT EXISTS cassandraSpeedTestDb.products_by_category (
           CATEGORY1 text,
           CATEGORY2 text,
           CATEGORY3 text,
           CATEGORY4 text,
           ITEMID int,
           BRAND text,
           ITEMCODE text,
           ITEMNAME text,
           PRIMARY KEY ((CATEGORY1, CATEGORY2, CATEGORY3), ITEMID)
        );
    """);
    }

    public static void insertProductsByCategory() {
        try {
            Statement stmt = sqlConn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT * FROM categories_eng;");

            PreparedStatement ps = session.prepare(
                    "INSERT INTO products_by_category (CATEGORY1, CATEGORY2, CATEGORY3, CATEGORY4, ITEMID, BRAND, ITEMCODE, ITEMNAME) " +
                            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
            );

            while (rs.next()) {
                BoundStatement bound = ps.bind(
                        rs.getString("CATEGORY1"),
                        rs.getString("CATEGORY2"),
                        rs.getString("CATEGORY3"),
                        rs.getString("CATEGORY4"),
                        rs.getInt("ITEMID"),
                        rs.getString("BRAND"),
                        rs.getString("ITEMCODE"),
                        rs.getString("ITEMNAME")
                );
                session.execute(bound);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }



    public static void createOrderDetailsByOrder(){
        session.execute("""
        CREATE TABLE IF NOT EXISTS cassandraSpeedTestDb.order_details_by_order (
           ORDERID text,
           ORDERDETAILID text,
           AMOUNT int,
           UNITPRICE decimal,
           TOTALPRICE decimal,
           ITEMID int,
           ITEMCODE text,
           PRIMARY KEY ((ORDERID), ORDERDETAILID)
        );
    """);
    }


    public static void insertOrderDetailsByOrder() {
        try {
            Statement stmt = sqlConn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT * FROM order_details;");

            PreparedStatement ps = session.prepare(
                    "INSERT INTO order_details_by_order (ORDERID, ORDERDETAILID, AMOUNT, UNITPRICE, TOTALPRICE, ITEMID, ITEMCODE) " +
                            "VALUES (?, ?, ?, ?, ?, ?, ?)"
            );

            while (rs.next()) {
                BoundStatement bound = ps.bind(
                        rs.getString("ORDERID"),
                        rs.getString("ORDERDETAILID"),
                        rs.getInt("AMOUNT"),
                        rs.getBigDecimal("UNITPRICE"),
                        rs.getBigDecimal("TOTALPRICE"),
                        rs.getInt("ITEMID"),
                        rs.getString("ITEMCODE")
                );
                session.execute(bound);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }




    public static void createOrdersByBranch(){
        session.execute("""
        CREATE TABLE IF NOT EXISTS orders_by_branch (
             ORDERID text,
             BRANCH_ID text,
             DATE_ timestamp,
             USERID text,
             NAMESURNAME text,
             TOTALBASKET decimal,
             PRIMARY KEY ((BRANCH_ID), DATE_, ORDERID)
        )WITH CLUSTERING ORDER BY (DATE_ DESC);
    """);
    }

    public static void insertOrdersByBranch() {
        try {
            Statement stmt = sqlConn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT * FROM order_details;");

            PreparedStatement ps = session.prepare(
                    "INSERT INTO orders_by_branch (ORDERID, BRANCH_ID, DATE_, USERID, NAMESURNAME, TOTALBASKET) " +
                            "VALUES (?, ?, ?, ?, ?, ?)"
            );

            while (rs.next()) {
                BoundStatement bound = ps.bind(
                        rs.getString("ORDERID"),
                        rs.getString("BRANCH_ID"),
                        rs.getTimestamp("DATE_"),
                        rs.getString("USERID"),
                        rs.getString("NAMESURNAME"),
                        rs.getBigDecimal("TOTALBASKET")
                );
                session.execute(bound);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }




    public static void createOrdersByUser() {
        session.execute("""
    // Najpierw tworzymy typ UDT, aby zagnieździć szczegóły pozycji zamówienia
    CREATE TYPE IF NOT EXISTS cassandraSpeedTestDb.item_details (
        orderdetailid text,
        itemid int,
        itemcode text,
        itemname text,      // Denormalizacja z 'categories'
        amount int,
        unitprice decimal,
        totalprice decimal
    );  
    """);

        session.execute("""
    CREATE TABLE IF NOT EXISTS cassandraSpeedTestDb.orders_by_user (
        // Klucze główne
        USERID uuid, // Klucz partycjonujący
        DATE_ timestamp, // Klucz klastrujący 1
        ORDERID uuid, // Klucz klastrujący 2

        // Dane z Orders
        BRANCH_ID text,
        TOTALBASKET decimal,

        // Denormalizacja z Customers
        NAMESURNAME text, 

        // Kluczowy "join": szczegóły pozycji zamówienia
        ORDER_ITEMS list<frozen<item_details>>,
        
        PRIMARY KEY (USERID, DATE_, ORDERID)
    ) WITH CLUSTERING ORDER BY (DATE_ DESC);
""");
    }






}




