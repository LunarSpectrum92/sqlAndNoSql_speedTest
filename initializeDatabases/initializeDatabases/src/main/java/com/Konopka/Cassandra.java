package com.Konopka;

import com.datastax.driver.core.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
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
        dropTables();
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
        System.out.println("insertCustomers zakonczone");
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
        System.out.println("insertBranches zakonczone");

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
        System.out.println("insertCategories zakonczone");

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
        System.out.println("insertProductsByCategory zakonczone");

    }



    public static void createOrderDetailsByOrder(){
        session.execute("""
        CREATE TABLE IF NOT EXISTS cassandraSpeedTestDb.order_details_by_order (
           ORDERID bigint,
           ORDERDETAILID bigint,
           AMOUNT int,
           UNITPRICE text,
           TOTALPRICE text,
           ITEMID bigint,
           ITEMCODE bigint,
           PRIMARY KEY ((ORDERID), ORDERDETAILID)
        );
    """);
    }


//    public static void insertOrderDetailsByOrder() {
//        try {
//            Statement stmt = sqlConn.createStatement();
//            ResultSet rs = stmt.executeQuery("SELECT * FROM order_details;");
//
//            PreparedStatement ps = session.prepare(
//                    "INSERT INTO order_details_by_order (ORDERID, ORDERDETAILID, AMOUNT, UNITPRICE, TOTALPRICE, ITEMID, ITEMCODE) " +
//                            "VALUES (?, ?, ?, ?, ?, ?, ?)"
//            );
//
//            while (rs.next()) {
//                BoundStatement bound = ps.bind(
//                        rs.getLong("ORDERID"),
//                        rs.getLong("ORDERDETAILID"),
//                        rs.getInt("AMOUNT"),
//                        rs.getString("UNITPRICE"),
//                        rs.getString("TOTALPRICE"),
//                        rs.getLong("ITEMID"),
//                        rs.getLong("ITEMCODE")
//                );
//                session.execute(bound);
//            }
//        } catch (SQLException e) {
//            throw new RuntimeException(e);
//        }
//    }

    public static void insertOrderDetailsByOrder() {
        String csvFile = "C:\\Users\\Jarek\\Documents\\projekty\\sqlAndNoSql_speedTest\\data\\Order_Details.csv";
        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
            String line = br.readLine();
            PreparedStatement stmt = session.prepare(
                    "INSERT INTO order_details_by_order (ORDERID, ORDERDETAILID, AMOUNT, UNITPRICE, TOTALPRICE, ITEMID, ITEMCODE) VALUES (?, ?, ?, ?, ?, ?, ?)"
            );
            int i =0;
            while ((line = br.readLine()) != null) {
                i++;
                String[] values = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
                String unitPriceText = values[3].replace("\"", "");
                String totalPriceText = values[4].replace("\"", "");
                session.execute(stmt.bind(
                        Long.parseLong(values[0]),
                        Long.parseLong(values[1]),
                        Integer.parseInt(values[2]),
                        unitPriceText,
                        totalPriceText,
                        Long.parseLong(values[5]),
                        Long.parseLong(values[6])
                ));
                if(i > 50000){
                    break;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("insertOrderDetailsByOrder zakonczone");
    }



    public static void createOrdersByBranch(){
        session.execute("""
        CREATE TABLE IF NOT EXISTS orders_by_branch (
             ORDERID BIGINT,
             BRANCH_ID text,
             DATE_ timestamp,
             USERID BIGINT,
             NAMESURNAME text,
             TOTALBASKET text,
             PRIMARY KEY ((BRANCH_ID), DATE_, ORDERID)
        )WITH CLUSTERING ORDER BY (DATE_ DESC);
    """);
    }

    public static void insertOrdersByBranch() {
        String ordersByBranchCsv = "C:\\Users\\Jarek\\Documents\\projekty\\sqlAndNoSql_speedTest\\data\\Order_Details.csv";
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        try(BufferedReader br = new BufferedReader(new FileReader(ordersByBranchCsv))){
            String line = br.readLine();
            PreparedStatement stmt = session.prepare(
                    "INSERT INTO orders_by_branch (ORDERID, BRANCH_ID, DATE_, USERID, NAMESURNAME, TOTALBASKET) " + "VALUES (?, ?, ?, ?, ?, ?)"
            );
            int i = 0;
            while ((line = br.readLine()) != null) {
                i++;
                String[] values = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
                String branchId = values[1].replace("\"", "");
                String totalBasket = values[5].replace("\"", "");
                LocalDateTime localDateTime = LocalDateTime.parse(values[2], formatter);
                java.time.Instant instant = localDateTime.toInstant(ZoneOffset.UTC);

                session.execute(stmt.bind(
                        Long.parseLong(values[0]),
                        branchId,
                        instant,
                        Long.parseLong(values[3]),
                        values[4],
                        totalBasket
                ));
                if(i > 50000){
                    break;
                }
            }

        }catch (IOException e){
            e.printStackTrace();
        }
    }


//    //insert zmniejszony do 1000 rekordów do testów ponieważ za długo sie ładuje
//    public static void insertOrdersByBranch() {
//        try {
//            Statement stmt = sqlConn.createStatement();
//            ResultSet rs = stmt.executeQuery("SELECT * FROM order_details;");
//
//            PreparedStatement ps = session.prepare(
//                    "INSERT INTO orders_by_branch (ORDERID, BRANCH_ID, DATE_, USERID, NAMESURNAME, TOTALBASKET) " +
//                            "VALUES (?, ?, ?, ?, ?, ?)"
//            );
//            int i = 0;
//            while (rs.next()) {
//                i++;
//                BoundStatement bound = ps.bind(
//                        rs.getLong("ORDERID"),
//                        rs.getString("BRANCH_ID"),
//                        rs.getTimestamp("DATE_"),
//                        rs.getLong("USERID"),
//                        rs.getString("NAMESURNAME"),
//                        rs.getString("TOTALBASKET")
//                );
//                session.execute(bound);
//                if(i > 1000){
//                    break;
//                }
//            }
//        } catch (SQLException e) {
//            throw new RuntimeException(e);
//        }
//        System.out.println("insertOrdersByBranch zakonczone");
//
//    }




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























    public static void dropTables() {
        String[] dropQueries = {
                "DROP TABLE IF EXISTS cassandraSpeedTestDb.customers;",
                "DROP TABLE IF EXISTS cassandraSpeedTestDb.branches;",
                "DROP TABLE IF EXISTS cassandraSpeedTestDb.categories;",
                "DROP TABLE IF EXISTS cassandraSpeedTestDb.products_by_category;",
                "DROP TABLE IF EXISTS cassandraSpeedTestDb.order_details_by_order;",
                "DROP TABLE IF EXISTS cassandraSpeedTestDb.orders_by_branch;"
        };

        for (String query : dropQueries) {
            session.execute(query);
        }

        System.out.println("All tables dropped successfully.");
    }

}




