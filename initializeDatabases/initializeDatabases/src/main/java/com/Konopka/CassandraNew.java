package com.Konopka;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;

import java.net.InetSocketAddress;
import java.util.List;


public class CassandraNew {


    private static CqlSession session;
    private static CassandraNew instance;


    public static CassandraNew getInstance() {
        if (instance == null) {
            instance = new CassandraNew();
        }
        return instance;
    }

    private void ConnectCassandra() {
        try {
            session = CqlSession.builder()
                    .addContactPoint(new InetSocketAddress("localhost", 9042))
                    .withLocalDatacenter("datacenter1")
                    .build();

            System.out.println("Połączono z Cassandra!");
        } catch (Exception e) {
            System.err.println("Nie udało się połączyć!");
            e.printStackTrace();
        }
    }


    CassandraNew() {
        ConnectCassandra();
        CreateKeySpace();
        dropTables();
        createCustomers();
        insertCustomers();
        createBranches();
        insertBranches();
        createCategories();
        insertCategories();
        createOrderDetailsByOrder();
        insertOrderDetailsByOrder();
        createOrdersByBranch();
        insertOrdersByBranch();
        createOrdersByUser();
        insertOrdersByUser();
    }


    private static void CreateKeySpace() {
        try {
            session.execute("CREATE KEYSPACE IF NOT EXISTS cassandraSpeedTestDb " +
                    "WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};");
            System.out.println("udalo sie stworzyc keyspace");
        } catch (Exception e) {
            System.err.println("Blad podczas tworzenia keyspace");
            e.printStackTrace();
        }
    }


    private static void createCustomers() {
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


    private static void insertCustomers() {
        String cqlCommand = "COPY cassandraSpeedTestDb.customers (USERID,USERNAME_,NAMESURNAME, STATUS_, USERGENDER, USERBIRTHDATE, REGION, CITY ,TOWN , DISTRICT, ADDRESS) FROM '/var/lib/cassandra-files/Customers_ENG.csv' WITH DELIMITER=';' AND HEADER=TRUE;";
        List<String> command = List.of(
                "docker",
                "exec",
                "cassandra-db",
                "cqlsh",
                "-e",
                cqlCommand
        );
        try {
            ProcessBuilder processBuilder = new ProcessBuilder(command);
            processBuilder.inheritIO();
            System.out.println("Uruchamianie importu z cqlsh...");
            Process process = processBuilder.start();
            int exitCode = process.waitFor();
            if (exitCode == 0) {
                System.out.println("Import zakończony pomyślnie.");
            } else {
                System.err.println("Import zakończony błędem! Kod wyjścia: " + exitCode);
            }

        } catch (Exception e) {
            System.err.println("Wystąpił błąd podczas wywoływania procesu Docker/cqlsh!");
            e.printStackTrace();
        }
    }



    private static void createBranches() {
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


    private static void insertBranches() {
        String cqlCommand = """
                    COPY cassandraSpeedTestDb.branches (BRANCH_ID,REGION,CITY, TOWN, BRANCH_TOWN, LAT, LON)
                    FROM '/var/lib/cassandra-files/Branches.csv'
                    WITH DELIMITER=',' AND HEADER=TRUE
                    """;
        List<String> command = List.of(
                "docker",
                "exec",
                "cassandra-db",
                "cqlsh",
                "-e",
                cqlCommand
        );
        try {
            ProcessBuilder processBuilder = new ProcessBuilder(command);
            processBuilder.inheritIO();
            System.out.println("Uruchamianie importu z cqlsh...");
            Process process = processBuilder.start();
            int exitCode = process.waitFor();
            if (exitCode == 0) {
                System.out.println("Import zakończony pomyślnie.");
            } else {
                System.err.println("Import zakończony błędem! Kod wyjścia: " + exitCode);
            }

        } catch (Exception e) {
            System.err.println("Wystąpił błąd podczas wywoływania procesu Docker/cqlsh!");
            e.printStackTrace();
        }
    }


    private static void createCategories() {
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


    private static void insertCategories() {
        String cqlCommand = """
                    COPY cassandraSpeedTestDb.categories (ITEMID, CATEGORY1, CATEGORY1_ID, CATEGORY2, CATEGORY2_ID, CATEGORY3, CATEGORY3_ID,CATEGORY4, CATEGORY4_ID, BRAND, ITEMCODE, ITEMNAME)
                    FROM '/var/lib/cassandra-files/Categories_ENG.csv'
                    WITH DELIMITER=';' AND HEADER=TRUE
                    """;
        List<String> command = List.of(
                "docker",
                "exec",
                "cassandra-db",
                "cqlsh",
                "-e",
                cqlCommand
        );
        try {
            ProcessBuilder processBuilder = new ProcessBuilder(command);
            processBuilder.inheritIO();
            System.out.println("Uruchamianie importu z cqlsh...");
            Process process = processBuilder.start();
            int exitCode = process.waitFor();
            if (exitCode == 0) {
                System.out.println("Import zakończony pomyślnie.");
            } else {
                System.err.println("Import zakończony błędem! Kod wyjścia: " + exitCode);
            }

        } catch (Exception e) {
            System.err.println("Wystąpił błąd podczas wywoływania procesu Docker/cqlsh!");
            e.printStackTrace();
        }
    }




    private static void createOrderDetailsByOrder() {
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


    private static void insertOrderDetailsByOrder() {
        String cqlCommand = """
                    COPY cassandraSpeedTestDb.order_details_by_order (ORDERID, ORDERDETAILID, AMOUNT, UNITPRICE, TOTALPRICE, ITEMID, ITEMCODE)
                    FROM '/var/lib/cassandra-files/Order_Details_final.csv'
                    WITH DELIMITER=',' AND HEADER=TRUE
                    """;
        List<String> command = List.of(
                "docker",
                "exec",
                "cassandra-db",
                "cqlsh",
                "-e",
                cqlCommand
        );
        try {
            ProcessBuilder processBuilder = new ProcessBuilder(command);
            processBuilder.inheritIO();
            System.out.println("Uruchamianie importu z cqlsh...");
            Process process = processBuilder.start();
            int exitCode = process.waitFor();
            if (exitCode == 0) {
                System.out.println("Import zakończony pomyślnie.");
            } else {
                System.err.println("Import zakończony błędem! Kod wyjścia: " + exitCode);
            }

        } catch (Exception e) {
            System.err.println("Wystąpił błąd podczas wywoływania procesu Docker/cqlsh!");
            e.printStackTrace();
        }
    }


    private static void createOrdersByBranch() {
        session.execute("""
                    CREATE TABLE IF NOT EXISTS cassandraSpeedTestDb.orders_by_branch (
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


    private static void insertOrdersByBranch() {
        String cqlCommand = """
                    COPY cassandraSpeedTestDb.orders_by_branch (ORDERID, BRANCH_ID, DATE_, USERID, NAMESURNAME, TOTALBASKET)
                    FROM '/var/lib/cassandra-files/Orders_final.csv'
                    WITH DELIMITER=',' AND HEADER=TRUE
                    """;
        List<String> command = List.of(
                "docker",
                "exec",
                "cassandra-db",
                "cqlsh",
                "-e",
                cqlCommand
        );
        try {
            ProcessBuilder processBuilder = new ProcessBuilder(command);
            processBuilder.inheritIO();
            System.out.println("Uruchamianie importu z cqlsh...");
            Process process = processBuilder.start();
            int exitCode = process.waitFor();
            if (exitCode == 0) {
                System.out.println("Import zakończony pomyślnie.");
            } else {
                System.err.println("Import zakończony błędem! Kod wyjścia: " + exitCode);
            }

        } catch (Exception e) {
            System.err.println("Wystąpił błąd podczas wywoływania procesu Docker/cqlsh!");
            e.printStackTrace();
        }
    }

    private static void createOrdersByUser() {
        session.execute("""
                CREATE TYPE IF NOT EXISTS cassandraSpeedTestDb.item_details (
                    orderdetailid text,
                    itemid int,
                    itemcode text,
                    itemname text,
                    amount int,
                    unitprice text,
                    totalprice text
                );
                """);

        session.execute("""
                    CREATE TABLE IF NOT EXISTS cassandraSpeedTestDb.orders_by_user (
                        USERID int,
                        DATE_ timestamp,
                        ORDERID bigint,
                        BRANCH_ID text,
                        TOTALBASKET text,
                        NAMESURNAME text,
                        ORDER_ITEMS list<frozen<item_details>>,
                        PRIMARY KEY (USERID, DATE_, ORDERID)
                    ) WITH CLUSTERING ORDER BY (DATE_ DESC);
                """);
    }


    private static void insertOrdersByUser() {
        String cqlCommand = """
                    COPY cassandraSpeedTestDb.orders_by_user (USERID, DATE_, ORDERID, BRANCH_ID, TOTALBASKET, NAMESURNAME, ORDER_ITEMS)
                    FROM '/var/lib/cassandra-files/orders_by_user.csv'
                    WITH DELIMITER=';' AND HEADER=TRUE
                    """;
        List<String> command = List.of(
                "docker",
                "exec",
                "cassandra-db",
                "cqlsh",
                "-e",
                cqlCommand
        );
        try {
            ProcessBuilder processBuilder = new ProcessBuilder(command);
            processBuilder.inheritIO();
            System.out.println("Uruchamianie importu z cqlsh...");
            Process process = processBuilder.start();
            int exitCode = process.waitFor();
            if (exitCode == 0) {
                System.out.println("Import zakończony pomyślnie.");
            } else {
                System.err.println("Import zakończony błędem! Kod wyjścia: " + exitCode);
            }

        } catch (Exception e) {
            System.err.println("Wystąpił błąd podczas wywoływania procesu Docker/cqlsh!");
            e.printStackTrace();
        }
    }


    private static void dropTables() {
        String[] dropQueries = {
                "DROP TABLE IF EXISTS cassandraSpeedTestDb.customers;",
                "DROP TABLE IF EXISTS cassandraSpeedTestDb.branches;",
                "DROP TABLE IF EXISTS cassandraSpeedTestDb.categories;",
                "DROP TABLE IF EXISTS cassandraSpeedTestDb.order_details_by_order;",
                "DROP TABLE IF EXISTS cassandraSpeedTestDb.orders_by_branch;",
                "DROP TABLE IF EXISTS cassandraSpeedTestDb.orders_by_user;",
                "DROP TYPE IF EXISTS cassandraSpeedTestDb.item_details;"
        };

        for (String query : dropQueries) {
            session.execute(query);
        }

        System.out.println("All tables dropped successfully.");
    }

}