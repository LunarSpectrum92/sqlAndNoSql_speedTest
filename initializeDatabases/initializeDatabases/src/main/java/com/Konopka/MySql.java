package com.Konopka;

import java.sql.*;

public class MySql {
    private static final String URL = "jdbc:mysql://localhost:3306/SqlSpeedTest?allowLoadLocalInfile=true";
    private static final String USER = "root";
    private static final String PASSWORD = "admin";
    private static MySql instance;
    private static Connection conn;

    private MySql() {
            getConnection();
            dropTables();
            Branches();
            Categories_ENG();
            Customers_ENG();
            Orders();
            Order_Details();
    }

    public static Connection getConnection() {
        if (conn == null) {
            try {
                conn = DriverManager.getConnection(URL, USER, PASSWORD);
                System.out.println("Połączono z bazą MySQL!");
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return conn;
    }

    public static MySql getInstance() {
        if (instance == null) {
            instance = new MySql();
            System.out.println("instancja wywołana");
        }
        return instance;
    }

    public static void closeConnection() {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static void Branches() {
        String createTable = """
            CREATE TABLE IF NOT EXISTS Branches (
                BRANCH_ID VARCHAR(100) PRIMARY KEY,
                REGION VARCHAR(100),
                CITY VARCHAR(100),
                TOWN VARCHAR(100),
                BRANCH_TOWN VARCHAR(100),
                LAT BIGINT,
                LON BIGINT
            );
        """;

        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(createTable);
            System.out.println("Tabela Branches utworzona.");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        loadCsv("Branches", "/var/lib/mysql-files/Branches.csv", ",");
    }

    public static void Categories_ENG() {
        String createTable = """
            CREATE TABLE IF NOT EXISTS Categories_ENG (
                ITEMID BIGINT AUTO_INCREMENT PRIMARY KEY,
                CATEGORY1 VARCHAR(100),
                CATEGORY1_ID VARCHAR(50),
                CATEGORY2 VARCHAR(100),
                CATEGORY2_ID VARCHAR(50),
                CATEGORY3 VARCHAR(100),
                CATEGORY3_ID VARCHAR(50),
                CATEGORY4 VARCHAR(100),
                CATEGORY4_ID VARCHAR(50),
                BRAND VARCHAR(100),
                ITEMCODE VARCHAR(100),
                ITEMNAME VARCHAR(500)
            );
        """;

        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(createTable);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        loadCsv("Categories_ENG", "/var/lib/mysql-files/Categories_ENG.csv", ";");
    }

    public static void Customers_ENG() {
        String createTable = """
            CREATE TABLE IF NOT EXISTS Customers_ENG (
                USERID BIGINT AUTO_INCREMENT PRIMARY KEY,
                USERNAME_ VARCHAR(100),
                NAMESURNAME VARCHAR(100),
                STATUS_ BOOLEAN,
                USERGENDER CHAR(1),
                USERBIRTHDATE DATE,
                REGION VARCHAR(50),
                CITY VARCHAR(50),
                TOWN VARCHAR(50),
                DISTRICT VARCHAR(50),
                ADDRESSTEXT TEXT
            );
        """;

        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(createTable);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        loadCsv("Customers_ENG", "/var/lib/mysql-files/Customers_ENG.csv", ";");
    }

    public static void Orders() {
        String createTable = """
            CREATE TABLE IF NOT EXISTS Orders (
                ORDERID BIGINT PRIMARY KEY,
                BRANCH_ID VARCHAR(20),
                DATE_ DATETIME,
                USERID BIGINT,
                NAMESURNAME VARCHAR(100),
                TOTALBASKET VARCHAR(100),
                FOREIGN KEY (BRANCH_ID) REFERENCES Branches(BRANCH_ID),
                FOREIGN KEY (USERID) REFERENCES Customers_ENG(USERID)
            );
        """;

        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(createTable);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        loadCsv("Orders", "/var/lib/mysql-files/Orders_final.csv", ",");
    }

    public static void Order_Details() {
        String createTable = """
            CREATE TABLE IF NOT EXISTS Order_Details (
                ORDERID BIGINT,
                ORDERDETAILID BIGINT PRIMARY KEY,
                AMOUNT INT,
                UNITPRICE VARCHAR(50),
                TOTALPRICE VARCHAR(50),
                ITEMID BIGINT,
                ITEMCODE BIGINT,
                FOREIGN KEY (ORDERID) REFERENCES Orders(ORDERID),
                FOREIGN KEY (ITEMID) REFERENCES Categories_ENG(ITEMID)
            );
        """;

        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(createTable);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        loadCsv("Order_Details", "/var/lib/mysql-files/Order_Details_final.csv", ",");
    }

    private static void loadCsv(String tableName, String filePath, String delimiter) {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("SET autocommit=0");
            stmt.execute("SET unique_checks=0");
            stmt.execute("SET foreign_key_checks=0");

            String sql = "LOAD DATA INFILE '" + filePath.replace("\\", "\\\\") + "' " +
                    "INTO TABLE " + tableName + " " +
                    "FIELDS TERMINATED BY '" + delimiter + "' " +
                    "LINES TERMINATED BY '\\n' " +
                    "IGNORE 1 LINES";
            stmt.execute(sql);


            stmt.execute("SET unique_checks=1");
            stmt.execute("SET foreign_key_checks=1");
            stmt.execute("COMMIT");

            System.out.println("CSV został wczytany do tabeli " + tableName);
        } catch (SQLException e) {
            System.out.println("Błąd podczas wczytywania CSV do tabeli " + tableName + ": " + e.getMessage());
        }
    }


    public static void dropTables() {
        System.out.println("test");
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("SET FOREIGN_KEY_CHECKS = 0");
            System.out.println("Tabele usunięte.");

            stmt.executeUpdate("DROP TABLE IF EXISTS Order_Details");
            System.out.println("Tabele usunięte.");

            stmt.executeUpdate("DROP TABLE IF EXISTS Categories_ENG");
            System.out.println("Tabele usunięte.");

            stmt.executeUpdate("DROP TABLE IF EXISTS Customers_ENG");
            System.out.println("Tabele usunięte.");

            stmt.executeUpdate("DROP TABLE IF EXISTS Orders");
            System.out.println("Tabele usunięte.");

            stmt.executeUpdate("DROP TABLE IF EXISTS Branches");
            System.out.println("Tabele usunięte.");

            stmt.executeUpdate("SET FOREIGN_KEY_CHECKS = 1");
            System.out.println("Tabele usunięte.");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
