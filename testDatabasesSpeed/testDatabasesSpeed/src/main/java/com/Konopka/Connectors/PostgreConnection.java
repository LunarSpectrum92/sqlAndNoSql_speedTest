package com.Konopka.Connectors;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class PostgreConnection {

    private static final String URL = "jdbc:postgresql://localhost:5432/SqlSpeedTest";
    private static final String USER = "admin";
    private static final String PASSWORD = "admin123";
    private static PostgreConnection instance;
    private static Connection conn;

    public static Connection getConnection() {
        return conn;
    }


    private PostgreConnection() {
        PostgreConnection();
    }

    private static void PostgreConnection() {
        if (conn == null) {
            try {
                conn = DriverManager.getConnection(URL, USER, PASSWORD);
                System.out.println("Połączono z bazą!");
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }






}
