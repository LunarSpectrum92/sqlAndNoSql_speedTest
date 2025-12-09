package com.Konopka.Connectors;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class MySqlConnection {

    private static final String URL = "jdbc:mysql://localhost:3306/SqlSpeedTest?allowLoadLocalInfile=true";
    private static final String USER = "root";
    private static final String PASSWORD = "admin";
    private static Connection conn;

    public static Connection MySqlConnect() {
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

    public MySqlConnection(){
        MySqlConnect();
    }
}
