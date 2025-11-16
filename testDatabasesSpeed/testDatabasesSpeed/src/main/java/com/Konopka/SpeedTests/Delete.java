package com.Konopka.SpeedTests;

import com.datastax.driver.core.Session;

import java.sql.Connection;

public class Delete {

    private static Connection conn;
    private static Session session;

    public Delete(Session session, Connection conn){
        Delete.session = session;
        Delete.conn = conn;
    }







}
