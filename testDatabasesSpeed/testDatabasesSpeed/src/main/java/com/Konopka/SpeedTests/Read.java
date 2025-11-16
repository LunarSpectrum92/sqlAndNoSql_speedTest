package com.Konopka.SpeedTests;

import com.datastax.driver.core.Session;

import java.sql.Connection;

public class Read {

    private static Connection conn;
    private static Session session;

    public Read(Session session, Connection conn){
        Read.session = session;
        Read.conn = conn;
    }








}
