package com.Konopka.SpeedTests;

import com.Konopka.Connectors.CassandraConnection;
import com.Konopka.Connectors.PostgreConnection;
import com.datastax.driver.core.Session;

import java.sql.Connection;

public class Create {

    private static Connection conn;
    private static Session session;

public Create(Session session, Connection conn){
    Create.session = session;
    Create.conn = conn;
}














}
