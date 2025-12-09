package com.Konopka.Connectors;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.session.Session;

import java.net.InetSocketAddress;

public class CassandraNewConnection {

    private static Session session;

    public static Session getSession() {
        return session;
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


    public CassandraNewConnection(){
        ConnectCassandra();
    }




}
