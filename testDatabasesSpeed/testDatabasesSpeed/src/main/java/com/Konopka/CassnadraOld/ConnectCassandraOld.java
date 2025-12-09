package com.Konopka.CassnadraOld;

import com.Konopka.Interfaces.ConnectInterface;
import com.datastax.oss.driver.api.core.CqlSession;

import java.net.InetSocketAddress;

public class ConnectCassandraOld implements ConnectInterface {

    private CqlSession session;

    public ConnectCassandraOld(){
        ConnectToDatabase();
    }

    public CqlSession getSession() {
        return session;
    }

    public void ConnectToDatabase() {
        try {
            session = CqlSession.builder()
                    .addContactPoint(new InetSocketAddress("localhost", 9043))
                    .withLocalDatacenter("datacenter1")
                    .build();
            System.out.println("Połączono z Cassandra!");
        } catch (Exception e) {
            System.err.println("Nie udało się połączyć!");
            e.printStackTrace();
        }
    }

    public void CloseConnection(){
        if(session != null) {
            session.close();
            System.out.println("Połączenie do Cassandra zamknięte");
        }
    }





}
