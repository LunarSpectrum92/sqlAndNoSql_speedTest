package com.Konopka.Connectors;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

import java.sql.Connection;

public class CassandraConnection {


    private static CassandraConnection instance;
    private static Cluster cluster;
    private static Session session;

    public static Session getSession() {
        return session;
    }




    private CassandraConnection(){
        connectCassandra();
    }

    private static void connectCassandra(){
        cluster = Cluster.builder()
                .addContactPoint("localhost")
                .withPort(9042)
                .build();
        session = cluster.connect("cassandraSpeedTestDb");
        System.out.println("Połączono z Cassandra!");
    }


    public static CassandraConnection getInstance(){
        if(instance == null){
            instance = new CassandraConnection();
        }
        return instance;
    }



}
