package com.Konopka;

import com.Konopka.Connectors.CassandraConnection;
import com.Konopka.Connectors.PostgreConnection;
import com.datastax.driver.core.Session;

import java.sql.Connection;


public class Main {
    public static void main(String[] args) {


        Session session = CassandraConnection.getSession();
        Connection conn = PostgreConnection.getConn();




        }
    }
