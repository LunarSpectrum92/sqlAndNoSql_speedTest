package com.Konopka.SpeedTests;


import com.datastax.oss.driver.api.core.session.Session;

import java.sql.Connection;

public class Read {

    private static Connection connMySql;
    private static Connection connPostgres;
    private static Session sessionCassOld;
    private static Session sessionCassNew;

    public Read(com.datastax.oss.driver.api.core.session.Session sessionCassOld, com.datastax.oss.driver.api.core.session.Session sessionCassNew, Connection connPostgres, Connection connMySql){
        Read.connMySql =  connMySql;
        Read.connPostgres =  connPostgres;
        Read.sessionCassOld =  sessionCassOld;
        Read.sessionCassNew =  sessionCassNew;
    }

    //-select orders
    //-select orders_details
    //-select customers
    //-imie user id oraz order id oraz totalbasket
    //-wszystko razem

    public static void SelectOrders(){

    }





}
