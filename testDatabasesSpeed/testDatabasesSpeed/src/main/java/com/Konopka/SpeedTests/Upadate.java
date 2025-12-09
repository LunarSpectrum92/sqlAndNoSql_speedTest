package com.Konopka.SpeedTests;


import com.datastax.oss.driver.api.core.session.Session;

import java.sql.Connection;

public class Upadate {

    private static Connection connMySql;
    private static Connection connPostgres;
    private static Session sessionCassOld;
    private static Session sessionCassNew;

    public Upadate(com.datastax.oss.driver.api.core.session.Session sessionCassOld, com.datastax.oss.driver.api.core.session.Session sessionCassNew, Connection connPostgres, Connection connMySql){
        Upadate.connMySql =  connMySql;
        Upadate.connPostgres =  connPostgres;
        Upadate.sessionCassOld =  sessionCassOld;
        Upadate.sessionCassNew =  sessionCassNew;
    }









}
