package com.Konopka.SpeedTests;


import com.datastax.oss.driver.api.core.session.Session;

import java.sql.Connection;

public class Delete {

    private static Connection connMySql;
    private static Connection connPostgres;
    private static Session sessionCassOld;
    private static Session sessionCassNew;

    public Delete(com.datastax.oss.driver.api.core.session.Session sessionCassOld, com.datastax.oss.driver.api.core.session.Session sessionCassNew, Connection connPostgres, Connection connMySql){
        Delete.connMySql =  connMySql;
        Delete.connPostgres =  connPostgres;
        Delete.sessionCassOld =  sessionCassOld;
        Delete.sessionCassNew =  sessionCassNew;
    }







}
