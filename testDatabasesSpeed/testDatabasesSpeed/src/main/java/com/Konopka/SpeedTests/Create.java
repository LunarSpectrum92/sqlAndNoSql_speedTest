package com.Konopka.SpeedTests;


import com.datastax.oss.driver.api.core.session.Session;

import java.sql.Connection;

public class Create {

    private static Connection connMySql;
    private static Connection connPostgres;
    private static Session sessionCassOld;
    private static Session sessionCassNew;

    public Create(Session sessionCassOld, Session sessionCassNew, Connection connPostgres, Connection connMySql){
        Create.connMySql =  connMySql;
        Create.connPostgres =  connPostgres;
        Create.sessionCassOld =  sessionCassOld;
        Create.sessionCassNew =  sessionCassNew;
    }













}
