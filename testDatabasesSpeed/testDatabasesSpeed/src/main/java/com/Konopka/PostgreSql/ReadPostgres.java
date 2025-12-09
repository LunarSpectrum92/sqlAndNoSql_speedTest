package com.Konopka.PostgreSql;

import com.Konopka.Interfaces.ReadInterface;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

public class ReadPostgres implements ReadInterface {
    public static Connection connection;

    public ReadPostgres(Connection connection) {
        ReadPostgres.connection = connection;
    }

    @Override
    public Long SelectOrders() {
        try (Statement statement = connection.createStatement()) { // Używamy Statement
            System.out.println("start select orders_by_branch");
            Long startTime = System.nanoTime();
            ResultSet resultSet = statement.executeQuery("""
                SELECT *
                FROM orders;
                """);
            Long endTime = System.nanoTime();
            System.out.println("koniec select orders_by_branch czas dzialanie programu wyniosl: " + (endTime - startTime));
            return endTime - startTime;
        } catch (Exception e) {
            System.err.println("nie udalo sie pobrac tabeli orders_by_branch");
            e.printStackTrace();
        }
        return null;
    }


    @Override
    public Long SelectOrderDetails() {
        try (Statement statement = connection.createStatement()) {
            System.out.println("start select order_details_by_order");
            Long startTime = System.nanoTime();
            ResultSet resultSet = statement.executeQuery("""
                 SELECT *
                 FROM order_details;
                """);
            Long endTime = System.nanoTime();
            System.out.println("koniec select order_details_by_order czas dzialanie programu wyniosl: " + (endTime - startTime));
            return endTime - startTime;
        } catch (Exception e) {
            System.err.println("nie udalo sie pobrac tabeli order_details_by_order");
            e.printStackTrace();
        }
        return null;
    }


    @Override
    public Long SelectCustomers() {
        try (Statement statement = connection.createStatement()) {
            System.out.println("start select customers");
            Long startTime = System.nanoTime();

            ResultSet resultSet = statement.executeQuery("""
                 SELECT *
                 FROM customers_eng;
                """);

            Long endTime = System.nanoTime();
            System.out.println("koniec select customers czas dzialanie programu wyniosl: " + (endTime - startTime));
            return endTime - startTime;
        } catch (Exception e) {
            System.err.println("nie udalo sie pobrac tabeli customers");
            e.printStackTrace();
        }
        return null;
    }


    @Override
    public Long SelectCustomerOrderDetails() {
        try (Statement statement = connection.createStatement()) {
            System.out.println("start select orders_by_user");
            Long startTime = System.nanoTime();
            ResultSet resultSet = statement.executeQuery("""
SELECT
    c.USERID,
    o.DATE_,
    o.ORDERID,
    b.BRANCH_ID,
    o.TOTALBASKET,
    c.NAMESURNAME
FROM orders o
         JOIN customers_eng c ON o.userid = c.userid
         JOIN branches b ON o.branch_id = b.branch_id;
                """);
            Long endTime = System.nanoTime();
            System.out.println("koniec select orders_by_user czas dzialanie programu wyniosl: " + (endTime - startTime));
            return endTime - startTime;
        } catch (Exception e) {
            System.err.println("nie udalo sie pobrac tabeli orders_by_user");
            e.printStackTrace();
        }
        return null;
    }


    @Override
    public Long CountOrdersForCustomer() {
        try (Statement statement = connection.createStatement()) {
            System.out.println("start count orders_by_user");
            Long startTime = System.nanoTime();
            ResultSet resultSet = statement.executeQuery("""
SELECT c.USERID, count(o.ORDERID)
FROM orders o
         JOIN customers_eng c ON o.userid = c.userid
         JOIN branches b ON o.branch_id = b.branch_id
group by c.userid;  
                """);
            Long endTime = System.nanoTime();
            System.out.println("koniec count orders_by_user czas dzialanie programu wyniosl: " + (endTime - startTime));
        return endTime - startTime;
        } catch (Exception e) {
            System.err.println("nie udalo sie pobrac tabeli orders_by_user");
            e.printStackTrace();
        }
        return null;
    }


    @Override
    public Long SelectAll() {
        try (Statement statement = connection.createStatement()) {
            System.out.println("start select all");
            Long startTime = System.nanoTime();
            ResultSet resultSet = statement.executeQuery("""
SELECT
    c.USERID,
    o.DATE_,
    o.ORDERID,
    b.BRANCH_ID,
    o.TOTALBASKET,
    c.NAMESURNAME,
    '[' || string_agg(
            '{orderdetailid:''' || d.orderdetailid || ''', ' ||
            'itemid:' || d.itemid || ', ' ||
            'itemcode:''' || c_e.itemcode || ''', ' ||
            'itemname:''' || c_e.itemname || ''', ' ||
            'amount:' || d.amount || ', ' ||
            'unitprice:''' || d.unitprice || ''', ' ||
            'totalprice:''' || d.totalprice || '''}', ', '
           ) || ']' AS ORDER_ITEMS
FROM orders o
         JOIN customers_eng c ON o.userid = c.userid
         JOIN branches b ON o.branch_id = b.branch_id
         JOIN order_details d ON o.orderid = d.orderid
         JOIN categories_eng c_e ON d.itemid = c_e.itemid
GROUP BY c.USERID, o.DATE_, o.ORDERID, b.BRANCH_ID, o.TOTALBASKET, c.NAMESURNAME
ORDER BY c.USERID, o.DATE_ DESC;
                """);
            Long endTime = System.nanoTime();
            System.out.println("koniec select all czas dzialanie programu wyniosl: " + (endTime - startTime));
         return endTime - startTime;
        } catch (Exception e) {
            System.err.println("nie udalo sie pobrac tabeli all");
            e.printStackTrace();
        }
        return null;
    }
}
