# Benchmark CRUD – Java + Cassandra / PostgreSQL / MySQL

Projekt ma na celu porównanie wydajności operacji CRUD wykonywanych na trzech różnych bazach danych:

- **Apache Cassandra**
- **PostgreSQL**
- **MySQL**

Aplikacja wgrywa zestaw testowych danych oraz mierzy czas wykonywania operacji:
- Create (INSERT)
- Read (SELECT)
- Update (UPDATE)
- Delete (DELETE)

Dzięki temu możliwe jest zestawienie szybkości poszczególnych operacji w różnych systemach bazodanowych.

---

## 1. Wymagania

### Oprogramowanie
- Java 25
- Maven
- Docker 

### Bazy danych
Aplikacja zakłada istnienie uruchomionych instancji:

- Apache Cassandra
- Apache Cassandra old
- PostgreSQL
- MySQL

---

## 2. Konfiguracja

Parametry połączenia znajdują się w pliku:



## odpalenie

Polecenie służące do stowrzenia plików csv które kopiowane będą do cassandra

COPY (SELECT
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
ORDER BY c.USERID, o.DATE_ DESC)
TO '/tmp/import/orders_by_user.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ';');




ALTER TABLE Categories_ENG
ADD INDEX idx_categories_item (itemid);
ALTER TABLE Order_Details
ADD INDEX idx_orderdetails_order (orderid),
ADD INDEX idx_orderdetails_item (itemid);
ALTER TABLE Branches
ADD INDEX idx_branches_branchid (branch_id);
ALTER TABLE Customers_ENG
ADD INDEX idx_customers_userid (userid);
ALTER TABLE Orders
ADD INDEX idx_orders_userid (userid),
ADD INDEX idx_orders_branch (branch_id),
ADD INDEX idx_orders_date (date_),
ADD INDEX idx_orders_orderid (orderid);