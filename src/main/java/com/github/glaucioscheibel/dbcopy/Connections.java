package com.github.glaucioscheibel.dbcopy;

import java.sql.Connection;
import java.sql.DriverManager;

public final class Connections {

    private Connections() {}

    public static Connection getSource() throws Exception {
        DBProp dbp = new DBProp();
        Connection con1 = DriverManager.getConnection(dbp.getProperty("db1.url"), dbp.getProperty("db1.user"),
                dbp.getProperty("db1.pass"));
        return con1;
    }

    public static Connection getTarget() throws Exception {
        DBProp dbp = new DBProp();
        Connection con2 = DriverManager.getConnection(dbp.getProperty("db2.url"), dbp.getProperty("db2.user"),
                dbp.getProperty("db2.pass"));
        return con2;
    }
}
