package com.github.glaucioscheibel.dbcopy;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileReader;
import java.io.PrintWriter;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public final class Main {

    private Main() {}

    public static void main(String[] args) throws Exception {
        DecimalFormat df = new DecimalFormat("#,##0");
        PrintWriter log = new PrintWriter("dbcopy.log");

        //Origem
        Connection con1 = Connections.getSource();
        String db1 = con1.getMetaData().getDatabaseProductName();
        Statement st1 = con1.createStatement();
        st1.setFetchSize(10000);
        System.out.println(db1);

        //Destino
        Connection con2 = Connections.getTarget();
        String db2 = con2.getMetaData().getDatabaseProductName();
        Statement st2 = con2.createStatement();
        System.out.println(db2);
        boolean mssql = db2.equalsIgnoreCase("Microsoft SQL Server");
        boolean mysql = db2.equalsIgnoreCase("Mysql");
        boolean oracle = db2.equalsIgnoreCase("Oracle");
        List<String> tabelas = new ArrayList<>();
        FileReader fr = new FileReader("tabelas.txt");
        BufferedReader br = new BufferedReader(fr);
        String linha;
        while ((linha = br.readLine()) != null) {
            if (!linha.startsWith("//")) {
                tabelas.add(linha);
            }
        }
        br.close();
        br = null;
        fr.close();
        fr = null;

        if (mssql) {
            try {
                st2.executeUpdate("exec sp_msforeachtable \"ALTER TABLE ? NOCHECK CONSTRAINT ALL\"");
            } catch (SQLException se) {
                se.printStackTrace();
            }
        }
        if (mysql) {
            try {
                st2.executeUpdate("SET foreign_key_checks = 0");
            } catch (SQLException se) {
                se.printStackTrace();
            }
        }
        if (oracle) {
            fr = new FileReader("oracle.sql");
            br = new BufferedReader(fr);
            StringBuilder cmd = new StringBuilder();
            while ((linha = br.readLine()) != null) {
                cmd.append(linha);
            }
            br.close();
            fr.close();
            try {
                st2.executeUpdate(cmd.toString());
            } catch (SQLException se) {
                se.printStackTrace();
            }
            br = null;
            fr = null;
            cmd = null;
        }

        con2.setAutoCommit(false);
        try {
            Set<String> columns = new HashSet<>();
            boolean msid = false;
            for (String tabela : tabelas) {
                System.out.println("================= " + tabela + " =================");

                int cont = 0;
                try {
                    columns.clear();

                    try {
                        st2.executeUpdate("truncate table " + tabela);
                    } catch (SQLException e) {
                        try {
                            st2.executeUpdate("delete from " + tabela);
                        } catch (SQLException ee) {
                            System.err.println(ee.getMessage());
                        }
                    }
                    con2.commit();
                    if (mssql) {
                        try {
                            st2.executeUpdate("set identity_insert " + tabela + " on");
                            msid = true;
                        } catch (SQLException se) {}
                    }
                    ResultSet rs1 = st1.executeQuery("select * from " + tabela); // origem
                    ResultSetMetaData rsmd1 = rs1.getMetaData();
                    ResultSet rs2 = st2.executeQuery("select * from " + tabela); // destino
                    ResultSetMetaData rsmd2 = rs2.getMetaData();
                    int cols = rsmd2.getColumnCount();
                    for (int i = 1; i <= cols; i++) {
                        columns.add(rsmd2.getColumnName(i).toLowerCase());
                    }
                    log.println(tabela + " " + columns);
                    log.flush();
                    rsmd2 = null;
                    rs2.close();
                    rs2 = null;
                    cols = rsmd1.getColumnCount();
                    StringBuilder sb = new StringBuilder("insert into " + tabela + "(");
                    int colreal = 0;
                    for (int i = 1; i <= cols; i++) {
                        String colu = rsmd1.getColumnName(i);
                        if (columns.contains(colu.toLowerCase())) {
                            if (colreal > 0) {
                                sb.append(',');
                            }
                            sb.append(colu);
                            colreal++;
                        }
                    }
                    sb.append(") values (");
                    for (int i = 1; i <= colreal; i++) {
                        if (i > 1) {
                            sb.append(',');
                        }
                        sb.append('?');
                    }
                    sb.append(')');
                    System.out.println(sb);
                    log.println(sb);
                    log.flush();
                    PreparedStatement ps2 = con2.prepareStatement(sb.toString());
                    while (rs1.next()) {
                        ps2.clearParameters();
                        colreal = 0;
                        for (int i = 1; i <= cols; i++) {
                            if (columns.contains(rsmd1.getColumnName(i).toLowerCase())) {
                                colreal++;
                                switch (rsmd1.getColumnType(i)) {
                                    case Types.BIGINT:
                                        long laux = rs1.getLong(i);
                                        if (rs1.wasNull()) {
                                            ps2.setNull(colreal, Types.BIGINT);
                                        } else {
                                            ps2.setLong(colreal, laux);
                                        }
                                        break;
                                    case Types.INTEGER:
                                        int iaux = rs1.getInt(i);
                                        if (rs1.wasNull()) {
                                            ps2.setNull(colreal, Types.INTEGER);
                                        } else {
                                            ps2.setInt(colreal, iaux);
                                        }
                                        break;
                                    case Types.TIMESTAMP:
                                        Timestamp its = rs1.getTimestamp(i);
                                        if (rs1.wasNull()) {
                                            ps2.setNull(colreal, Types.TIMESTAMP);
                                        } else {
                                            ps2.setTimestamp(colreal, its);
                                        }
                                        break;
                                    case Types.DATE:
                                        Date dtaux = rs1.getDate(i);
                                        if (rs1.wasNull()) {
                                            ps2.setNull(colreal, Types.DATE);
                                        } else {
                                            ps2.setDate(colreal, dtaux);
                                        }
                                        break;
                                    case Types.VARCHAR:
                                    case Types.LONGVARCHAR:
                                    case Types.CHAR:
                                        String saux = rs1.getString(i);
                                        if (rs1.wasNull()) {
                                            ps2.setNull(colreal, Types.VARCHAR);
                                        } else {
                                            ps2.setString(colreal, StringUtil.getCleanString(saux));
                                        }
                                        break;
                                    case Types.TINYINT:
                                        byte b = rs1.getByte(i);
                                        if (rs1.wasNull()) {
                                            ps2.setNull(colreal, Types.TINYINT);
                                        } else {
                                            ps2.setBoolean(colreal, b != 0);
                                        }
                                        break;
                                    case Types.BIT:
                                    case Types.BOOLEAN:
                                        boolean baux = rs1.getBoolean(i);
                                        if (rs1.wasNull()) {
                                            if (oracle) {
                                                ps2.setNull(colreal, Types.BIT);
                                            } else {
                                                ps2.setNull(colreal, Types.BOOLEAN);
                                            }
                                        } else {
                                            ps2.setBoolean(colreal, baux);
                                        }
                                        break;
                                    case Types.REAL:
                                    case Types.NUMERIC:
                                    case Types.FLOAT:
                                        float faux = rs1.getFloat(i);
                                        if (rs1.wasNull()) {
                                            ps2.setNull(colreal, Types.NUMERIC);
                                        } else {
                                            ps2.setFloat(colreal, faux);
                                        }
                                        break;
                                    case Types.DOUBLE:
                                    case Types.DECIMAL:
                                        double daux = rs1.getDouble(i);
                                        if (rs1.wasNull()) {
                                            ps2.setNull(colreal, Types.DOUBLE);
                                        } else {
                                            ps2.setDouble(colreal, daux);
                                        }
                                        break;
                                    case Types.BLOB:
                                    case Types.BINARY:
                                    case Types.VARBINARY:
                                    case Types.LONGVARBINARY:
                                        Blob blaux = rs1.getBlob(i);
                                        if (rs1.wasNull()) {
                                            ps2.setNull(colreal, Types.BLOB);
                                        } else {
                                            long length = blaux.length();
                                            byte[] bytes = blaux.getBytes(1, (int) length);
                                            ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                                            ps2.setBinaryStream(colreal, bis, length);
                                        }
                                        break;
                                    default:
                                        System.out.print(rsmd1.getColumnType(i));
                                        System.out.print(" - ");
                                        System.out.println(rsmd1.getColumnTypeName(i));
                                        log.println(rsmd1.getColumnTypeName(i));
                                        log.flush();
                                        System.exit(0);
                                }
                            }
                        }
                        try {
                            ps2.executeUpdate();
                        } catch (SQLException se) {
                            System.err.println(se.getMessage() + " Tabela: " + tabela);
                            log.println(se.getMessage() + " Tabela: " + tabela);
                            log.flush();
                        }
                        cont++;
                        if (cont % 10000 == 0) {
                            con2.commit();
                            System.out.println(df.format(cont) + " registros copiados");
                            log.println(df.format(cont) + " registros copiados");
                            log.flush();
                        }
                    }
                    ps2.close();
                    if (msid) {
                        try {
                            st2.executeUpdate("set identity_insert " + tabela + " off");
                            msid = false;
                        } catch (SQLException se) {}
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                    System.err.println(e.getMessage() + " Tabela: " + tabela);
                    log.println(e.getMessage() + " Tabela: " + tabela);
                    log.flush();
                }
                con2.commit();
                System.out.println(df.format(cont) + " registros copiados");
                log.println(df.format(cont) + " registros copiados");
                log.flush();
            }
            if (db2.equalsIgnoreCase("Microsoft SQL Server")) {
                try {
                    st2.executeUpdate("exec sp_msforeachtable \"ALTER TABLE ? WITH CHECK CHECK CONSTRAINT ALL\"");
                } catch (SQLException se) {}
            }
            if (db2.equalsIgnoreCase("Mysql")) {
                try {
                    st2.executeUpdate("SET foreign_key_checks = 1");
                } catch (SQLException se) {
                    se.printStackTrace();
                }
            }
            if (db2.equalsIgnoreCase("Oracle")) {
                /*
                String cmd
                        = "begin\n"
                        + "for i in (select constraint_name, table_name from user_constraints) LOOP\n"
                        + "execute immediate 'alter table '||i.table_name||' enable constraint '||i.constraint_name||'';\n"
                        + "end loop;\n"
                        + "end;\n";
                try {
                    st2.executeUpdate(cmd);
                } catch (SQLException se) {
                    se.printStackTrace();
                }
                */
            }
            st1.close();
            st2.close();
        } finally {
            con2.close();
            con1.close();
            log.close();
        }
    }
}
