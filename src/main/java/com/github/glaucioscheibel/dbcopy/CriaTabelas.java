package com.github.glaucioscheibel.dbcopy;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class CriaTabelas {

    private CriaTabelas() {
    }

    public static void main(String[] args) throws Exception {
        //Origem
        Connection con1 = Connections.getSource();
        DatabaseMetaData dbmd1 = con1.getMetaData();
        //Destino
        Connection con2 = Connections.getTarget();
        Statement st2 = con2.createStatement();
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

        for (String tabela : tabelas) {
            boolean ini = true;
            StringBuilder sb = new StringBuilder("CREATE TABLE ");
            sb.append(tabela);
            sb.append(" (");
            ResultSet rs = dbmd1.getColumns(null, null, tabela, null);
            while (rs.next()) {
                if (!ini) {
                    sb.append(", ");
                } else {
                    ini = false;
                }
                sb.append(rs.getString(4).toUpperCase());
                sb.append(' ');
                int type = rs.getInt(5);
                sb.append(toType(type));
                if (hasSize(type)) {
                    int size = rs.getInt(7);
                    if (!rs.wasNull()) {
                        sb.append('(');
                        sb.append(size);
                        int dig = rs.getInt(9);
                        if (!rs.wasNull()) {
                            sb.append(',');
                            sb.append(dig);
                        }
                        sb.append(')');
                    }
                }
            }
            sb.append(")");
            System.out.println(sb);
            try {
                st2.execute("drop table " + tabela);
            } catch (SQLException se) {}
            try {
                st2.execute(sb.toString());
            } catch (SQLException se) {
                se.printStackTrace();
            }
        }
    }

    private static boolean hasSize(int jdbctype) {
        switch (jdbctype) {
            case -7:
            case -6:
            case -5:
            case 4:
            case 5:
            case 1:
            case 8:
            case 7:
            case 12:
                return true;
        }
        return false;
    }

    private static String toType(int jdbctype) {
        switch (jdbctype) {
            case -7:
            case -6:
            case -5:
            case 4:
            case 5:
            case 8:
                return "NUMBER";
            case -1:
                return "CLOB";
            case 12:
                return "VARCHAR";
            case 91:
                return "DATE";
            case 93:
                return "TIMESTAMP";
            case -4:
                return "BLOB";
            case 7:
                return "FLOAT";
            case 1:
                return "CHAR";
            default:
                System.out.println("####################################################");
                System.out.println(jdbctype);
                System.exit(-1);
        }
        return null;
    }
}
