package com.github.glaucioscheibel.dbcopy;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public final class CriaFKs {

    private CriaFKs() {
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

        LinkedList<String> alterList = new LinkedList<>();
        for (String tabela : tabelas) {
            ResultSet rs = dbmd1.getImportedKeys(null, null, tabela);
            int i = 1;
            List<String> fcols = new ArrayList<>();
            List<String> cols = new ArrayList<>();
            while (rs.next()) {
                String ftable = rs.getString(3).toUpperCase();
                String fcol = rs.getString(4).toUpperCase();
                String col = rs.getString(8).toUpperCase();
                short seq = rs.getShort(9);
                if (seq == 1) {
                    fcols.clear();
                    cols.clear();
                } else {
                    alterList.removeLast();
                    i--;
                }
                fcols.add(fcol);
                cols.add(col);
                StringBuilder sb = new StringBuilder("ALTER TABLE ");
                sb.append(tabela);
                sb.append(" ADD CONSTRAINT ");
                sb.append(tabela);
                sb.append("_FK_");
                sb.append(i++);
                sb.append(" FOREIGN KEY (");
                sb.append(toList(cols));
                sb.append(") REFERENCES ");
                sb.append(ftable);
                sb.append('(');
                sb.append(toList(fcols));
                sb.append(')');
                alterList.add(sb.toString());
            }
        }
        for (String alter : alterList) {
            System.out.println(alter);
            try {
                st2.executeUpdate(alter);
            } catch (SQLException se) {
                System.err.println(se.getMessage());
            }
        }
    }

    private static String toList(List<String> ls) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < ls.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(ls.get(i));
        }
        return sb.toString();
    }
}
