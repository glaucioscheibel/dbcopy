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

public final class CriaPKs {

    private CriaPKs() {
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
            StringBuilder sbpk = new StringBuilder("ALTER TABLE ");
            sbpk.append(tabela);
            sbpk.append(" ADD CONSTRAINT ");
            sbpk.append(tabela);
            sbpk.append("_PK PRIMARY KEY (");
            ResultSet rs = dbmd1.getPrimaryKeys(null, null, tabela);
            boolean ini = true;
            while (rs.next()) {
                StringBuilder sbnn = new StringBuilder("ALTER TABLE ");
                String col = rs.getString(4).toUpperCase();
                if (!ini) {
                    sbpk.append(", ");
                } else {
                    ini = false;
                }
                sbpk.append(col);
                sbnn.append(tabela);
                sbnn.append(" MODIFY ");
                sbnn.append(col);
                sbnn.append(" NOT NULL");
                System.out.println(sbnn);
                try {
                    st2.executeUpdate(sbnn.toString());
                } catch (SQLException se) {
                    System.err.println(se.getMessage());
                }
            }
            sbpk.append(')');
            System.out.println(sbpk);
            try {
                st2.executeUpdate(sbpk.toString());
            } catch (SQLException se) {
                System.err.println(se.getMessage());
            }
        }
    }
}
