package com.github.glaucioscheibel.dbcopy;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class GeraRelacaoTabelas {

    private GeraRelacaoTabelas() {}

    public static void main(String[] args) throws Exception {
        DBProp dbp = new DBProp();
        List<String> tabelas = new ArrayList<>();
        Connection con1 = Connections.getSource();
        DatabaseMetaData dmd = con1.getMetaData();
        ResultSet rs1 = dmd.getTables(null, dbp.getProperty("db1.db"), null, null);
        while (rs1.next()) {
            String table = rs1.getString(3).toUpperCase();
            if (!table.startsWith("_") && !table.startsWith("QRTZ") && !table.startsWith("WFG_")
                    && !table.startsWith("DASH_") && !table.startsWith("SYSTEM_") && !table.startsWith("XML_")
                    && !table.startsWith("TRACE_") && !table.startsWith("SQL_") && !table.startsWith("SERVER_")
                    && !table.startsWith("DM_")) {
                tabelas.add(table);
            }
        }
        rs1.close();
        con1.close();
        Collections.sort(tabelas);
        PrintWriter pr = new PrintWriter("tabelas.txt");
        tabelas.stream().forEach((tabela) -> {
            pr.println(tabela);
        });
        pr.close();
    }
}
