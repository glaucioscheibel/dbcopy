package com.github.glaucioscheibel.dbcopy;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.FileReader;
import java.io.InputStream;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.commons.codec.binary.Base64;
import org.bson.Document;

public final class CriaMongoDB {

    private CriaMongoDB() {
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public static void main(String[] args) throws Exception {
        DBProp dbp = new DBProp();
        //Origem
        Connection con1 = Connections.getSource();
        String db1 = con1.getMetaData().getDatabaseProductName();
        Statement st1 = con1.createStatement();
        System.out.println(db1);

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

        MongoClient m = new MongoClient();
        MongoDatabase db = m.getDatabase(dbp.getProperty("db2.db"));

        for (String tabela : tabelas) {
            int cont = 0;
            System.out.println(tabela);
            MongoCollection coll = db.getCollection(tabela);
            ResultSet rs1 = st1.executeQuery("select * from " + tabela); // origem
            ResultSetMetaData rsmd1 = rs1.getMetaData();
            while (rs1.next()) {
                int cols = rsmd1.getColumnCount();
                Document doc = new Document();
                for (int i = 1; i <= cols; i++) {
                    switch (rsmd1.getColumnType(i)) {
                        case Types.VARCHAR:
                        case Types.CHAR:
                            String vc = rs1.getString(i);
                            if (!rs1.wasNull()) {
                                doc.put(rsmd1.getColumnName(i), vc);
                            }
                            break;
                        case Types.BIGINT:
                            long l = rs1.getLong(i);
                            if (!rs1.wasNull()) {
                                doc.put(rsmd1.getColumnName(i), l);
                            }
                            break;
                        case Types.INTEGER:
                        case Types.TINYINT:
                            int n = rs1.getInt(i);
                            if (!rs1.wasNull()) {
                                doc.put(rsmd1.getColumnName(i), n);
                            }
                            break;
                        case Types.BOOLEAN:
                            boolean b = rs1.getBoolean(i);
                            if (!rs1.wasNull()) {
                                doc.put(rsmd1.getColumnName(i), b);
                            }
                            break;
                        case Types.REAL:
                        case Types.NUMERIC:
                        case Types.FLOAT:
                        case Types.DOUBLE:
                        case Types.DECIMAL:
                            double d = rs1.getDouble(i);
                            if (!rs1.wasNull()) {
                                doc.put(rsmd1.getColumnName(i), d);
                            }
                            break;
                        case Types.CLOB:
                            Clob c = rs1.getClob(i);
                            if (!rs1.wasNull()) {
                                boolean lin = false;
                                StringBuilder sb = new StringBuilder();
                                br = new BufferedReader(c.getCharacterStream());
                                while ((linha = br.readLine()) != null) {
                                    if (lin) {
                                        sb.append('\n');
                                    } else {
                                        lin = true;
                                    }
                                    sb.append(linha);
                                }
                                br.close();
                                doc.put(rsmd1.getColumnName(i), sb.toString());
                            }
                            break;
                        case Types.BLOB:
                            Blob bl = rs1.getBlob(i);
                            if (!rs1.wasNull()) {
                                InputStream is = bl.getBinaryStream();
                                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                                int tam;
                                byte[] buf = new byte[8192];
                                while ((tam = is.read(buf)) != -1) {
                                    baos.write(buf, 0, tam);
                                }
                                is.close();
                                doc.put(rsmd1.getColumnName(i), Base64.encodeBase64String(baos.toByteArray()));
                            }
                            break;
                        case Types.TIMESTAMP:
                            Timestamp its = rs1.getTimestamp(i);
                            if (!rs1.wasNull()) {
                                doc.put(rsmd1.getColumnName(i), new Date(its.getTime()));
                            }
                            break;
                        default:
                            throw new Exception(rsmd1.getColumnTypeName(i));
                    }
                }
                coll.insertOne(doc);
                cont++;
                if (cont % 10000 == 0) {
                    System.out.println(cont + " registros copiados");
                }
            }
        }
        m.close();
    }
}
