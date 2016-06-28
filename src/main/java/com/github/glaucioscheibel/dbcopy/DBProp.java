package com.github.glaucioscheibel.dbcopy;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class DBProp extends Properties {

    private static final long serialVersionUID = 1L;

    public DBProp() {
        InputStream is = DBProp.class.getClassLoader().getResourceAsStream("db.properties");
        try {
            this.load(is);
            is.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
