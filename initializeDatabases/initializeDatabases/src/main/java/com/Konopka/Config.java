package com.Konopka;

import java.io.InputStream;
import java.util.Properties;

public class Config {
    private static final Properties props = new Properties();

    static {
        try (InputStream in = Config.class.getClassLoader().getResourceAsStream("config.properties")) {
            if (in == null) {
                throw new RuntimeException("Nie znaleziono pliku config.properties!");
            }
            props.load(in);
        } catch (Exception e) {
            throw new RuntimeException("Błąd ładowania config.properties: " + e.getMessage(), e);
        }
    }

    public static String get(String key) {
        return props.getProperty(key);
    }
}
