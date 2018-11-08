package com.dunkrik.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ApplicationConfig {

    private Properties properties;

    private static ApplicationConfig config;

    private ApplicationConfig() throws IOException {
        properties = new Properties();
        InputStream in = ApplicationConfig.class.getClassLoader().getResourceAsStream("application.properties");
        properties.load(in);
    }

    private static synchronized ApplicationConfig getInstance() throws IOException {
        if (config == null) {
            config = new ApplicationConfig();
        }
        return config;
    }

    public static String getProperty(String key) {
        try {
            String data = String.valueOf(getInstance().properties.get(key));
            if (data != null) {
                return data;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "";
    }


}
