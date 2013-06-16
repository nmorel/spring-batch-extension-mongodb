package com.github.nmorel.spring.batch.mongodb;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.PropertyPlaceholderConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;

import java.net.UnknownHostException;

@Configuration
@PropertySource( "classpath:batch.properties" )
public class ConfigContext
{

    @Autowired
    Environment env;

    @Bean
    static PropertyPlaceholderConfigurer configurer()
    {
        return new PropertyPlaceholderConfigurer();
    }

    @Bean
    DB database() throws UnknownHostException
    {
        MongoClient client = new MongoClient(env.getProperty("mongodb.host"), env.getProperty("mongodb.port", int.class));
        return client.getDB(env.getProperty("mongodb.name"));
    }

}
