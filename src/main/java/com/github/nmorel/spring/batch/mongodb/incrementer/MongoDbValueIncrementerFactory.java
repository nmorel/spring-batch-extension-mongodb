package com.github.nmorel.spring.batch.mongodb.incrementer;

import com.mongodb.DB;
import org.springframework.util.Assert;

/**
 * MongoDB implementation of the {@link ValueIncrementerFactory}
 * interface.
 */
public class MongoDbValueIncrementerFactory implements ValueIncrementerFactory
{
    /** The MongoDB database */
    private DB db;

    public MongoDbValueIncrementerFactory( DB db )
    {
        Assert.notNull(db, "db must not be null");
        this.db = db;
    }

    @Override
    public ValueIncrementer getIncrementer( String incrementerName )
    {
        Assert.notNull(incrementerName);
        MongoDbValueIncrementer incrementer = new MongoDbValueIncrementer(db, incrementerName);
        incrementer.afterPropertiesSet();
        return incrementer;
    }
}
