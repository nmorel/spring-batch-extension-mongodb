package com.github.nmorel.spring.batch.mongodb.repository.dao;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

/** Base class for all MongoDB DAO implementation */
public abstract class AbstractMongoDbDao implements InitializingBean
{
    public static final String DEFAULT_COLLECTION_PREFIX = "Batch";

    public static final int DEFAULT_EXIT_MESSAGE_LENGTH = 2500;

    protected static final String UPDATED_EXISTING_STATUS = "updatedExisting";

    protected static final String VERSION_KEY = "version";

    protected static final String START_TIME_KEY = "startTime";

    protected static final String END_TIME_KEY = "endTime";

    protected static final String EXIT_CODE_KEY = "exitCode";

    protected static final String EXIT_MESSAGE_KEY = "exitMessage";

    protected static final String LAST_UPDATED_KEY = "lastUpdated";

    protected static final String STATUS_KEY = "status";

    protected static final String JOB_EXECUTION_ID_KEY = "jobExecutionId";

    protected static final String JOB_INSTANCE_ID_KEY = "jobInstanceId";

    protected static final String STEP_EXECUTION_ID_KEY = "stepExecutionId";

    protected static final String JOB_NAME_KEY = "jobName";

    protected DB db;

    protected String prefix = DEFAULT_COLLECTION_PREFIX;

    public void setDb( DB db )
    {
        this.db = db;
    }

    public void setPrefix( String prefix )
    {
        this.prefix = prefix;
    }

    protected DBCollection getCollection()
    {
        return getCollection(getCollectionName());
    }

    protected DBCollection getCollection( String collectionName )
    {
        return db.getCollection(prefix + collectionName);
    }

    protected abstract String getCollectionName();

    @Override
    public void afterPropertiesSet() throws Exception
    {
        Assert.notNull(db, "The db must not be null.");
    }
}
