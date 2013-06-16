package com.github.nmorel.spring.batch.mongodb.repository.support;

import com.github.nmorel.spring.batch.mongodb.incrementer.MongoDbValueIncrementerFactory;
import com.github.nmorel.spring.batch.mongodb.incrementer.ValueIncrementerFactory;
import com.github.nmorel.spring.batch.mongodb.repository.dao.*;
import com.mongodb.DB;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.repository.ExecutionContextSerializer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.dao.*;
import org.springframework.batch.core.repository.support.SimpleJobRepository;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

/**
 * A {@link org.springframework.beans.factory.FactoryBean} that automates the creation of a
 * {@link org.springframework.batch.core.repository.support.SimpleJobRepository} with MongoDB dao.
 */
public class MongoDbJobRepositoryFactoryBean implements FactoryBean, InitializingBean
{

    protected static final Log logger = LogFactory.getLog(MongoDbJobRepositoryFactoryBean.class);

    private DB db;

    private String collectionPrefix = AbstractMongoDbDao.DEFAULT_COLLECTION_PREFIX;

    private ValueIncrementerFactory incrementerFactory;

    private int maxVarCharLength = AbstractMongoDbDao.DEFAULT_EXIT_MESSAGE_LENGTH;

    private ExecutionContextSerializer serializer;

    /**
     * A custom implementation of the {@link ExecutionContextSerializer}.
     * The default, if not injected, is the {@link org.springframework.batch.core.repository.dao.XStreamExecutionContextStringSerializer}.
     *
     * @see ExecutionContextSerializer
     */
    public void setSerializer( ExecutionContextSerializer serializer )
    {
        this.serializer = serializer;
    }

    /**
     * Public setter for the length of long string columns in database. Note this value will be used
     * for the exit message in both {@link com.github.nmorel.spring.batch.mongodb.repository.dao.MongoDbJobExecutionDao} and
     * {@link com.github.nmorel.spring.batch.mongodb.repository.dao.MongoDbStepExecutionDao} and also the short version of the execution
     * context in {@link com.github.nmorel.spring.batch.mongodb.repository.dao.MongoDbExecutionContextDao}.
     *
     * @param maxVarCharLength the exitMessageLength to set
     */
    public void setMaxVarCharLength( int maxVarCharLength )
    {
        this.maxVarCharLength = maxVarCharLength;
    }

    /**
     * Public setter for the {@link DB}.
     *
     * @param db a {@link DB}
     */
    public void setDb( DB db )
    {
        this.db = db;
    }

    /** Sets the collection prefix for all the batch meta-data tables. */
    public void setCollectionPrefix( String collectionPrefix )
    {
        this.collectionPrefix = collectionPrefix;
    }

    @Override
    public void afterPropertiesSet() throws Exception
    {
        Assert.notNull(db, "db must not be null.");

        if( incrementerFactory == null )
        {
            incrementerFactory = new MongoDbValueIncrementerFactory(db);
        }

        if( serializer == null )
        {
            XStreamExecutionContextStringSerializer defaultSerializer = new XStreamExecutionContextStringSerializer();
            defaultSerializer.afterPropertiesSet();

            serializer = defaultSerializer;
        }
    }

    protected JobInstanceDao createJobInstanceDao() throws Exception
    {
        MongoDbJobInstanceDao dao = new MongoDbJobInstanceDao();
        dao.setPrefix(collectionPrefix);
        dao.setDb(db);
        dao.setJobIncrementer(incrementerFactory.getIncrementer(collectionPrefix
                + "Sequence" + JobInstance.class.getSimpleName()));
        dao.afterPropertiesSet();
        return dao;
    }

    protected JobExecutionDao createJobExecutionDao() throws Exception
    {
        MongoDbJobExecutionDao dao = new MongoDbJobExecutionDao();
        dao.setDb(db);
        dao.setPrefix(collectionPrefix);
        dao.setJobExecutionIncrementer(incrementerFactory.getIncrementer(collectionPrefix
                + "Sequence" + JobExecution.class.getSimpleName()));
        dao.setExitMessageLength(maxVarCharLength);
        dao.afterPropertiesSet();
        return dao;
    }

    protected StepExecutionDao createStepExecutionDao() throws Exception
    {
        MongoDbStepExecutionDao dao = new MongoDbStepExecutionDao();
        dao.setDb(db);
        dao.setPrefix(collectionPrefix);
        dao.setStepExecutionIncrementer(incrementerFactory.getIncrementer(collectionPrefix
                + "Sequence" + StepExecution.class.getSimpleName()));
        dao.setExitMessageLength(maxVarCharLength);
        dao.afterPropertiesSet();
        return dao;
    }

    protected ExecutionContextDao createExecutionContextDao() throws Exception
    {
        MongoDbExecutionContextDao dao = new MongoDbExecutionContextDao();
        dao.setDb(db);
        dao.setSerializer(serializer);
        dao.setPrefix(collectionPrefix);
        dao.afterPropertiesSet();
        return dao;
    }

    /**
     * The type of object to be returned from {@link #getObject()}.
     *
     * @return JobRepository.class
     *
     * @see org.springframework.beans.factory.FactoryBean#getObjectType()
     */
    @Override
    public Class<JobRepository> getObjectType()
    {
        return JobRepository.class;
    }

    @Override
    public boolean isSingleton()
    {
        return true;
    }

    @Override
    public Object getObject() throws Exception
    {
        return getTarget();
    }

    private Object getTarget() throws Exception
    {
        return new SimpleJobRepository(createJobInstanceDao(), createJobExecutionDao(), createStepExecutionDao(),
                createExecutionContextDao());
    }
}
