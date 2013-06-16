package com.github.nmorel.spring.batch.mongodb.repository.dao;

import com.github.nmorel.spring.batch.mongodb.incrementer.ValueIncrementer;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import org.springframework.batch.core.*;
import org.springframework.batch.core.repository.dao.JobInstanceDao;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.mongodb.BasicDBObjectBuilder.start;


/** {@link org.springframework.batch.core.repository.dao.JobInstanceDao} implementation for MongoDB */
public class MongoDbJobInstanceDao extends AbstractMongoDbDao implements JobInstanceDao
{
    static final String COLLECTION_NAME = JobInstance.class.getSimpleName();

    private static final String JOB_KEY_KEY = "jobKey";

    private ValueIncrementer jobIncrementer;

    private JobKeyGenerator<JobParameters> jobKeyGenerator = new DefaultJobKeyGenerator();

    /**
     * Setter for {@link ValueIncrementer} to be used when
     * generating primary keys for {@link JobInstance} instances.
     *
     * @param jobIncrementer the {@link ValueIncrementer}
     */
    public void setJobIncrementer( ValueIncrementer jobIncrementer )
    {
        this.jobIncrementer = jobIncrementer;
    }

    @Override
    public void afterPropertiesSet() throws Exception
    {
        super.afterPropertiesSet();
        Assert.notNull(jobIncrementer, "The jobIncrementer must not be null.");
        getCollection().ensureIndex(new BasicDBObject(JOB_INSTANCE_ID_KEY, 1L));
    }

    @Override
    protected String getCollectionName()
    {
        return COLLECTION_NAME;
    }

    @Override
    public JobInstance createJobInstance( String jobName, JobParameters jobParameters )
    {
        Assert.notNull(jobName, "Job name must not be null.");
        Assert.notNull(jobParameters, "JobParameters must not be null.");

        Assert.state(getJobInstance(jobName, jobParameters) == null,
                "JobInstance must not already exist");

        Long jobId = jobIncrementer.nextLongValue();

        JobInstance jobInstance = new JobInstance(jobId, jobName);
        jobInstance.incrementVersion();

        getCollection().save(start()
                .add(JOB_INSTANCE_ID_KEY, jobId)
                .add(JOB_NAME_KEY, jobName)
                .add(JOB_KEY_KEY, jobKeyGenerator.generateKey(jobParameters))
                .add(VERSION_KEY, jobInstance.getVersion()).get());

        return jobInstance;
    }

    @Override
    public JobInstance getJobInstance( String jobName, JobParameters jobParameters )
    {
        Assert.notNull(jobName, "Job name must not be null.");
        Assert.notNull(jobParameters, "JobParameters must not be null.");

        String jobKey = jobKeyGenerator.generateKey(jobParameters);

        return mapJobInstance(getCollection().findOne(start()
                .add(JOB_NAME_KEY, jobName)
                .add(JOB_KEY_KEY, jobKey).get()));
    }

    @Override
    public JobInstance getJobInstance( Long instanceId )
    {
        return mapJobInstance(getCollection().findOne(new BasicDBObject(JOB_INSTANCE_ID_KEY, instanceId)));
    }

    @Override
    public JobInstance getJobInstance( JobExecution jobExecution )
    {
        DBObject instanceId = getCollection(MongoDbJobExecutionDao.COLLECTION_NAME)
                .findOne(new BasicDBObject(JOB_EXECUTION_ID_KEY, jobExecution.getId()), new BasicDBObject(JOB_INSTANCE_ID_KEY, 1L));
        return mapJobInstance(getCollection().findOne(new BasicDBObject(JOB_INSTANCE_ID_KEY, instanceId.get(JOB_INSTANCE_ID_KEY))));
    }

    @Override
    public List<JobInstance> getJobInstances( String jobName, int start, int count )
    {
        return mapJobInstances(getCollection().find(new BasicDBObject(JOB_NAME_KEY, jobName)).sort(new BasicDBObject(JOB_INSTANCE_ID_KEY, -1L))
                .skip(start).limit(count));
    }

    @Override
    public List<String> getJobNames()
    {
        List results = getCollection().distinct(JOB_NAME_KEY);
        Collections.sort(results);
        return results;
    }

    private List<JobInstance> mapJobInstances( DBCursor dbCursor )
    {
        List<JobInstance> results = new ArrayList<JobInstance>();
        while( dbCursor.hasNext() )
        {
            results.add(mapJobInstance(dbCursor.next()));
        }
        dbCursor.close();
        return results;
    }

    private JobInstance mapJobInstance( DBObject dbObject )
    {
        if( dbObject == null )
        {
            return null;
        }
        JobInstance jobInstance = new JobInstance((Long) dbObject.get(JOB_INSTANCE_ID_KEY), (String) dbObject.get(JOB_NAME_KEY));
        // should always be at version=0 because they never get updated
        jobInstance.incrementVersion();
        return jobInstance;
    }
}
