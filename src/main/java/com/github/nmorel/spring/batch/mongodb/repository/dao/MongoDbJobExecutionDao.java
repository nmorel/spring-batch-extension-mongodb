package com.github.nmorel.spring.batch.mongodb.repository.dao;

import com.github.nmorel.spring.batch.mongodb.incrementer.ValueIncrementer;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.*;
import org.springframework.batch.core.repository.dao.JobExecutionDao;
import org.springframework.batch.core.repository.dao.NoSuchObjectException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.util.Assert;

import java.util.*;

import static com.mongodb.BasicDBObjectBuilder.start;

/** {@link org.springframework.batch.core.repository.dao.JobExecutionDao} implementation for MongoDB */
public class MongoDbJobExecutionDao extends AbstractMongoDbDao implements JobExecutionDao
{
    static final String COLLECTION_NAME = JobExecution.class.getSimpleName();

    private static final Log logger = LogFactory.getLog(MongoDbJobExecutionDao.class);

    private static final String CREATE_TIME_KEY = "createTime";

    private static final String PARAM_COLLECTION_NAME = JobParameter.class.getSimpleName();

    private static final String PARAM_KEY_NAME_KEY = "keyName";

    private static final String PARAM_TYPE_KEY = "type";

    private static final String PARAM_STRING_VAL_KEY = "stringVal";

    private static final String PARAM_DATE_VAL_KEY = "dateVal";

    private static final String PARAM_LONG_VAL_KEY = "longVal";

    private static final String PARAM_DOUBLE_VAL_KEY = "doubleVal";

    private static final String PARAM_IDENTIFYING_KEY = "identifying";

    private int exitMessageLength = DEFAULT_EXIT_MESSAGE_LENGTH;

    private ValueIncrementer jobExecutionIncrementer;

    /**
     * Public setter for the exit message length in database. Do not set this if
     * you haven't modified the schema.
     *
     * @param exitMessageLength the exitMessageLength to set
     */
    public void setExitMessageLength( int exitMessageLength )
    {
        this.exitMessageLength = exitMessageLength;
    }

    /**
     * Setter for {@link ValueIncrementer} to be used when
     * generating primary keys for {@link JobExecution} instances.
     *
     * @param jobExecutionIncrementer the {@link ValueIncrementer}
     */
    public void setJobExecutionIncrementer( ValueIncrementer jobExecutionIncrementer )
    {
        this.jobExecutionIncrementer = jobExecutionIncrementer;
    }

    @Override
    public void afterPropertiesSet() throws Exception
    {
        super.afterPropertiesSet();
        Assert.notNull(jobExecutionIncrementer, "The jobExecutionIncrementer must not be null.");

        getCollection().ensureIndex(BasicDBObjectBuilder.start().add(JOB_EXECUTION_ID_KEY, 1).add(JOB_INSTANCE_ID_KEY, 1).get());
        getCollection(PARAM_COLLECTION_NAME).ensureIndex(BasicDBObjectBuilder.start().add(JOB_EXECUTION_ID_KEY, 1).get());
    }

    @Override
    protected String getCollectionName()
    {
        return COLLECTION_NAME;
    }

    @Override
    public void saveJobExecution( JobExecution jobExecution )
    {
        validateJobExecution(jobExecution);

        jobExecution.incrementVersion();

        Long id = jobExecutionIncrementer.nextLongValue();
        save(jobExecution, id);

        insertJobParameters(jobExecution.getId(), jobExecution.getJobParameters());
    }

    /**
     * Validate JobExecution. At a minimum, JobId, StartTime, EndTime, and
     * Status cannot be null.
     */
    private void validateJobExecution( JobExecution jobExecution )
    {
        Assert.notNull(jobExecution);
        Assert.notNull(jobExecution.getJobId(), "JobExecution Job-Id cannot be null.");
        Assert.notNull(jobExecution.getStatus(), "JobExecution status cannot be null.");
        Assert.notNull(jobExecution.getCreateTime(), "JobExecution create time cannot be null");
    }

    private DBObject toDbObjectWithoutVersion( JobExecution jobExecution )
    {
        String exitDescription = jobExecution.getExitStatus().getExitDescription();
        if( exitDescription != null && exitDescription.length() > exitMessageLength )
        {
            exitDescription = exitDescription.substring(0, exitMessageLength);
            logger.debug("Truncating long message before update of JobExecution: " + jobExecution);
        }
        return start()
                .add(JOB_EXECUTION_ID_KEY, jobExecution.getId())
                .add(JOB_INSTANCE_ID_KEY, jobExecution.getJobId())
                .add(START_TIME_KEY, jobExecution.getStartTime())
                .add(END_TIME_KEY, jobExecution.getEndTime())
                .add(STATUS_KEY, jobExecution.getStatus().toString())
                .add(EXIT_CODE_KEY, jobExecution.getExitStatus().getExitCode())
                .add(EXIT_MESSAGE_KEY, exitDescription)
                .add(CREATE_TIME_KEY, jobExecution.getCreateTime())
                .add(LAST_UPDATED_KEY, jobExecution.getLastUpdated()).get();
    }

    private void save( JobExecution jobExecution, Long id )
    {
        jobExecution.setId(id);
        DBObject object = toDbObjectWithoutVersion(jobExecution);
        object.put(VERSION_KEY, jobExecution.getVersion());
        getCollection().save(object);
    }

    @Override
    public void updateJobExecution( JobExecution jobExecution )
    {
        validateJobExecution(jobExecution);

        Assert.notNull(jobExecution.getId(),
                "JobExecution ID cannot be null. JobExecution must be saved before it can be updated");

        Assert.notNull(jobExecution.getVersion(),
                "JobExecution version cannot be null. JobExecution must be saved before it can be updated");

        synchronized(jobExecution)
        {
            Integer version = jobExecution.getVersion() + 1;

            if( getCollection().findOne(new BasicDBObject(JOB_EXECUTION_ID_KEY, jobExecution.getId())) == null )
            {
                throw new NoSuchObjectException("Invalid JobExecution, ID " + jobExecution.getId() + " not found.");
            }

            DBObject object = toDbObjectWithoutVersion(jobExecution);
            object.put(VERSION_KEY, version);
            getCollection().update(start()
                    .add(JOB_EXECUTION_ID_KEY, jobExecution.getId())
                    .add(VERSION_KEY, jobExecution.getVersion()).get(),
                    object);

            // Avoid concurrent modifications...
            DBObject lastError = db.getLastError();
            if( !((Boolean) lastError.get(UPDATED_EXISTING_STATUS)) )
            {
                DBObject existingJobExecution = getCollection()
                        .findOne(new BasicDBObject(JOB_EXECUTION_ID_KEY, jobExecution.getId()), new BasicDBObject(VERSION_KEY, 1));
                if( existingJobExecution == null )
                {
                    throw new IllegalArgumentException("Can't update this jobExecution, it was never saved.");
                }
                Integer curentVersion = ((Integer) existingJobExecution.get(VERSION_KEY));
                throw new OptimisticLockingFailureException("Attempt to update job execution id="
                        + jobExecution.getId() + " with wrong version (" + jobExecution.getVersion()
                        + "), where current version is " + curentVersion);
            }

            jobExecution.incrementVersion();
        }
    }

    @Override
    public List<JobExecution> findJobExecutions( JobInstance jobInstance )
    {
        Assert.notNull(jobInstance, "Job cannot be null.");
        Assert.notNull(jobInstance.getId(), "Job Id cannot be null.");

        DBCursor dbCursor = getCollection().find(new BasicDBObject(JOB_INSTANCE_ID_KEY, jobInstance
                .getId())).sort(new BasicDBObject(JOB_EXECUTION_ID_KEY, -1));
        List<JobExecution> result = new ArrayList<JobExecution>();
        while( dbCursor.hasNext() )
        {
            DBObject dbObject = dbCursor.next();
            result.add(mapJobExecution(jobInstance, dbObject));
        }
        dbCursor.close();
        return result;
    }

    @Override
    public JobExecution getLastJobExecution( JobInstance jobInstance )
    {
        Long id = jobInstance.getId();

        DBCursor dbCursor = getCollection().find(new BasicDBObject(JOB_INSTANCE_ID_KEY, id)).sort(new BasicDBObject(CREATE_TIME_KEY, -1)).limit(1);
        if( !dbCursor.hasNext() )
        {
            dbCursor.close();
            return null;
        }
        else
        {
            DBObject singleResult = dbCursor.next();
            if( dbCursor.hasNext() )
            {
                throw new IllegalStateException("There must be at most one latest job execution");
            }
            dbCursor.close();
            return mapJobExecution(jobInstance, singleResult);
        }
    }

    @Override
    public Set<JobExecution> findRunningJobExecutions( String jobName )
    {
        DBCursor instancesCursor = getCollection(MongoDbJobInstanceDao.COLLECTION_NAME)
                .find(new BasicDBObject(JOB_NAME_KEY, jobName), new BasicDBObject(JOB_INSTANCE_ID_KEY, 1L));
        List<Long> ids = new ArrayList<Long>();
        while( instancesCursor.hasNext() )
        {
            ids.add((Long) instancesCursor.next().get(JOB_INSTANCE_ID_KEY));
        }
        instancesCursor.close();

        DBCursor dbCursor = getCollection().find(BasicDBObjectBuilder.start()
                .add(JOB_INSTANCE_ID_KEY, new BasicDBObject("$in", ids.toArray()))
                .add(END_TIME_KEY, null).get()).sort(new BasicDBObject(JOB_EXECUTION_ID_KEY, -1L));
        Set<JobExecution> result = new HashSet<JobExecution>();
        while( dbCursor.hasNext() )
        {
            result.add(mapJobExecution(dbCursor.next()));
        }
        dbCursor.close();
        return result;
    }

    @Override
    public JobExecution getJobExecution( Long executionId )
    {
        return mapJobExecution(getCollection().findOne(new BasicDBObject(JOB_EXECUTION_ID_KEY, executionId)));
    }

    @Override
    public void synchronizeStatus( JobExecution jobExecution )
    {
        Long id = jobExecution.getId();
        DBObject jobExecutionObject = getCollection().findOne(new BasicDBObject(JOB_EXECUTION_ID_KEY, id), start().add(VERSION_KEY, 1L).get());
        int currentVersion = jobExecutionObject != null ? ((Integer) jobExecutionObject.get(VERSION_KEY)) : 0;
        if( currentVersion != jobExecution.getVersion() )
        {
            if( jobExecutionObject == null )
            {
                save(jobExecution, id);
                jobExecutionObject = getCollection().findOne(new BasicDBObject(JOB_EXECUTION_ID_KEY, id), start().add(STATUS_KEY, 1L).get());
            }
            String status = (String) jobExecutionObject.get(STATUS_KEY);
            jobExecution.upgradeStatus(BatchStatus.valueOf(status));
            jobExecution.setVersion(currentVersion);
        }
    }

    private JobExecution mapJobExecution( DBObject dbObject )
    {
        return mapJobExecution(null, dbObject);
    }

    private JobExecution mapJobExecution( JobInstance jobInstance, DBObject dbObject )
    {
        if( dbObject == null )
        {
            return null;
        }

        Long id = (Long) dbObject.get(JOB_EXECUTION_ID_KEY);
        JobExecution jobExecution;
        JobParameters jobParameters = getJobParameters(id);
        if( jobInstance == null )
        {
            jobExecution = new JobExecution(id, jobParameters);
        }
        else
        {
            jobExecution = new JobExecution(jobInstance, id, jobParameters);
        }
        jobExecution.setStartTime((Date) dbObject.get(START_TIME_KEY));
        jobExecution.setEndTime((Date) dbObject.get(END_TIME_KEY));
        jobExecution.setStatus(BatchStatus.valueOf((String) dbObject.get(STATUS_KEY)));
        jobExecution.setExitStatus(new ExitStatus(((String) dbObject.get(EXIT_CODE_KEY)), (String) dbObject.get(EXIT_MESSAGE_KEY)));
        jobExecution.setCreateTime((Date) dbObject.get(CREATE_TIME_KEY));
        jobExecution.setLastUpdated((Date) dbObject.get(LAST_UPDATED_KEY));
        jobExecution.setVersion((Integer) dbObject.get(VERSION_KEY));
        return jobExecution;
    }

    /**
     * Convenience method that inserts all parameters from the provided
     * JobParameters.
     */
    private void insertJobParameters( Long executionId, JobParameters jobParameters )
    {
        for( Map.Entry<String, JobParameter> entry : jobParameters.getParameters()
                .entrySet() )
        {
            JobParameter jobParameter = entry.getValue();
            insertParameter(executionId, jobParameter.getType(), entry.getKey(),
                    jobParameter.getValue(), jobParameter.isIdentifying());
        }
    }

    /**
     * Convenience method that inserts an individual records into the
     * JobParameters table.
     */
    private void insertParameter( Long executionId, JobParameter.ParameterType type, String key,
                                  Object value, boolean identifying )
    {
        BasicDBObjectBuilder builder = start().add(JOB_EXECUTION_ID_KEY, executionId).add(PARAM_KEY_NAME_KEY, key).add(PARAM_TYPE_KEY, type.name())
                .add(PARAM_IDENTIFYING_KEY, identifying ? "Y" : "N");

        if( type == JobParameter.ParameterType.STRING )
        {
            builder.add(PARAM_STRING_VAL_KEY, value);
        }
        else if( type == JobParameter.ParameterType.LONG )
        {
            builder.add(PARAM_LONG_VAL_KEY, value);
        }
        else if( type == JobParameter.ParameterType.DOUBLE )
        {
            builder.add(PARAM_DOUBLE_VAL_KEY, value);
        }
        else if( type == JobParameter.ParameterType.DATE )
        {
            builder.add(PARAM_DATE_VAL_KEY, value);
        }

        getCollection(PARAM_COLLECTION_NAME).save(builder.get());
    }

    /**
     * @param executionId
     * @return
     */
    private JobParameters getJobParameters( Long executionId )
    {
        DBCursor cursor = getCollection(PARAM_COLLECTION_NAME).find(new BasicDBObject(JOB_EXECUTION_ID_KEY, executionId));

        final Map<String, JobParameter> map = new HashMap<String, JobParameter>();

        while( cursor.hasNext() )
        {
            DBObject dbObject = cursor.next();
            JobParameter.ParameterType type = JobParameter.ParameterType.valueOf((String) dbObject.get(PARAM_TYPE_KEY));
            JobParameter value = null;

            if( type == JobParameter.ParameterType.STRING )
            {
                value = new JobParameter((String) dbObject.get(PARAM_STRING_VAL_KEY), ((String) dbObject.get(PARAM_IDENTIFYING_KEY))
                        .equalsIgnoreCase("Y"));
            }
            else if( type == JobParameter.ParameterType.LONG )
            {
                value = new JobParameter((Long) dbObject.get(PARAM_LONG_VAL_KEY), ((String) dbObject.get(PARAM_IDENTIFYING_KEY))
                        .equalsIgnoreCase("Y"));
            }
            else if( type == JobParameter.ParameterType.DOUBLE )
            {
                value = new JobParameter((Double) dbObject.get(PARAM_DOUBLE_VAL_KEY), ((String) dbObject.get(PARAM_IDENTIFYING_KEY))
                        .equalsIgnoreCase("Y"));
            }
            else if( type == JobParameter.ParameterType.DATE )
            {
                value = new JobParameter((Date) dbObject.get(PARAM_DATE_VAL_KEY), ((String) dbObject.get(PARAM_IDENTIFYING_KEY))
                        .equalsIgnoreCase("Y"));
            }

            map.put((String) dbObject.get(PARAM_KEY_NAME_KEY), value);
        }

        cursor.close();

        return new JobParameters(map);
    }
}
