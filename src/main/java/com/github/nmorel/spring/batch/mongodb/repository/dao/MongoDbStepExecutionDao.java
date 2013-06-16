package com.github.nmorel.spring.batch.mongodb.repository.dao;


import com.github.nmorel.spring.batch.mongodb.incrementer.ValueIncrementer;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.repository.dao.StepExecutionDao;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.util.Assert;

import java.util.Collection;
import java.util.Date;

import static com.mongodb.BasicDBObjectBuilder.start;

/** {@link org.springframework.batch.core.repository.dao.StepExecutionDao} implementation for MongoDB */
public class MongoDbStepExecutionDao extends AbstractMongoDbDao implements StepExecutionDao
{
    private static final Log logger = LogFactory.getLog(MongoDbStepExecutionDao.class);

    private static final String COLLECTION_NAME = StepExecution.class.getSimpleName();

    private static final String STEP_NAME_KEY = "stepName";

    private static final String COMMIT_COUNT_KEY = "commitCount";

    private static final String READ_COUNT_KEY = "readCount";

    private static final String FILTER_COUT_KEY = "filterCout";

    private static final String WRITE_COUNT_KEY = "writeCount";

    private static final String READ_SKIP_COUNT_KEY = "readSkipCount";

    private static final String WRITE_SKIP_COUNT_KEY = "writeSkipCount";

    private static final String PROCESS_SKIP_COUT_KEY = "processSkipCout";

    private static final String ROLLBACK_COUNT_KEY = "rollbackCount";

    private int exitMessageLength = DEFAULT_EXIT_MESSAGE_LENGTH;

    private ValueIncrementer stepExecutionIncrementer;

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

    public void setStepExecutionIncrementer( ValueIncrementer stepExecutionIncrementer )
    {
        this.stepExecutionIncrementer = stepExecutionIncrementer;
    }

    @Override
    public void afterPropertiesSet() throws Exception
    {
        super.afterPropertiesSet();
        Assert.notNull(stepExecutionIncrementer, "StepExecutionIncrementer cannot be null.");
        getCollection().ensureIndex(BasicDBObjectBuilder.start().add(STEP_EXECUTION_ID_KEY, 1).add(JOB_EXECUTION_ID_KEY, 1).get());
    }

    @Override
    protected String getCollectionName()
    {
        return COLLECTION_NAME;
    }

    @Override
    public void saveStepExecution( StepExecution stepExecution )
    {
        DBObject dbObject = buildStepExecutionParameters(stepExecution);
        getCollection().save(dbObject);
    }

    @Override
    public void saveStepExecutions( Collection<StepExecution> stepExecutions )
    {
        Assert.notNull(stepExecutions, "Attempt to save a null collection of step executions");

        if( !stepExecutions.isEmpty() )
        {
            for( StepExecution stepExecution : stepExecutions )
            {
                saveStepExecution(stepExecution);
            }
        }
    }

    private DBObject buildStepExecutionParameters( StepExecution stepExecution )
    {
        Assert.isNull(stepExecution.getId(),
                "to-be-saved (not updated) StepExecution can't already have an id assigned");
        Assert.isNull(stepExecution.getVersion(),
                "to-be-saved (not updated) StepExecution can't already have a version assigned");
        validateStepExecution(stepExecution);
        stepExecution.setId(stepExecutionIncrementer.nextLongValue());
        stepExecution.incrementVersion(); //Should be 0
        DBObject object = toDbObjectWithoutVersion(stepExecution);
        object.put(VERSION_KEY, stepExecution.getVersion());
        return object;
    }

    /**
     * Validate StepExecution. At a minimum, JobId, StartTime, and Status cannot
     * be null. EndTime can be null for an unfinished job.
     */
    private void validateStepExecution( StepExecution stepExecution )
    {
        Assert.notNull(stepExecution);
        Assert.notNull(stepExecution.getStepName(), "StepExecution step name cannot be null.");
        Assert.notNull(stepExecution.getStartTime(), "StepExecution start time cannot be null.");
        Assert.notNull(stepExecution.getStatus(), "StepExecution status cannot be null.");
    }

    @Override
    public void updateStepExecution( StepExecution stepExecution )
    {
        validateStepExecution(stepExecution);
        Assert.notNull(stepExecution.getId(), "StepExecution Id cannot be null. StepExecution must saved"
                + " before it can be updated.");

        // Do not check for existence of step execution considering
        // it is saved at every commit point.

        // Attempt to prevent concurrent modification errors by blocking here if
        // someone is already trying to do it.
        synchronized(stepExecution)
        {
            Integer version = stepExecution.getVersion() + 1;
            DBObject object = toDbObjectWithoutVersion(stepExecution);
            object.put(VERSION_KEY, version);
            getCollection().update(start()
                    .add(STEP_EXECUTION_ID_KEY, stepExecution.getId())
                    .add(VERSION_KEY, stepExecution.getVersion()).get(),
                    object);

            // Avoid concurrent modifications...
            DBObject lastError = db.getLastError();
            if( !((Boolean) lastError.get(UPDATED_EXISTING_STATUS)) )
            {
                DBObject existingStepExecution = getCollection()
                        .findOne(new BasicDBObject(STEP_EXECUTION_ID_KEY, stepExecution.getId()), new BasicDBObject(VERSION_KEY, 1));
                if( existingStepExecution == null )
                {
                    throw new IllegalArgumentException("Can't update this stepExecution, it was never saved.");
                }
                Integer curentVersion = ((Integer) existingStepExecution.get(VERSION_KEY));
                throw new OptimisticLockingFailureException("Attempt to update step execution id="
                        + stepExecution.getId() + " with wrong version (" + stepExecution.getVersion()
                        + "), where current version is " + curentVersion);
            }

            stepExecution.incrementVersion();
        }
    }

    @Override
    public StepExecution getStepExecution( JobExecution jobExecution, Long stepExecutionId )
    {
        return mapStepExecution(getCollection().findOne(BasicDBObjectBuilder.start()
                .add(STEP_EXECUTION_ID_KEY, stepExecutionId)
                .add(JOB_EXECUTION_ID_KEY, jobExecution.getId()).get()), jobExecution);
    }

    @Override
    public void addStepExecutions( JobExecution jobExecution )
    {
        DBCursor stepsCursor = getCollection().find(new BasicDBObject(JOB_EXECUTION_ID_KEY, jobExecution.getId()))
                .sort(new BasicDBObject(STEP_EXECUTION_ID_KEY, 1L));
        while( stepsCursor.hasNext() )
        {
            DBObject stepObject = stepsCursor.next();
            mapStepExecution(stepObject, jobExecution);
        }
        stepsCursor.close();
    }

    /**
     * Truncate the exit description if the length exceeds
     * {@link #DEFAULT_EXIT_MESSAGE_LENGTH}.
     *
     * @param description the string to truncate
     *
     * @return truncated description
     */
    private String truncateExitDescription( String description )
    {
        if( description != null && description.length() > exitMessageLength )
        {
            logger.debug("Truncating long message before update of StepExecution, original message is: " + description);
            return description.substring(0, exitMessageLength);
        }
        else
        {
            return description;
        }
    }

    private DBObject toDbObjectWithoutVersion( StepExecution stepExecution )
    {
        String exitDescription = truncateExitDescription(stepExecution.getExitStatus().getExitDescription());
        return start()
                .add(STEP_EXECUTION_ID_KEY, stepExecution.getId())
                .add(STEP_NAME_KEY, stepExecution.getStepName())
                .add(JOB_EXECUTION_ID_KEY, stepExecution.getJobExecutionId())
                .add(START_TIME_KEY, stepExecution.getStartTime())
                .add(END_TIME_KEY, stepExecution.getEndTime())
                .add(STATUS_KEY, stepExecution.getStatus().toString())
                .add(COMMIT_COUNT_KEY, stepExecution.getCommitCount())
                .add(READ_COUNT_KEY, stepExecution.getReadCount())
                .add(FILTER_COUT_KEY, stepExecution.getFilterCount())
                .add(WRITE_COUNT_KEY, stepExecution.getWriteCount())
                .add(EXIT_CODE_KEY, stepExecution.getExitStatus().getExitCode())
                .add(EXIT_MESSAGE_KEY, exitDescription)
                .add(READ_SKIP_COUNT_KEY, stepExecution.getReadSkipCount())
                .add(WRITE_SKIP_COUNT_KEY, stepExecution.getWriteSkipCount())
                .add(PROCESS_SKIP_COUT_KEY, stepExecution.getProcessSkipCount())
                .add(ROLLBACK_COUNT_KEY, stepExecution.getRollbackCount())
                .add(LAST_UPDATED_KEY, stepExecution.getLastUpdated()).get();
    }

    private StepExecution mapStepExecution( DBObject object, JobExecution jobExecution )
    {
        if( object == null )
        {
            return null;
        }

        StepExecution stepExecution = new StepExecution((String) object.get(STEP_NAME_KEY), jobExecution, ((Long) object.get(STEP_EXECUTION_ID_KEY)));
        stepExecution.setStartTime((Date) object.get(START_TIME_KEY));
        stepExecution.setEndTime((Date) object.get(END_TIME_KEY));
        stepExecution.setStatus(BatchStatus.valueOf((String) object.get(STATUS_KEY)));
        stepExecution.setCommitCount((Integer) object.get(COMMIT_COUNT_KEY));
        stepExecution.setReadCount((Integer) object.get(READ_COUNT_KEY));
        stepExecution.setFilterCount((Integer) object.get(FILTER_COUT_KEY));
        stepExecution.setWriteCount((Integer) object.get(WRITE_COUNT_KEY));
        stepExecution.setExitStatus(new ExitStatus((String) object.get(EXIT_CODE_KEY), ((String) object.get(EXIT_MESSAGE_KEY))));
        stepExecution.setReadSkipCount((Integer) object.get(READ_SKIP_COUNT_KEY));
        stepExecution.setWriteSkipCount((Integer) object.get(WRITE_SKIP_COUNT_KEY));
        stepExecution.setProcessSkipCount((Integer) object.get(PROCESS_SKIP_COUT_KEY));
        stepExecution.setRollbackCount((Integer) object.get(ROLLBACK_COUNT_KEY));
        stepExecution.setLastUpdated((Date) object.get(LAST_UPDATED_KEY));
        stepExecution.setVersion((Integer) object.get(VERSION_KEY));
        return stepExecution;
    }
}
