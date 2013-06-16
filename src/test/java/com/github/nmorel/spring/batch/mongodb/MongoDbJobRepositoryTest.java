package com.github.nmorel.spring.batch.mongodb;

import com.mongodb.DB;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;

@RunWith( SpringJUnit4ClassRunner.class )
@ContextConfiguration( classes = {TestContext.class} )
public class MongoDbJobRepositoryTest
{
    /** Logger */
    private final Log logger = LogFactory.getLog(getClass());

    private JobSupport job;

    private Set<Long> jobExecutionIds = new HashSet<Long>();

    private Set<Long> jobIds = new HashSet<Long>();

    private List<Serializable> list = new ArrayList<Serializable>();

    @Autowired
    private JobRepository repository;

    @Autowired
    private DB db;

    @Before
    public void onSetUpInTransaction() throws Exception
    {
        job = new JobSupport("test-job");
        job.setRestartable(true);

        db.dropDatabase();
    }

    @After
    public void tearDown()
    {
        db.dropDatabase();
    }

    @Test
    public void testFindOrCreateJob() throws Exception
    {
        job.setName("foo");
        int before = 0;
        JobExecution execution = repository.createJobExecution(job.getName(), new JobParameters());
        int after = (int) db.getCollection("BatchJobInstance").count();
        assertEquals(before + 1, after);
        assertNotNull(execution.getId());

        try
        {
            repository.createJobExecution(job.getName(), new JobParameters());
            fail();
        }
        catch( JobExecutionAlreadyRunningException e )
        {
        }
        after = (int) db.getCollection("BatchJobInstance").count();
        assertEquals(before + 1, after);
        assertNotNull(execution.getId());
    }

    @Test
    public void testFindOrCreateJobWithExecutionContext() throws Exception
    {
        job.setName("foo");
        int before = 0;
        JobExecution execution = repository.createJobExecution(job.getName(), new JobParameters());
        execution.getExecutionContext().put("foo", "bar");
        repository.updateExecutionContext(execution);
        int after = (int) db.getCollection("BatchJobExecution").count();
        assertEquals(before + 1, after);
        assertNotNull(execution.getId());
        JobExecution last = repository.getLastJobExecution(job.getName(), new JobParameters());
        assertEquals(execution, last);
        assertEquals(execution.getExecutionContext(), last.getExecutionContext());
    }

    @Test
    public void testFindOrCreateJobConcurrently() throws Exception
    {

        job.setName("bar");

        int before = 0;
        assertEquals(0, before);

        long t0 = System.currentTimeMillis();
        try
        {
            doConcurrentStart();
            fail("Expected JobExecutionAlreadyRunningException");
        }
        catch( JobExecutionAlreadyRunningException e )
        {
            // expected
        }
        long t1 = System.currentTimeMillis();

        JobExecution execution = (JobExecution) list.get(0);

        assertNotNull(execution);

        int after = (int) db.getCollection("BatchJobInstance").count();
        assertNotNull(execution.getId());
        assertEquals(before + 1, after);

        logger.info("Duration: " + (t1 - t0)
                + " - the second transaction did not block if this number is less than about 1000.");
    }

    @Test
    public void testFindOrCreateJobConcurrentlyWhenJobAlreadyExists() throws Exception
    {

        job = new JobSupport("test-job");
        job.setRestartable(true);
        job.setName("spam");

        JobExecution execution = repository.createJobExecution(job.getName(), new JobParameters());
        cacheJobIds(execution);
        execution.setEndTime(new Timestamp(System.currentTimeMillis()));
        repository.update(execution);
        execution.setStatus(BatchStatus.FAILED);

        int before = (int) db.getCollection("BatchJobInstance").count();
        assertEquals(1, before);

        long t0 = System.currentTimeMillis();
        try
        {
            doConcurrentStart();
            fail("Expected JobExecutionAlreadyRunningException");
        }
        catch( JobExecutionAlreadyRunningException e )
        {
            // expected
        }
        long t1 = System.currentTimeMillis();

        int after = (int) db.getCollection("BatchJobInstance").count();
        assertNotNull(execution.getId());
        assertEquals(before, after);

        logger.info("Duration: " + (t1 - t0)
                + " - the second transaction did not block if this number is less than about 1000.");
    }

    private void cacheJobIds( JobExecution execution )
    {
        if( execution == null )
        {
            return;
        }
        jobExecutionIds.add(execution.getId());
        jobIds.add(execution.getJobId());
    }

    private JobExecution doConcurrentStart() throws Exception
    {
        new Thread(new Runnable()
        {
            public void run()
            {

                try
                {
                    JobExecution execution = repository.createJobExecution(job.getName(), new JobParameters());
                    cacheJobIds(execution);
                    list.add(execution);
                    Thread.sleep(1000);
                }
                catch( Exception e )
                {
                    list.add(e);
                }

            }
        }).start();

        Thread.sleep(400);
        JobExecution execution = repository.createJobExecution(job.getName(), new JobParameters());
        cacheJobIds(execution);

        int count = 0;
        while( list.size() == 0 && count++ < 100 )
        {
            Thread.sleep(200);
        }

        assertEquals("Timed out waiting for JobExecution to be created", 1, list.size());
        assertTrue("JobExecution not created in thread: " + list.get(0), list.get(0) instanceof JobExecution);
        return (JobExecution) list.get(0);
    }
}
