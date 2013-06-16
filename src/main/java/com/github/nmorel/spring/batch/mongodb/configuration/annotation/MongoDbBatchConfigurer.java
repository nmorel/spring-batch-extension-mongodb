package com.github.nmorel.spring.batch.mongodb.configuration.annotation;

import com.github.nmorel.spring.batch.mongodb.repository.support.MongoDbJobRepositoryFactoryBean;
import com.mongodb.DB;
import org.springframework.batch.core.configuration.annotation.BatchConfigurer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;

import javax.annotation.PostConstruct;

/** Implementation of {@link BatchConfigurer} for MongoDB */
@Component
public class MongoDbBatchConfigurer implements BatchConfigurer
{
    private DB db;

    private PlatformTransactionManager transactionManager = new ResourcelessTransactionManager();

    private JobRepository jobRepository;

    private JobLauncher jobLauncher;

    protected MongoDbBatchConfigurer() {}

    public MongoDbBatchConfigurer( DB db )
    {
        setDb(db);
    }

    public void setDb( DB db )
    {
        this.db = db;
    }

    @Override
    public JobRepository getJobRepository()
    {
        return jobRepository;
    }

    @Override
    public PlatformTransactionManager getTransactionManager()
    {
        return transactionManager;
    }

    @Override
    public JobLauncher getJobLauncher()
    {
        return jobLauncher;
    }

    @PostConstruct
    public void initialize() throws Exception
    {
        this.jobRepository = createJobRepository();
        this.jobLauncher = createJobLauncher();
    }

    private JobLauncher createJobLauncher() throws Exception
    {
        SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
        jobLauncher.setJobRepository(jobRepository);
        jobLauncher.afterPropertiesSet();
        return jobLauncher;
    }

    protected JobRepository createJobRepository() throws Exception
    {
        MongoDbJobRepositoryFactoryBean factory = new MongoDbJobRepositoryFactoryBean();
        factory.setDb(db);
        factory.afterPropertiesSet();
        return (JobRepository) factory.getObject();
    }
}
