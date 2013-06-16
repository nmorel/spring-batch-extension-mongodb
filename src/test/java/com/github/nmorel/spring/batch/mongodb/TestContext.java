package com.github.nmorel.spring.batch.mongodb;

import com.github.nmorel.spring.batch.mongodb.configuration.annotation.MongoDbBatchConfigurer;
import com.github.nmorel.spring.batch.mongodb.explore.support.MongoDbJobExplorerFactoryBean;
import com.mongodb.DB;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.configuration.annotation.BatchConfigurer;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.support.JobRegistryBeanPostProcessor;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.launch.support.SimpleJobOperator;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@EnableBatchProcessing
@Import( {ConfigContext.class} )
public class TestContext
{
    @Bean
    public MongoDbJobExplorerFactoryBean jobExplorerFactoryBean( DB db )
    {
        MongoDbJobExplorerFactoryBean factory = new MongoDbJobExplorerFactoryBean();
        factory.setDb(db);
        return factory;
    }

    @Bean
    public BatchConfigurer batchConfigurer( final DB db )
    {
        return new MongoDbBatchConfigurer(db);
    }

    @Bean
    JobOperator jobOperator( final JobLauncher jobLauncher, final JobExplorer jobExplorer,
                             final JobRepository jobRepository, final JobRegistry jobRegistry )
    {
        return new SimpleJobOperator()
        {{
                setJobLauncher(jobLauncher);
                setJobExplorer(jobExplorer);
                setJobRepository(jobRepository);
                setJobRegistry(jobRegistry);
            }};
    }

    @Bean
    JobRegistryBeanPostProcessor jobRegisterBeanPostProcess( final JobRegistry jobRegistry )
    {
        return new JobRegistryBeanPostProcessor()
        {{
                setJobRegistry(jobRegistry);
            }};
    }

}
