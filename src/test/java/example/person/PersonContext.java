package example.person;

import com.mongodb.DB;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;

@Configuration
public class PersonContext
{
    @Bean
    ItemReader<Person> itemReader()
    {
        FlatFileItemReader<Person> reader = new FlatFileItemReader<Person>();
        reader.setResource(new ClassPathResource("support/sample-data.csv"));
        reader.setLineMapper(new DefaultLineMapper<Person>()
        {{
                setLineTokenizer(new DelimitedLineTokenizer()
                {{
                        setNames(new String[]{"firstName", "lastName"});
                    }});
                setFieldSetMapper(new BeanWrapperFieldSetMapper<Person>()
                {{
                        setTargetType(Person.class);
                    }});
            }});
        return reader;
    }

    @Bean
    PersonItemProcessor itemProcess()
    {
        return new PersonItemProcessor();
    }

    @Bean
    ItemWriter<Person> itemWriter( DB db )
    {
        return new PersonWriter(db);
    }

    @Bean
    public Job personJob( JobBuilderFactory jobs, Step s1 )
    {
        return jobs.get("personJob")
                .incrementer(new RunIdIncrementer())
                .flow(s1)
                .end()
                .build();
    }

    @Bean
    public Step step1( StepBuilderFactory stepBuilderFactory, ItemReader<Person> reader,
                       ItemWriter<Person> writer, ItemProcessor<Person, Person> processor )
    {
        return stepBuilderFactory.get("step1")
                .<Person, Person>chunk(10)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .build();
    }

}
