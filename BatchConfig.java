package com.mrps.extractorservice.config;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.BatchConfigurer;
import org.springframework.batch.core.configuration.annotation.DefaultBatchConfigurer;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.transaction.PlatformTransactionManager;

import com.mrps.extractorservice.batch.listener.DeleteStepListener;
import com.mrps.extractorservice.batch.listener.JobListener;
import com.mrps.extractorservice.batch.processor.DataItemProcessor;
import com.mrps.extractorservice.batch.processor.DeleteItemProcessor;
import com.mrps.extractorservice.batch.reader.DataReader;
import com.mrps.extractorservice.batch.reader.DeleteReader;
import com.mrps.extractorservice.batch.writer.DataWrite;
import com.mrps.extractorservice.batch.writer.DeleteWriter;

// import com.mrps.extractorservice.batch.writer.FinalEntityWriter;
import lombok.extern.slf4j.Slf4j;

/**
 * Application batch config
 */
@Configuration
@EnableBatchProcessing
@Slf4j
public class BatchConfig {

  @Autowired
  public JobBuilderFactory jobBuilderFactory;

  @Autowired
  public StepBuilderFactory stepBuilderFactory;

  @Qualifier("secondaryDB")
  DataSource ds;

  @Autowired
  DataReader dataReader;

  @Autowired
  DataItemProcessor dataItemProcessor;

  @Autowired
  private DeleteItemProcessor deleteItemProcessor;

  @Autowired
  private DeleteReader deleteReader;

  @Autowired
  private DeleteWriter deleteWriter;

  @Autowired
  DataWrite dataWrite;
  // @Autowired
  // FinalEntityWriter finalEntityWriter;

  @Bean("batchRepo")
  @DependsOn({"secondaryTx", "secondaryDB"})
  public JobRepository jobRepository(@Qualifier("secondaryDB") DataSource ds,
      @Qualifier("secondaryTx") PlatformTransactionManager transactionManager) throws Exception {

    JobRepositoryFactoryBean factory = new JobRepositoryFactoryBean();
    factory.setDataSource(ds);
    factory.setTransactionManager(transactionManager);
    /*
     * factory.setIsolationLevelForCreate("ISOLATION_SERIALIZABLE");
     * factory.setTablePrefix("BATCH_");
     */
    factory.setMaxVarCharLength(1000);
    return factory.getObject();
  }

  @Bean("batchLauncher")
  @DependsOn("batchRepo")
  public SimpleJobLauncher jobLauncher(@Qualifier("batchRepo") JobRepository jobRepository) {

    SimpleJobLauncher launcher = new SimpleJobLauncher();
    launcher.setJobRepository(jobRepository);
    return launcher;
  }

  @Bean
  public Job exampleJob(@Qualifier("stagingStep") Step step, JobBuilderFactory jobBuilderFactory,
      @Qualifier("batchRepo") JobRepository jobRepository) {

    return jobBuilderFactory.get("step1")
        // repository(jobRepository)
        .listener(createListener())
        // .preventRestart()
        .incrementer(new RunIdIncrementer())
        .flow(step)
        // .next(finalEntityStep())
        // .next(finalDeleteStep(jobRepository))
        .end()
        .build();
  }
  // @Bean("finalEntityStep")
  // public Step finalEntityStep() {
  // Step step = stepBuilderFactory.get("finalEntityStep")
  // .chunk(1)
  // .writer(finalEntityWriter)
  // .build();
  //
  // return step;
  // }

  private Step finalDeleteStep(@Qualifier("batchRepo") JobRepository jobRepository) {

    Step step = stepBuilderFactory.get("step2")
        .repository(jobRepository)
        .listener(new DeleteStepListener(deleteReader, deleteWriter))
        .chunk(1)
        .reader(deleteReader)
        .processor(deleteItemProcessor)
        .writer(deleteWriter)
        .build();
    return step;
  }

  @Bean
  public BatchConfigurer batchConfigurer(@Qualifier("secondaryDB") DataSource ds,
      @Qualifier("secondaryTx") PlatformTransactionManager transactionManager) {

    return new DefaultBatchConfigurer(ds) {

      @Override
      public PlatformTransactionManager getTransactionManager() {

        return transactionManager;
      }
    };
  }

  /**
   * config the step
   *
   * @return step
   */
  @Bean("stagingStep")
  public Step step1(@Qualifier("batchRepo") JobRepository jobRepository) {

    Step step = stepBuilderFactory.get("step1")
        .repository(jobRepository)
        .chunk(1)
        .reader(dataReader)
        .processor(dataItemProcessor)
        .writer(dataWrite)
        .build();
    return step;
  }

  private JobExecutionListener createListener() {

    return new JobListener(dataReader, dataWrite);
  }
}
