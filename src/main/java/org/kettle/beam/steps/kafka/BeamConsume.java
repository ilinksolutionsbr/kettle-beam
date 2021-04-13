package org.kettle.beam.steps.kafka;

import com.google.api.gax.batching.FlowControlSettings;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.pubsub.v1.PubsubMessage;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.kettle.beam.core.BeamDefaults;
import org.kettle.beam.steps.database.BeamDatabaseConnector;
import org.kettle.beam.steps.pubsub.BeamSubscribeData;
import org.kettle.beam.steps.pubsub.BeamSubscribeMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.value.ValueMetaSerializable;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class BeamConsume extends BaseStep implements StepInterface {

  private boolean started = false;
  private Thread thread;
  
  /**
   * This is the base step that forms that basis for all steps. You can derive from this class to implement your own
   * steps.
   *
   * @param stepMeta          The StepMeta object to run.
   * @param stepDataInterface the data object to store temporary data, database connections, caches, result sets,
   *                          hashtables etc.
   * @param copyNr            The copynumber for this step.
   * @param transMeta         The TransInfo of which the step stepMeta is part of.
   * @param trans             The (running) transformation to obtain information shared among the steps.
   */
  public BeamConsume( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  @Override
  public synchronized boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {
    if(this.started){return true;}
    boolean result = false;

    try {

      BeamConsumeMeta meta = (BeamConsumeMeta) smi;

      RowMeta outputRowMeta = new RowMeta();
      String keyField = this.getParentVariableSpace().environmentSubstitute(meta.getKeyField());
      String messageField = this.getParentVariableSpace().environmentSubstitute(meta.getMessageField());
      outputRowMeta.addValueMeta(new ValueMetaString(keyField));
      outputRowMeta.addValueMeta(new ValueMetaString(messageField));

      Consumer<String, String> consumer = this.createConsumer(meta);

      this.started = true;

      this.thread = new Thread(() -> {
        while (true) {
          final ConsumerRecords<String, String> records;
          records = consumer.poll(1);
          if (records.count() == 0) {
            continue;
          }
          this.log.logDebug( "Received: " + records.count());
          records.forEach(record -> {
            this.flush(outputRowMeta, record);
          });
          if(meta.isAllowingCommitOnConsumedOffset()) {
            consumer.commitSync();
          }
        }
      });
      this.thread.start();

      this.log.logDebug("Initialized");

      result = true;

    }catch (Exception ex){
      this.log.logError(BeamConsume.class.getName() + " -> " + ex.getMessage(), ex);
      this.setOutputDone();
      throw new KettleException(ex);

    }

    return result;
  }

  @Override
  public void dispose(StepMetaInterface smi, StepDataInterface sdi) {
    super.dispose(smi, sdi);
    try {
      if (this.thread != null) {
        this.log.logDebug("Stopped");
        this.thread.stop();
        this.thread = null;
        this.started = false;
      }
    }catch (Exception ex){}
  }

  private Consumer<String, String> createConsumer(BeamConsumeMeta meta) {
    final Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.getParentVariableSpace().environmentSubstitute(meta.getBootstrapServers()));
    props.put(ConsumerConfig.GROUP_ID_CONFIG, this.getParentVariableSpace().environmentSubstitute(meta.getGroupId()));
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

    if(meta.isRestrictedToCommitted()){
      props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    }else{
      props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    }

    if(meta.isAllowingCommitOnConsumedOffset()){
      props.put("enable.auto.commit", "true");
      props.put("auto.commit.interval.ms", "1000");
    }

    props.put("retries", 0);
    props.put("max.request.size", 512000);
    props.put("max.partition.fetch.bytes", 512000);
    props.put("max.poll.records", 1);

    for (ConfigOption configOption : meta.getConfigOptions()) {
      Object value;
      String optionValue = configOption.getValue();
      switch(configOption.getType()) {
        case String:value=optionValue; break;
        case Short: value=Short.valueOf( optionValue ); break;
        case Int: value = Integer.valueOf( optionValue ); break;
        case Long: value = Long.valueOf( optionValue ); break;
        case Double: value = Double.valueOf( optionValue ); break;
        case Boolean: value = Boolean.valueOf( optionValue ); break;
        default:
          throw new RuntimeException( "Config option parameter "+configOption.getParameter()+" uses unsupported type "+configOption.getType().name() );
      }
      props.put(configOption.getParameter(), value);
    }

    // Create the consumer using props.
    final Consumer<String, String> consumer = new KafkaConsumer<String, String>(props);

    // Subscribe to the topic.
    String topics = this.getParentVariableSpace().environmentSubstitute(meta.getTopics());
    consumer.subscribe(Collections.singletonList(topics));

    return consumer;
  }

  private void flush(RowMeta outputRowMeta, ConsumerRecord<String, String> record) {
    try{
      Object[] newRow = new Object[] { record.key(), record.value() };
      this.putRow(outputRowMeta, newRow);
      if (isRowLevel()) {
        logRowlevel("Kafka -> Consumer", outputRowMeta.getString(newRow));
      }
    }catch (Exception ex){
      this.log.logError("Kafka -> Consumer", ex);
    }
  }

}
