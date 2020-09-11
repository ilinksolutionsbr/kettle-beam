package org.kettle.beam.steps.kafka;

import com.google.cloud.Tuple;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.kettle.beam.core.util.Strings;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

import java.sql.Types;
import java.util.Map;
import java.util.Properties;

public class BeamProduce extends BaseStep implements StepInterface {

  private Producer<String, String> producer;
  private int count = 0;


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
  public BeamProduce( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  @Override
  public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {
    boolean result = false;

    try {
      Object[] row = this.getRow();
      if(row == null){
        this.setOutputDone();
        return false;
      }

      BeamProduceMeta meta = (BeamProduceMeta) smi;

      Producer<String, String> producer = this.createProducer(meta);
      String topics = this.getParentVariableSpace().environmentSubstitute(meta.getTopic());
      String keyField = this.getParentVariableSpace().environmentSubstitute(meta.getKeyField());
      String messageField = this.getParentVariableSpace().environmentSubstitute(meta.getMessageField());
      String messageValue = null;
      String fieldName;
      int i = 0;

      for(ValueMetaInterface valueMetaInterface : this.getInputRowMeta().getValueMetaList()) {
        fieldName = valueMetaInterface.getName().trim();
        if (fieldName.equalsIgnoreCase(messageField)) {
          messageValue = this.getInputRowMeta().getString(row, i);
          break;
        }
        i++;
      }

      if(!Strings.isNullOrEmpty(messageValue)) {
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topics, messageValue);
        RecordMetadata metadata = producer.send(record).get();
        this.count++;
        if(this.count >= 1000){
          this.producer.flush();
          this.count = 0;
        }
        this.log.logDebug("Record sent with key " + record.key() + " to partition " + metadata.partition() + " with offset " + metadata.offset());

        RowMeta outputRowMeta = new RowMeta();
        outputRowMeta.addValueMeta(new ValueMetaString(keyField));
        outputRowMeta.addValueMeta(new ValueMetaString(messageField));
        Object[] rowOutput = new Object[]{record.key(), messageValue};
        this.putRow(outputRowMeta, rowOutput);

      }

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
      this.producer.flush();
      this.producer.close();
      this.producer = null;
    }catch (Exception ex){}
  }

  public Producer<String, String> createProducer(BeamProduceMeta meta) {
    if(this.producer != null){return this.producer;}
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.getParentVariableSpace().environmentSubstitute(meta.getBootstrapServers()));
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "kettle");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    this.producer = new KafkaProducer<>(props);
    this.log.logDebug("Initialized");
    return this.producer;
  }

}
