package org.kettle.beam.core.fn;

import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Value;
import org.apache.avro.util.Utf8;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.kettle.beam.core.BeamKettle;
import org.kettle.beam.core.KettleRow;
import org.kettle.beam.core.util.Json;
import org.kettle.beam.core.util.JsonRowMeta;
import org.kettle.beam.core.util.Strings;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;

public class FirestoreEntityToKettleRowFn extends DoFn<Entity, KettleRow> {

    public static final String PARTITION_DATE_FORMAT = "yyyy-MM-dd";
    public static final String PARTITION_HOUR_FORMAT = "yyyy-MM-dd'T'HH:mm:ss";

    private String stepname;
    private String projectId;
    private String kind;
    private String rowMetaJson;
    private List<String> stepPluginClasses;
    private List<String> xpPluginClasses;

    private transient Counter initCounter;
    private transient Counter inputCounter;
    private transient Counter writtenCounter;
    private transient Counter errorCounter;

    // Log and count parse errors.
    private static final Logger LOG = LoggerFactory.getLogger( FirestoreEntityToKettleRowFn.class );
    private final Counter numErrors = Metrics.counter("main", "BeamFirestoreInputErrors");

    private transient RowMetaInterface rowMeta;
    private transient SimpleDateFormat simpleDateTimeFormat;
    private transient SimpleDateFormat simpleDateFormat;

    public FirestoreEntityToKettleRowFn(String stepname, String projectId, String kind, String rowMetaJson, List<String> stepPluginClasses, List<String> xpPluginClasses) {
        this.stepname = stepname;
        this.projectId = projectId;
        this.kind = kind;
        this.rowMetaJson = rowMetaJson;
        this.stepPluginClasses = stepPluginClasses;
        this.xpPluginClasses = xpPluginClasses;
    }

    @Setup
    public void setUp() {
        try {
            inputCounter = Metrics.counter("input", stepname);
            writtenCounter = Metrics.counter("written", stepname);

            // Initialize Kettle Beam
            //
            BeamKettle.init(stepPluginClasses, xpPluginClasses);
            this.rowMeta = JsonRowMeta.fromJson(rowMetaJson);

            Metrics.counter("init", stepname).inc();
        } catch (Exception e) {
            numErrors.inc();
            LOG.error("Error in setup of KettleRow to Entity function", e);
            throw new RuntimeException("Error in setup of KettleRow to Entity function", e);
        }
    }

    @ProcessElement
    public void processElement(ProcessContext processContext) {
        try {
            Entity entity = processContext.element();
            inputCounter.inc();

            if(entity != null) {
                if(rowMeta.size() == 0){return;}
                Object[] newRow = new Object[rowMeta.size()];
                int index = 0;

                Map<String, Value> properties = entity.getPropertiesMap();

                for(Map.Entry<String, Value> entry: properties.entrySet()) {
                    ValueMetaInterface valueMeta = rowMeta.getValueMeta(index);
                    Value value = entry.getValue();

                    if (value != null) {
                        switch(valueMeta.getType()) {
                            case ValueMetaInterface.TYPE_STRING:
                                newRow[index] = value.getStringValue();
                                break;
                            case ValueMetaInterface.TYPE_INTEGER:
                                newRow[index] = value.getIntegerValue();
                                break;
                            case ValueMetaInterface.TYPE_NUMBER:
                                newRow[index] = value.getDoubleValue();
                                break;
                            case ValueMetaInterface.TYPE_BOOLEAN:
                                newRow[index] = value.getBooleanValue();
                                break;
                            case ValueMetaInterface.TYPE_NONE:
                                newRow[index] = value.getNullValue();
                                break;
                            case ValueMetaInterface.TYPE_DATE:
                                // We get a Long back
                                //
                                String stringValue = value.getStringValue();
                                if(stringValue.length() > 10) {
                                    SimpleDateFormat formatter = new SimpleDateFormat(PARTITION_HOUR_FORMAT);
                                    newRow[index] = formatter.parse(stringValue);
                                } else {
                                    SimpleDateFormat formatter = new SimpleDateFormat(PARTITION_DATE_FORMAT);
                                    newRow[index] = formatter.parse(stringValue);
                                }
                                break;
                            default:
                                throw new RuntimeException("Conversion from Avro JSON to Kettle is not yet supported for Kettle data type '"+valueMeta.getTypeDesc()+"'");
                        }
                    }

                    index++;
                }

                processContext.output( new KettleRow( newRow ) );
                writtenCounter.inc();
            } else {
                throw new Exception("A returned entity is null");
            }
        } catch (Exception e) {
            numErrors.inc();
            LOG.error("Error in Entity to KettleRow function -> step '" + this.stepname + "'", e);
            throw new RuntimeException("Error in Entity to KettleRow function", e);
        }
    }

    //  Based on:
    //         https://cloud.google.com/dataprep/docs/html/BigQuery-Data-Type-Conversions_102563896
    //         https://cloud.google.com/datastore/docs/concepts/metadataqueries
    //
    public enum AvroType {
        STRING(ValueMetaInterface.TYPE_STRING),
        BYTES(ValueMetaInterface.TYPE_STRING),
        INTEGER(ValueMetaInterface.TYPE_INTEGER),
        INT64(ValueMetaInterface.TYPE_INTEGER),
        FLOAT(ValueMetaInterface.TYPE_NUMBER),
        FLOAT64(ValueMetaInterface.TYPE_NUMBER),
        BOOLEAN(ValueMetaInterface.TYPE_BOOLEAN),
        BOOL(ValueMetaInterface.TYPE_BOOLEAN),
        TIMESTAMP(ValueMetaInterface.TYPE_DATE),
        DATE(ValueMetaInterface.TYPE_DATE),
        TIME(ValueMetaInterface.TYPE_DATE),
        DATETIME(ValueMetaInterface.TYPE_DATE),
        NULL(ValueMetaInterface.TYPE_NONE),
        DOUBLE(ValueMetaInterface.TYPE_NUMBER),
        REFERENCE(ValueMetaInterface.TYPE_STRING),
        POINT(ValueMetaInterface.TYPE_STRING),
        LONG(ValueMetaInterface.TYPE_INTEGER)
        ;

        private int kettleType;

        private AvroType(int kettleType) {
            this.kettleType = kettleType;
        }

        /**
         * Gets kettleType
         *
         * @return value of kettleType
         */
        public int getKettleType() {
            return kettleType;
        }
    }
}
