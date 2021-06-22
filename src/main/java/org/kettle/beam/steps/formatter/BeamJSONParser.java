package org.kettle.beam.steps.formatter;

import com.google.cloud.Tuple;
import org.kettle.beam.core.util.Json;
import org.kettle.beam.core.util.Strings;
import org.kettle.beam.steps.bq.BQField;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleValueException;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.*;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;

import java.math.BigDecimal;
import java.sql.Types;
import java.util.*;

/**
 * Step para converter string em JSON
 * @author Renato Dornelas Cardoso
 */
public class BeamJSONParser extends BaseStep implements StepInterface {

    //region Constructors

    public BeamJSONParser(StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta, Trans trans ) {
        super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
    }

    //endregion


    //region Methods

    @Override
    public boolean processRow(StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {
        try {
            Object[] row = this.getRow();
            if (row == null) {
                this.setOutputDone();
                return false;
            }

            BeamJSONParserMeta meta = (BeamJSONParserMeta)smi;
            if(Strings.isNullOrEmpty(meta.getJsonField())){throw new KettleException("Campo Json nao informado.");}

            Map<String, JSONField> fields = new HashMap<>();
            for(JSONField field : meta.getFields()){
                fields.put(field.getName().toLowerCase(), field);
            }

            Map<String, Tuple<Object, Integer>> dataSet = this.getDateSet(row);
            if(row.length > 0) {
                if (!dataSet.containsKey(meta.getJsonField())) {
                    throw new KettleException("Campo Json '" + meta.getJsonField() + "' nao encontrado.");
                }
                String json = dataSet.get(meta.getJsonField()).x().toString();
                if(!Strings.isNullOrEmpty(json)) {
                    int jsonIndex = json.indexOf("{");
                    int arrayJsonIndex = json.indexOf("[");

                    if(jsonIndex >= 0 && arrayJsonIndex >= 0){
                        if(jsonIndex < arrayJsonIndex){
                            this.flushJsonObject(json.substring(jsonIndex), fields, meta);
                        }else{
                            this.flushJsonArray(json.substring(arrayJsonIndex), fields, meta);
                        }

                    }else if(jsonIndex >= 0){
                        this.flushJsonObject(json.substring(jsonIndex), fields, meta);

                    }else if(arrayJsonIndex >= 0){
                        this.flushJsonArray(json.substring(arrayJsonIndex), fields, meta);

                    }

                }
            }

            return true;

        }catch (KettleException ex){
            throw ex;

        }catch (Exception ex){
            this.log.logError("Beam Json Parser Error", ex);
            return false;
        }

    }

    private Map<String, Tuple<Object, Integer>> getDateSet(Object[] row) throws KettleValueException {
        Map<String, Tuple<Object, Integer>> dataSet = new HashMap<>();
        if(row == null){return dataSet;}
        String field;
        Tuple<Object, Integer> tuple;
        int i = 0;
        for(ValueMetaInterface valueMetaInterface : this.getInputRowMeta().getValueMetaList()){
            field = valueMetaInterface.getName().trim();
            tuple = null;
            switch (valueMetaInterface.getType()){
                case ValueMetaInterface.TYPE_STRING: tuple = Tuple.of(this.getInputRowMeta().getString(row, i), Types.VARCHAR) ; break;
                case ValueMetaInterface.TYPE_INTEGER: tuple = Tuple.of(this.getInputRowMeta().getInteger(row, i), Types.INTEGER); break;
                case ValueMetaInterface.TYPE_NUMBER: tuple = Tuple.of(this.getInputRowMeta().getNumber(row, i), Types.NUMERIC); break;
                case ValueMetaInterface.TYPE_BIGNUMBER: tuple = Tuple.of(this.getInputRowMeta().getBigNumber(row, i), Types.NUMERIC); break;
                case ValueMetaInterface.TYPE_BOOLEAN: tuple = Tuple.of(this.getInputRowMeta().getBoolean(row, i), Types.BOOLEAN); break;
                case ValueMetaInterface.TYPE_DATE: tuple = Tuple.of(this.getInputRowMeta().getDate(row, i), Types.DATE); break;
                case ValueMetaInterface.TYPE_TIMESTAMP: tuple = Tuple.of(this.getInputRowMeta().getDate(row, i), Types.TIMESTAMP); break;
                case ValueMetaInterface.TYPE_INET: tuple = Tuple.of(this.getInputRowMeta().getString(row, i), Types.DATALINK); break;
                case ValueMetaInterface.TYPE_NONE: tuple = Tuple.of(this.getInputRowMeta().getString(row, i), Types.BINARY); break;
                case ValueMetaInterface.TYPE_SERIALIZABLE: tuple = Tuple.of(this.getInputRowMeta().getString(row, i), Types.BINARY); break;
            }

            if(tuple != null) {
                dataSet.put(field, tuple);
            }
            i++;
        }
        return dataSet;
    }


    private void flushJsonObject(String json, Map<String, JSONField> fields, BeamJSONParserMeta meta) throws Exception {
        Map<String, Object> jsonObject = Json.getInstance().deserialize(json);
        this.flush(jsonObject, fields, meta);
    }

    private void flushJsonArray(String json, Map<String, JSONField> fields, BeamJSONParserMeta meta) throws Exception {
        List<Map<String, Object>> list = Json.getInstance().deserializeList(json);
        for(Map<String, Object> jsonObject : list){
            this.flush(jsonObject, fields, meta);
        }
    }


    private void flush(Map<String, Object> jsonObject, Map<String, JSONField> fields, BeamJSONParserMeta meta) throws KettleStepException {
        RowMeta rowMeta = new RowMeta();
        Map<String, RowValue> row = new HashMap<>();
        this.prepareRow(row,"", jsonObject, fields);
        RowValue rowValue;

        List<Object> values = new ArrayList<>();
        for(JSONField field : meta.getFields()){
            rowValue = row.get(field.getNewNameOrName());
            if(rowValue != null) {
                rowMeta.addValueMeta(rowValue.getMeta());
                values.add(rowValue.getData());
            }
        }

        this.putRow(rowMeta, values.toArray());
        if (isRowLevel()) {
            logRowlevel("Beam Json Parser", jsonObject);
        }
    }

    private void prepareRow(Map<String, RowValue> row, String prefix, Map<String, Object> jsonObject, Map<String, JSONField> fields){
        Object value;
        String attributeName;
        JSONField field;
        RowValue rowValue;
        ValueMetaInterface valueMeta;

        for(Map.Entry<String, Object> entry: jsonObject.entrySet()){
            value = entry.getValue();
            attributeName = prefix + entry.getKey().trim();
            field = fields.get(attributeName.toLowerCase());
            if(field != null) {
                if (value != null && value instanceof Map) {
                    this.prepareRow(row, attributeName + "_", (Map<String, Object>) value, fields);

                } else if (value != null && value instanceof List) {
                    List<Map<String, Object>> list = (List<Map<String, Object>>) value;
                    if(fields.containsKey(attributeName + "_count")){
                        valueMeta = new ValueMetaInteger(fields.get(attributeName + "_count").getNewNameOrName());
                        rowValue = new RowValue(valueMeta, Long.parseLong("" + list.size()));
                        row.put(field.getNewNameOrName(), rowValue);
                    }
                    int i = 0;
                    for (Map<String, Object> jsonObjectChild : list) {
                        this.prepareRow(row, attributeName + "_" + i + "_", (Map<String, Object>) jsonObjectChild, fields);
                        i++;
                    }

                } else {
                    valueMeta = this.createValueMeta(field);
                    if (value != null) {
                        if (value instanceof Byte
                                || value instanceof Short
                                || value instanceof Integer) {
                            value = Long.parseLong(value.toString());
                        }
                    }
                    row.put(field.getNewNameOrName(), new RowValue(valueMeta, value));
                }
            }
        }
    }

    private ValueMetaInterface createValueMeta(JSONField field){
        if(field.getKettleType().equalsIgnoreCase(BQField.BEAM_DATATYPE_BIG_NUMBER)){
            return new ValueMetaBigNumber(field.getNewNameOrName());

        } else if(field.getKettleType().equalsIgnoreCase(BQField.BEAM_DATATYPE_BINARY)) {
            return new ValueMetaBinary(field.getNewNameOrName());

        } else if(field.getKettleType().equalsIgnoreCase(BQField.BEAM_DATATYPE_BOOLEAN)) {
            return new ValueMetaBoolean(field.getNewNameOrName());

        } else if(field.getKettleType().equalsIgnoreCase(BQField.BEAM_DATATYPE_DATE)) {
            return new ValueMetaDate(field.getNewNameOrName());

        } else if(field.getKettleType().equalsIgnoreCase(BQField.BEAM_DATATYPE_INTEGER)) {
            return new ValueMetaInteger(field.getNewNameOrName());

        } else if(field.getKettleType().equalsIgnoreCase(BQField.BEAM_DATATYPE_INTERNET_ADDRESS)) {
            return new ValueMetaInternetAddress(field.getNewNameOrName());

        } else if(field.getKettleType().equalsIgnoreCase(BQField.BEAM_DATATYPE_NUMBER)) {
            return new ValueMetaNumber(field.getNewNameOrName());

        } else if(field.getKettleType().equalsIgnoreCase(BQField.BEAM_DATATYPE_STRING)) {
            return new ValueMetaString(field.getNewNameOrName());

        } else if(field.getKettleType().equalsIgnoreCase(BQField.BEAM_DATATYPE_TIMESTAMP)) {
            return new ValueMetaTimestamp(field.getNewNameOrName());

        } else{
            return new ValueMetaString(field.getNewNameOrName());
        }
    }

    private class RowValue{
        private ValueMetaInterface meta;
        private Object data;

        public ValueMetaInterface getMeta() {
            return meta;
        }
        public void setMeta(ValueMetaInterface meta) {
            this.meta = meta;
        }

        public Object getData() {
            return data;
        }
        public void setData(Object data) {
            this.data = data;
        }

        public RowValue(){}
        public RowValue(ValueMetaInterface meta, Object data){
            this.meta = meta;
            this.data = data;
        }
    }

    //endregion

}
