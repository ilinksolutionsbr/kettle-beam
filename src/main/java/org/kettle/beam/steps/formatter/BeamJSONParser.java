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
                            this.flushJsonObject(json.substring(jsonIndex));
                        }else{
                            this.flushJsonArray(json.substring(arrayJsonIndex));
                        }

                    }else if(jsonIndex >= 0){
                        this.flushJsonObject(json.substring(jsonIndex));

                    }else if(arrayJsonIndex >= 0){
                        this.flushJsonArray(json.substring(arrayJsonIndex));

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


    private void flushJsonObject(String json) throws Exception {
        Map<String, Object> jsonObject = Json.getInstance().deserialize(json);
        this.flush(jsonObject);
    }

    private void flushJsonArray(String json) throws Exception {
        List<Map<String, Object>> list = Json.getInstance().deserializeList(json);
        for(Map<String, Object> jsonObject : list){
            this.flush(jsonObject);
        }
    }


    private void flush(Map<String, Object> jsonObject) throws KettleStepException {
        RowMeta rowMeta = new RowMeta();
        List<Object> row = new ArrayList<>();
        this.prepareRow(row,rowMeta,"", jsonObject);
        this.putRow(rowMeta, row.toArray());
        if (isRowLevel()) {
            logRowlevel("Beam Json Parser", jsonObject);
        }
    }

    private void prepareRow(List<Object> row, RowMeta rowMeta, String prefix, Map<String, Object> jsonObject){
        Object value;
        String attributeName;
        for(Map.Entry<String, Object> entry: jsonObject.entrySet()){
            value = entry.getValue();
            attributeName = prefix + entry.getKey();
            if(value != null && value instanceof Map) {
                this.prepareRow(row, rowMeta, attributeName + "_", (Map<String, Object>) value);

            }else if(value != null && value instanceof List) {
                List<Map<String, Object>> list = (List<Map<String, Object>>)value;
                rowMeta.addValueMeta(new ValueMetaInteger(attributeName + "_count"));
                row.add(Long.parseLong("" + list.size()));
                int i = 0;
                for(Map<String, Object> jsonObjectChild : list){
                    this.prepareRow(row, rowMeta, attributeName + "_" + i + "_", (Map<String, Object>)jsonObjectChild);
                    i++;
                }

            }else{
                rowMeta.addValueMeta(this.createValueMeta(attributeName, entry.getValue()));
                if(value != null) {
                    if (value instanceof Byte
                            || value instanceof Short
                            || value instanceof Integer) {
                        value = Long.parseLong(value.toString());
                    } else if (value instanceof Calendar) {
                        value = ((Calendar) value).getTime().getTime();
                    }
                }
                row.add(value);
            }
        }
    }

    private ValueMetaInterface createValueMeta(String name, Object value){
        if(value == null){
            return new ValueMetaString(name);

        }else if(value instanceof java.sql.Date
                || value instanceof java.util.Date){
            return new ValueMetaDate(name);

        }else if(value instanceof Calendar) {
            return new ValueMetaTimestamp(name);

        }else if(value instanceof Byte
                || value instanceof Short
                || value instanceof Integer) {
            return new ValueMetaInteger(name);

        }else if(value instanceof Long) {
            return new ValueMetaNumber(name);

        }else if(value instanceof BigDecimal
                || value instanceof Double
                || value instanceof Float) {
            return new ValueMetaBigNumber(name);

        }else if(value instanceof Boolean) {
            return new ValueMetaBoolean(name);

        }else{
            return new ValueMetaString(name);

        }

    }

    //endregion

}
