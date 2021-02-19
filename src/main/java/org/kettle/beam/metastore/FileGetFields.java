package org.kettle.beam.metastore;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.text.SimpleDateFormat;
import java.util.*;

import org.kettle.beam.util.BeamConst;
import org.kettle.beam.util.DateFormat;
import org.pentaho.di.core.exception.KettleAuthException;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.i18n.BaseMessages;

/**
 * @Autor Igor Brandorff - Callink
 */
public class FileGetFields {

    private static Class<?> PKG = FileDefinitionDialog.class; // for i18n purposes, needed by Translator2!!

    public FileGetFields() {
    }

    public List<FieldsMetadata> process(String path, String delimiter) throws Exception {

        List<FieldsMetadata> metadata = new ArrayList();

        if(delimiter.isEmpty()){
        throw new Exception("O campo " + BaseMessages.getString(PKG,"FileDefinitionDialog.Separator.Label") + " está vazio. " +
                "Preencha para Obter os campos.");
        } else {
            if(path.isEmpty()){
                throw new Exception("O campo " + BaseMessages.getString(PKG,"FileDefinitionDialog.FilePath.Label") + " está vazio. " +
                        "Preencha para Obter os campos.");
            } else {
                metadata = verificaArquivo(path, delimiter);
            }
        }

        return metadata;
    }

    public List<FieldsMetadata> verificaArquivo(String filePath, String delimiter) throws Exception {

        List<FieldsMetadata> metadata = new ArrayList();

        //Receber um arquivo
        File file = new File(filePath);
        if(file.exists()){
            if(file.length() == 0){
                Exception ex = new KettleException("O arquivo informado está vazio");
                throw ex;
            } else {
                BufferedReader reader = new BufferedReader(new FileReader(filePath));

                //Reading the file header to correctly map the fields;
                String header = reader.readLine();
                List<String> columns = Arrays.asList(header.split(delimiter));

                //Readig next line to map data types;
                String firstSample = reader.readLine();
                if(firstSample == null){
                    //If line is null the file has only the header so we'll use the header to try to define column types
                    firstSample = header;
                }
                List<String> dataTypes = Arrays.asList(firstSample.split(delimiter));
                if(dataTypes.size() == columns.size()){
                    for(int i=0; i<dataTypes.size(); i++){
                        dataTypes.set(i, typeIdentifier(dataTypes.get(i)));
                    }

                    //Criando pares de valores
                    for(int i=0; i<columns.size(); i++){
                        FieldsMetadata meta = new FieldsMetadata(columns.get(i), dataTypes.get(i));
                        metadata.add(meta);
                    }

                } else {
                    Exception ex = new KettleException("" +
                            "A quantidade de colunas/itens do cabeçalho é divergente da quantidade de colunas encontradas nos dados. " +
                            "Verifique se o arquivo possui a mesma quantidade de colunas no cabeçalho e na primeira linha de dados.");
                    throw ex;
                }
            }
        } else {
            Exception ex = new KettleAuthException("O arquivo informado não existe. Insira o caminho absoluto do arquivo incluindo sua extensão no campo");
            throw ex;
        }
        //Return 'columns' to header names
        //Return 'types' to data types of each column
        return metadata;
    }

    public String typeIdentifier(String itemToIdentify){

        String type = BeamConst.FILE_GET_FIELDS_TYPE_STRING;

        //Identify Boolean Type
        if(itemToIdentify.equalsIgnoreCase("true") || itemToIdentify.equalsIgnoreCase("false")){
            type = BeamConst.FILE_GET_FIELDS_TYPE_BOOLEAN;

            return type;
        }

        //Identify Boolean Type
        if(itemToIdentify.equalsIgnoreCase("1") || itemToIdentify.equalsIgnoreCase("0")){
            type = BeamConst.FILE_GET_FIELDS_TYPE_BINARY;

            return type;
        }

        //Convert to int
        try {
            Integer.parseInt(itemToIdentify);
            type = BeamConst.FILE_GET_FIELDS_TYPE_INTEGER;
            return type;
        } catch(Exception e) { //Nothing to do
        }

        //Convert to double
        try {
            Double.parseDouble(itemToIdentify);
            type = BeamConst.FILE_GET_FIELDS_TYPE_NUMBER;
            return type;
        } catch(Exception e) { //Nothing to do
        }

        //Convert into timestamp
        for(String a : DateFormat.timestampTypes){
            try {
                Date date = new SimpleDateFormat(a).parse(itemToIdentify);
                type = BeamConst.FILE_GET_FIELDS_TYPE_TIMESTAMP;
                return type;
            } catch (Exception e){
                //Nothing to do
            }
        }

        //Convert into date
        for(String a : DateFormat.datatypes){
            try {
                Date date = new SimpleDateFormat(a).parse(itemToIdentify);
                type = BeamConst.FILE_GET_FIELDS_TYPE_DATE;
                return type;
            } catch (Exception e){
                //Nothing to do
            }
        }
        return type;
    }
}