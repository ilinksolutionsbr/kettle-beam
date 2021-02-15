package org.kettle.beam.metastore;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import org.kettle.beam.util.DateFormat;

public class FileGetFields {

    public FileGetFields() {
    }

    public void process(String path, String delimiter){
        try {
            verificaArquivo(path, delimiter);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void verificaArquivo(String filePath, String delimiter) throws Exception {

        //Receber um arquivo
        File file = new File(filePath);
        if(file.exists()){
            if(file.length() == 0){
                Exception ex = new Exception("O arquivo informado está vazio");
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

                    //TODO REMOVER
                    for(int j = 0; j<dataTypes.size(); j++){
                        System.out.println(columns.get(j) + " " + dataTypes.get(j));
                    }
                    //TODO FIM
                } else {
                    Exception ex = new Exception("A quantidade de colunas/itens do cabeçalho não é igual a quantidade de colunas da segunda linha do arquivo.");
                    throw ex;
                }
            }
        } else {
            Exception ex = new Exception("O arquivo do caminho informado não existe. Insira o caminho absoluto do arquivo incluindo sua extensão.");
            throw ex;
        }

        //Return 'columns' to header names
        //Return 'dataTypes' to data types of each column
    }

    public String typeIdentifier(String itemToIdentify){

        String type = "String";

        //Identify Boolean Type
        if(itemToIdentify.equalsIgnoreCase("true") || itemToIdentify.equalsIgnoreCase("false")){
            type = "Boolean";

            return type;
        }

        //Identify Boolean Type
        if(itemToIdentify.equalsIgnoreCase("1") || itemToIdentify.equalsIgnoreCase("0")){
            type = "Binary";

            return type;
        }

        //Convert to int
        try {
            Integer.parseInt(itemToIdentify);
            type = "Integer";
            return type;
        } catch(Exception e) { //Nothing to do
        }

        //Convert to double
        try {
            Double.parseDouble(itemToIdentify);
            type = "Number";
            return type;
        } catch(Exception e) { //Nothing to do
        }

        //Convert into timestamp
        for(String a : DateFormat.timestampTypes){
            try {
                Date date = new SimpleDateFormat(a).parse(itemToIdentify);
                type = "Timestamp";
                return type;
            } catch (Exception e){
                //Nothing to do
            }
        }

        //Convert into date
        for(String a : DateFormat.datatypes){
            try {
                Date date = new SimpleDateFormat(a).parse(itemToIdentify);
                type = "Date";
                return type;
            } catch (Exception e){
                //Nothing to do
            }
        }
        return type;
    }

}