package org.kettle.beam.util;

import com.google.common.base.Strings;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class DatabaseUtil {

    public static ResultSetMetaData executeGetFieldsQuery(String sql, String driver, String connectionString, String username, String password) throws SQLException, ClassNotFoundException {

        try {
            Connection connection = null;
            PreparedStatement preparedStatement = null;

            java.util.List<String> parameters = new ArrayList<>();
            sql = prepareSQL(sql, parameters);

            Class.forName(driver);
            connection = DriverManager.getConnection(connectionString, username, password);
            preparedStatement = connection.prepareStatement(sql);
            preparedStatement.setMaxRows(1);

            //iNSERINDO PARÂMETROS NULOS CASO EXISTAM, APENAS PARA RECEBER OS METADADOS DO RESULTADO
            if(parameters.size() > 0) {
                for(int i = 0; i<parameters.size(); i++){
                    preparedStatement.setNull(i+1, Types.VARCHAR);
                }

            }

            ResultSet resultSet = preparedStatement.executeQuery();
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();

            preparedStatement.close();
            connection.close();

            return resultSetMetaData;
        } catch (SQLException sqlException) {
            throw new SQLException(sqlException);
        } catch (ClassNotFoundException ex) {
            throw new ClassNotFoundException(ex.getMessage());
        }
    }

    public static String prepareSQL(String sql, List<String> parameters){
        if(Strings.isNullOrEmpty(sql) || parameters == null){return sql;}
        sql = sql.replace("\n\r", " ").replace("\n", " ").replace("\r", " ").replace("\t", " ");
        int startIndex;
        int endIndex;
        String variable;
        String sqlPart1;
        String sqlPart2;
        while ((startIndex = sql.indexOf("${")) >= 0){
            endIndex = sql.indexOf("}");
            if(endIndex <= startIndex){break;}
            variable = sql.substring(startIndex, endIndex+1);
            sqlPart1 = sql.substring(0, startIndex);
            sqlPart2 = sql.substring(endIndex+1);
            sql = sqlPart1 + "?" + sqlPart2;
            parameters.add(variable.substring(2, variable.length()-1));
        }
        return sql;
    }
}
