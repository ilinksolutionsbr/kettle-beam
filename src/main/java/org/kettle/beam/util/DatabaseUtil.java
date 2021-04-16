package org.kettle.beam.util;

import com.google.common.base.Strings;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class DatabaseUtil {

    public static final String JDBC_POSTGRESQL_PREFIX = "jdbc:postgresql://";
    public static final String JDBC_SQL_SERVER_PREFIX = "jdbc:sqlserver://";
    public static final String JDBC_MYSQL_PREFIX = "jdbc:mysql://";
    public static final String JDBC_JTDS_PREFIX = "jdbc:jtds:sqlserver://";

    public static ResultSetMetaData executeGetFieldsQuery(String sql, String driver, String connectionString, String username, String password) throws SQLException, ClassNotFoundException {

        try {
            Connection connection = null;
            PreparedStatement preparedStatement = null;

            List<String> parameters = new ArrayList<>();
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

    public static String getConnectionUrl(String databaseType, String server, String port, String databaseName, String customUrl) throws Exception {

        if (!Strings.isNullOrEmpty(customUrl)) {
            return customUrl;
        } else {
            boolean fieldsMissing = Strings.isNullOrEmpty(server) || Strings.isNullOrEmpty(port) || Strings.isNullOrEmpty(databaseName);

            if(fieldsMissing) {
                throw new Exception("Os campos de servidor, porta e nome da base de dados devem estar preenchidos.");
            }

            StringBuilder connectionUrl = new StringBuilder();
            boolean isSqlServer = databaseType.equals("Microsoft SQL Server");

            if (databaseType.equals("PostgreSQL")) {
                connectionUrl.append(JDBC_POSTGRESQL_PREFIX);
            } else if (isSqlServer) {
                connectionUrl.append(JDBC_SQL_SERVER_PREFIX);
            } else if (databaseType.equals("MySQL")) {
                connectionUrl.append(JDBC_MYSQL_PREFIX);
            } else if (databaseType.equals("jTDS")) {
                connectionUrl.append(JDBC_JTDS_PREFIX);
            } else {
                throw new Exception("Tipo de banco de dados inválido.");
            }

            connectionUrl.append(server).append(":").append(port);

            if (isSqlServer) {
                connectionUrl.append(";databaseName=");
            } else {
                connectionUrl.append("/");
            }

            connectionUrl.append(databaseName);

            return connectionUrl.toString();
        }
    }
}
