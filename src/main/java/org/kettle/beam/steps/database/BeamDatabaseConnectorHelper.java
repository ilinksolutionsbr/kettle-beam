package org.kettle.beam.steps.database;

import com.google.common.base.Strings;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.*;

import java.sql.Types;
import java.util.*;

public class BeamDatabaseConnectorHelper {

    //region Attributes

    private static BeamDatabaseConnectorHelper instance;
    private static Object lockInstance = new Object();

    public static String QUERY_TYPE_INSERT = "insert";
    public static String QUERY_TYPE_UPDATE = "update";
    public static String QUERY_TYPE_DELETE = "delete";
    public static String QUERY_TYPE_SELECT = "select";

    public static String BEAM_DATATYPE_BIG_NUMBER = "BigNumber";
    public static String BEAM_DATATYPE_BINARY = "Binary";
    public static String BEAM_DATATYPE_BOOLEAN = "Boolean";
    public static String BEAM_DATATYPE_DATE = "Date";
    public static String BEAM_DATATYPE_INTEGER = "Integer";
    public static String BEAM_DATATYPE_INTERNET_ADDRESS = "Internet Address";
    public static String BEAM_DATATYPE_NUMBER = "Number";
    public static String BEAM_DATATYPE_STRING = "String";
    public static String BEAM_DATATYPE_TIMESTAMP = "Timestamp";


    private Map<String, String> drivers;
    private Map<String, String> queryTypes;

    //endregion

    //region Getters Setters

    public Map<String, String> getDrivers(){
        if(this.drivers == null){this.drivers = new HashMap<>();}
        return this.drivers;
    }

    public Map<String, String> getQueryTypes(){
        if(this.queryTypes == null){this.queryTypes = new HashMap<>();}
        return this.queryTypes;
    }

    public String[] getDatabases(){
        return this.getDrivers().keySet().toArray(new String[0]);
    }
    public String getDriver(String databaseName){
        return this.getDrivers().get(databaseName);
    }

    public String[] getQueryTypeNames(){
        return this.getQueryTypes().keySet().toArray(new String[0]);
    }
    public String getQueryType(String type){
        return this.getQueryTypes().get(type);
    }

    //endregion

    //region Constructors

    private BeamDatabaseConnectorHelper(){this.initialize();}

    //endregion

    //region Methods

    public static BeamDatabaseConnectorHelper getInstance(){
        if(instance == null){
            synchronized (lockInstance){
                if(instance == null){instance = new BeamDatabaseConnectorHelper();}
            }
        }
        return instance;
    }

    private void initialize(){
        this.getDrivers().put("Microsoft SQL Server", "com.microsoft.sqlserver.jdbc.SQLServerDriver");
        this.getDrivers().put("MySQL", "com.mysql.jdbc.Driver");
        this.getDrivers().put("Postgre SQL", "org.postgresql.Driver");
        this.getDrivers().put("JTDS", "net.sourceforge.jtds.jdbc.Driver");

        this.getQueryTypes().put("Insert", QUERY_TYPE_INSERT);
        this.getQueryTypes().put("Update", QUERY_TYPE_UPDATE);
        this.getQueryTypes().put("Delete", QUERY_TYPE_DELETE);
        this.getQueryTypes().put("Select", QUERY_TYPE_SELECT);

    }

    public String prepareSQL(String sql, List<String> parameters){
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

    //endregion

}
