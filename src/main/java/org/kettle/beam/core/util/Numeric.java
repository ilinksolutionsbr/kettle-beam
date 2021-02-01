package org.kettle.beam.core.util;

public class Numeric {

    public static boolean isNumeric(String value){
        return isInteger(value) || isDouble(value);
    }

    public static boolean isInteger(String value){
        if(Strings.isNullOrEmpty(value)){return false;}
        try{
            Integer.parseInt(value);
            return true;
        }catch (Exception ex){
            return false;
        }
    }

    public static boolean isDouble(String value){
        if(Strings.isNullOrEmpty(value)){return false;}
        try{
            Double.parseDouble(value);
            return true;
        }catch (Exception ex){
            return false;
        }
    }

}
