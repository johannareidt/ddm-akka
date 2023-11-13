package de.ddm.helper;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvException;
import lombok.Getter;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;


public class CSVTable implements Serializable {

    private static final String TAG = "CSVTable";
    private final static Logger LOGGER =
            Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
    @Getter
    private final String filepath;
    @Getter
    private List<String[]> myEntries;
    @Getter
    private final HashMap<String, CSVColumn> columns;
    public CSVTable(String filepath, List<String[]>  myEntries){
        this.filepath = filepath;
        this.myEntries = myEntries;
        this.columns = new HashMap<>();
    }

    public String[] getColumnNames(){
        return myEntries.get(0);
    }

    public int getNumberOfColumns(){
        return getColumnNames().length;
    }

    public String getColumnName(int i){
        return getColumnNames()[i];
    }

    /*
    public int getColumnIndex(String columnName){
        String[] columns = getColumnNames();
        for(int i = 0; i <= columns.length; i++){
            if(Objects.equals(columns[i], columnName)){
                return i;
            }
        }
        return -1;
    }

     */


    public void split(){
        HashMap<Integer, List<String>> temp = new HashMap<>();
        int s = getNumberOfColumns();
        for(int i=0; i<s; i++){
            temp.put(i, new ArrayList<String>());
        }
        Iterator<String[]> iterator = this.myEntries.listIterator();
        iterator.next(); //ignore columnName row
        while (iterator.hasNext()){
            String[] row = iterator.next();
            for(int i=0; i<s; i++){
                temp.get(i).add(row[i]);
            }
        }
        for(int i=0; i<s; i++){
            this.columns.put(getColumnName(i),
                    new CSVColumn(this.filepath, getColumnName(i),temp.get(i).toArray(String[]::new)));
        }
        this.myEntries = null;
    }

    public CSVColumn getColumn(String columnName){
        return this.columns.get(columnName);
    }

    /*
    public CSVColumn getColumn(int columnIndex){
        return this.columns.get(getColumnName(columnIndex));
    }

     */

    /* input reader
    public static CSVTable readFile(String filepath){
        try {
            CSVReader reader = new CSVReaderBuilder(new FileReader("yourfile.csv")).build();
            CSVTable res = new CSVTable(filepath, reader.readAll());
            reader.close();
            return res;
        } catch (FileNotFoundException e) {
            LOGGER.log(Level.SEVERE, TAG+" readFile: file not found; ", e);
        } catch (CsvException e) {
            LOGGER.log(Level.SEVERE, TAG+" readFile: csv; ", e);
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, TAG+" readFile: io; ", e);
        }
        return null;
    }
     */

    public EmptyTable toEmpty(){
        return new EmptyTable(this.filepath, this.getColumnNames());
    }
}
