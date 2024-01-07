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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.impl.SimpleLog;


public class CSVTable implements Serializable {

    private static final Log log = new SimpleLog("CSVTable");
    @Getter
    private final String filepath;
    @Getter
    private List<String[]> myEntries;
    @Getter
    private final HashMap<String, CSVColumn> columns;
    @Getter
    private final String[] header;

    public CSVTable(String filepath, List<String[]>  myEntries, String[] header){
        this.filepath = filepath;
        this.myEntries = myEntries;
        this.columns = new HashMap<>();
        this.header = header;
    }

    public String[] getColumnNames(){
        if(header == null){
            return new String[]{};
        }
        if(header.length == 0){
            return new String[]{};
        }
        return header;
    }

    public int getNumberOfColumns(){
        return getColumnNames().length;
    }

    public String getColumnName(int i){
        if(i < this.getNumberOfColumns()) return getColumnNames()[i];
        log.info("overflow: column: access "+i+" ,max: "+this.getNumberOfColumns());
        return "Attr_"+i;
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
        int s = getNumberOfColumns()+5; //+5 als Überlaufschutz
        for(int i=0; i<s; i++){
            temp.put(i, new ArrayList<String>());
        }
        Iterator<String[]> iterator = this.myEntries.listIterator();
        iterator.next(); //ignore columnName row
        while (iterator.hasNext()){
            String[] row = iterator.next();
            for(int i=0; i<row.length; i++){
                temp.get(i).add(row[i]);
            }
        }
        for(int i=0; i<s; i++){
            this.columns.put(getColumnName(i),
                    new CSVColumn(this.filepath, getColumnName(i),temp.get(i)));
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
