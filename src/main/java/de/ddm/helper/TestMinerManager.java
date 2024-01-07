package de.ddm.helper;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class TestMinerManager {
    public static void main(String[] args) {
        MinerManager minerManager = new MinerManager();
        List<File> files = TestHelper.getFiles();
        //HelperMain.getReader();
        try {
            List<CSVReader> readers = TestHelper.getReaders(files);
        } catch (CsvValidationException | IOException e) {
            throw new RuntimeException(e);
        }
        List<String[]> header = TestHelper.getHeaders(files);
        
    }
}
