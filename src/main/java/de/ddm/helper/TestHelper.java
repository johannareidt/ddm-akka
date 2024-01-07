package de.ddm.helper;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import de.ddm.singletons.DomainConfigurationSingleton;
import de.ddm.singletons.InputConfigurationSingleton;
import de.ddm.structures.InclusionDependency;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;

public class TestHelper {
    static final int batchSize = DomainConfigurationSingleton.get().getInputReaderBatchSize();

    public static List<File> getFiles(){

        File folder = new File("C:\\Users\\johan\\Documents\\GitHub\\ddm-akka\\data\\TPCH");
        return List.of(Objects.requireNonNull(folder.listFiles()));
    }

    public static CSVReader getReader(File inputFile) throws IOException, CsvValidationException {
        CSVReader reader = InputConfigurationSingleton.get().createCSVReader(inputFile);
        //String[] header = InputConfigurationSingleton.get().getHeader(inputFile);

        if (InputConfigurationSingleton.get().isFileHasHeader())
            reader.readNext();
        return reader;
    }


    public static List<CSVReader> getReaders(List<File> files) throws CsvValidationException, IOException {
        List<CSVReader> readers = new ArrayList<>();
        for (File f: files){
            //System.out.println(Arrays.toString(getHeader(f)));
            readers.add(TestHelper.getReader(f));
        }
        return readers;
    }

    public static String[] getHeader(File inputFile) throws IOException, CsvValidationException {
        return InputConfigurationSingleton.get().getHeader(inputFile);
    }

    public static List<String[]> getHeaders(List<File> files) {
        List<String[]> headers = new ArrayList<>();
        for(File f: files){
            try {
                headers.add(getHeader(f));
            } catch (IOException | CsvValidationException e) {
                throw new RuntimeException(e);
            }
        }
        return headers;
    }

    public static List<String[]> readBatch(CSVReader reader) throws CsvValidationException, IOException {
        List<String[]> batch = new ArrayList<String[]>(batchSize);
        for (int i = 0; i < batchSize; i++) {
            String[] line = reader.readNext();
            if (line == null)
                break;
            batch.add(line);
        }
        //System.out.println(batch);
        return batch;
    }

    public static List<CSVTable> readBatches(String filepath, CSVReader reader) throws CsvValidationException, IOException {
        //List<List<String[]>> res = new ArrayList<>();
        List<CSVTable> tables = new ArrayList<CSVTable>();
        List<String[]> last = new ArrayList<String[]>();

        String[] header = getHeader(new File(filepath));

        //System.out.println(filepath + Arrays.toString(header));
        last = readBatch(reader);
        while (last.size() != 0) {
            //res.add(last);
            //Create Table Task
            tables.add(new CSVTable(filepath, last, header));
            tables.get(tables.size() - 1).split();
            last = readBatch(reader);
        }
        return tables;
        //Read Batch message
        /*
        if (message.getBatch().size() != 0) {
            this.inputReaders.get(message.getId()).tell(new InputReader.ReadBatchMessage(this.getContext().getSelf()));

            this.tasks.add(new DependencyWorker.CreateTableTask(
                    inputFiles[message.id].getPath(),
                    message.batch));
        }else{
            System.out.println("Batch zero");
        }

         */
    }

    public static HashMap<String, List<CSVTable>> read(List<File> files) throws CsvValidationException, IOException {
        HashMap<String, List<CSVTable>> tables = new HashMap<String, List<CSVTable>>();
        for (File file : files) {
            CSVReader reader = getReader(file);
            tables.put(file.getPath(), readBatches(file.getPath(), reader));
            //read ReadBatchMessage from input reader
        }
        return tables;
    }

    public static void loadColumns(HashMap<String, List<CSVTable>> tables) {
        HelperMain.columns.clear();
        for (String path : tables.keySet()) {
            HelperMain.columns.put(path, new ArrayList<CSVColumn>());
            HashMap<String, List<CSVColumn>> tempTableColumns = new HashMap<String, List<CSVColumn>>();
            for (CSVTable t : tables.get(path)) {
                for (String cn : t.getColumnNames()) {
                    if (!tempTableColumns.containsKey(cn)) {
                        tempTableColumns.put(cn, new ArrayList<CSVColumn>());
                    }
                    tempTableColumns.get(cn).add(t.getColumn(cn));
                }
            }
            for (String cn : tempTableColumns.keySet()) {
                ArrayList<String> entries = new ArrayList<String>();
                for (CSVColumn c : tempTableColumns.get(cn)) {
                    entries.addAll(c.getEntries());
                }
                HelperMain.columns.get(path).add(new CSVColumn(path, cn, entries));
            }


        }
    }

    public static List<String> getResults() throws IOException {
        String resultspath = "C:\\Users\\johan\\Documents\\GitHub\\ddm-akka\\data\\results.txt";
        // Open the file
        FileInputStream fstream = new FileInputStream(resultspath);
        BufferedReader br = new BufferedReader(new InputStreamReader(fstream));

        List<String> lines = new ArrayList<String>();
        String line;

        //Read File Line By Line
        while ((line = br.readLine()) != null) {
            // Print the content on the console - do what you want to do
            lines.add(line);
        }

        //Close the input stream
        fstream.close();
        return lines;
    }

    public static void compareWithSolution(List<InclusionDependency> results){

        List<String> solution = new ArrayList<>();
        try {
            solution = TestHelper.getResults();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        List<InclusionDependency> match = new ArrayList<>();
        List<InclusionDependency> falseMatch = new ArrayList<>();
        List<String> shouldBeMatch = new ArrayList<>();

        List<String> contained = new ArrayList<>();


        for(InclusionDependency id: results){
            if(solution.contains(id.toString())){
                match.add(id);
                contained.add(id.toString());
            }else{
                falseMatch.add(id);
            }
        }

        for(String s: solution){
            if(!contained.contains(s)) shouldBeMatch.add(s);
        }




        System.out.println("match: length: "+match.size());
        for(InclusionDependency p: match) {
            System.out.println("match:  " + p);
            //analyzeTask( p, "match: ");

        }

        //System.out.println("correctNotMatch: length: "+correctNotMatch.size());


        System.out.println("falseMatch: length: "+falseMatch.size());
        for(InclusionDependency p: falseMatch) {
            System.out.println("falseMatch:  " + p);
            //analyzeTask( p, "falseMatch: ");
        }
        System.out.println("shouldBeMatch: length: "+shouldBeMatch.size());
        for(String p: shouldBeMatch) {
            System.out.println("shouldBeMatch:  " + p);
            //analyzeTask(p, "shouldBeMatch: ");
        }

    }


}