package cis5550.generic;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

import cis5550.kvs.Row;
import cis5550.tools.*;
import cis5550.webserver.Server;

public class Worker {
    private static final Logger logger = Logger.getLogger(Server.class);
    private static final String TABLE_TEMPLATE = "<!DOCTYPE html>\n" +
            "<html>\n" +
            "<head>\n" +
            "    <title>CIS5550 HW5</title>\n" +
            "</head>\n" +
            "<body>\n" +
            "\t<h1>{{tableName}}</h1>\n" +
            "    <table border=\"1\">\n" +
            "        {{tableData}}\n" +
            "    </table>\n" +
            "</body>\n" +
            "</html>";
    private static final String HOST = "localhost";
    protected static Map<String, Map<String, List<Row>>> database = new ConcurrentHashMap<>();

    public static class HashUtil {
        public static String generateSHA256(byte[] data) {
            try {
                MessageDigest digest = MessageDigest.getInstance("SHA-256");
                byte[] hashBytes = digest.digest(data);
                return bytesToHex(hashBytes);
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException("Failed to generate hash", e);
            }
        }

        private static String bytesToHex(byte[] bytes) {
            StringBuilder hexString = new StringBuilder(2 * bytes.length);
            for (byte b : bytes) {
                String hex = Integer.toHexString(0xff & b);
                if (hex.length() == 1) {
                    hexString.append('0');
                }
                hexString.append(hex);
            }
            return hexString.toString();
        }
    }

    public static class DynamicHTMLGenerator {

        public static String generateHTMLForTablesList(TreeMap<String, Long> data, int workerPortNo) {
            StringBuilder tableData = new StringBuilder();

            // Add table headers
            if (!data.isEmpty()) {
                tableData.append("<tr>");
                for (String key : data.keySet()) {
                    tableData.append("<th><a href=" + generateTableHyperLink(key, workerPortNo) + ">").append(key).append("</th>");
                }
                tableData.append("</tr>");
            }

            tableData.append("<tr>");
            // Add table data
            for (Long count : data.values()) {
                tableData.append("<td>").append(count).append("</td>");
            }
            tableData.append("</tr>");

            // Load the HTML template from "tableTemplate.html"
            //	            System.out.println(htmlTemplate);
            return TABLE_TEMPLATE.replace("{{tableData}}", tableData.toString()).replace("{{tableName}}", "Tables Meta Data");

        }

        public static String generateHTMLForTableView(String tableName, TreeSet<String> allColumns, TreeSet<Row> rows) {
            int displayedRowCount = 0;
            String nextKey = null;

            TreeSet<String> columns = new TreeSet<>();

            for (Row row : rows) {
                if (displayedRowCount >= 10) {
                    nextKey = row.key();
                    break;
                }
                for (String col : row.columns()) {
                    columns.add(col);
                }

                displayedRowCount++;
            }

            displayedRowCount = 0;

            // collect all columns for different keys
            StringBuilder tableData = new StringBuilder();

            // Add table headers
            if (!columns.isEmpty()) {
                tableData.append("<tr>");
                tableData.append("<th>").append("keys").append("</th>");
                for (String column : columns) {
                    tableData.append("<th>").append(column).append("</th>");
                }
                tableData.append("</tr>");
            }

            // Add table data
            for (Row row : rows) {
                if (displayedRowCount >= 10) {
                    nextKey = row.key();
                    break;
                }
                tableData.append("<tr>");
                tableData.append("<td>").append(KeyEncoder.decode(row.key())).append("</td>");

                for (String column : columns) {
                    String data = row.get(column);
                    if (data == null) {
                        tableData.append("<td>").append("</td>");
                    } else {
                        tableData.append("<td>").append(data).append("</td>");
                    }
                }

                tableData.append("</tr>");
                displayedRowCount++;
            }

            if (nextKey != null) {
                // add the "Next" link
                tableData.append("<div><Button class='bottom-link'>");
                tableData.append("<a href=\"/view/").append(tableName).append("?fromRow=").append(nextKey).append("\">Next</a>");
                tableData.append("</Button></div>");
                // add css
                tableData.append("<style>");
                tableData.append(".bottom-link {");
                tableData.append("  position: fixed;");
                tableData.append("  bottom: 0;");
                tableData.append("  width: 30%;");
                tableData.append("  font-size: 24px;");
                tableData.append("  font-weight: bold;");
                tableData.append("  text-align: center;");
                tableData.append("  background-color: #f9f9f9;");
                tableData.append("  padding: 10px 0;");
                tableData.append("}");
                tableData.append("</style>");
            }

            // Load the HTML template from "tableTemplate.html"
            //	            System.out.println(htmlTemplate);
            return TABLE_TEMPLATE.replace("{{tableData}}", tableData.toString()).replace("{{tableName}}", tableName);

        }
    }


    /**
     * Creates a thread that makes the periodic /ping requests
     */
    public static void startPingThread(String coordinatorEntry, int workerPortNo, String workerId, String storagePath) {
        // create a URL
        String urlStr;
        if (workerId == null && storagePath != null) {
            workerId = generateRandomId(); // generate a random workerId
            writeWorkerId(workerId, storagePath); // write the workerId into a file called Id
        }
        urlStr = "http://" + coordinatorEntry + "/ping?id=" +
                workerId + "&port=" + workerPortNo;
        Thread thread = new Thread(() -> {
            while (true) {
                // invoke Thread.sleep to wait for the required interval
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                URL url;
                InputStream is = null;
                try {
                    url = new URL(urlStr);
                    is = url.openStream();  // 打开流
                } catch (IOException e) {
                    logger.warn(e.getMessage());
                    e.printStackTrace();
                } finally {
                    if (is != null) {
                        try {
                            is.close();  // 关闭流
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        });
        thread.start();
    }

    /**
     * writes the created workerId to file called id.
     *
     * @param workerId
     * @param storagePath
     */
    private static void writeWorkerId(String workerId, String storagePath) {
        File directory = new File(storagePath);

        // check if the directory exists; if not, create it
        if (!directory.exists()) {
            boolean directoryCreated = directory.mkdirs();
            if (directoryCreated) {
                System.out.println("Directory created successfully: " + directory.getAbsolutePath());
            } else {
                System.err.println("Failed to create the directory.");
                return;
            }
        }
        try {
            File idFile = new File(storagePath, "id");
            idFile.createNewFile();
            FileWriter writer = new FileWriter(idFile);
            writer.write(workerId);
            writer.close();
        } catch (IOException e) {
            System.err.println("Error creating the file: " + e.getMessage());
        }
    }

    /**
     * Generates a random id with 5 lower case-letters.
     *
     * @return
     */
    private static String generateRandomId() {

        Random random = new Random();
        StringBuilder workerId = new StringBuilder();

        for (int i = 0; i < 5; i++) {
            char randomChar = (char) (random.nextInt(26) + 'a'); // Generate a random lower-case letter
            workerId.append(randomChar);
        }

        return workerId.toString();
    }

    /**
     * Generates HTML page for user interface purpose.
     *
     * @param route
     * @param tableName
     * @param storagePath
     * @param workerPortNo
     * @return
     */
    public static String generateHTMLPage(String route, String tableName, String storagePath, int workerPortNo, String fromRow) {
        if (route.equals("/")) {
            // get table names from in memory and disk (sorted)
            TreeMap<String, Long> tableNamesWithCountsMap = getTableNames(storagePath);
            return DynamicHTMLGenerator.generateHTMLForTablesList(tableNamesWithCountsMap, workerPortNo);
        } else if (route.equals("/view")) {
            // get all columns for tableName
            TreeSet<String> allColumns = new TreeSet<>();
            TreeSet<Row> rowsSet = new TreeSet<>((r1, r2) -> { // override comparator for TreeSet
                return r1.key().compareTo(r2.key());
            });
            getAllColumnsAndRowsSet(allColumns, rowsSet, tableName, storagePath, fromRow);

            return DynamicHTMLGenerator.generateHTMLForTableView(tableName, allColumns, rowsSet);
        }

        return null;
    }

    protected static void getAllColumnsAndRowsSet(TreeSet<String> allColumns, TreeSet<Row> rowsSet, String tableName, String storagePath, String fromRow) {
        // get table view from in-memory
        Map<String, List<Row>> rows = database.get(tableName);
        if (rows != null) {
            for (String key : rows.keySet()) {
                Row row = getRow(tableName, key, null, storagePath);
                if (fromRow == null || row.key().compareTo(fromRow) >= 0) {
                    rowsSet.add(row);
                }

                for (String col : row.columns()) {
                    allColumns.add(col);
                }
            }
        }
        // get table view from disk
        // check if the storage directory exists
        File storageDir = new File(storagePath);

        if (storageDir != null) { // storage directory exists
            File[] tablesList = storageDir.listFiles();

            if (tablesList != null) {
                for (File table : tablesList) {
                    if (table.getName().equals(tableName)) {
                        if (table.isFile()) {
                            logger.warn("Encounter a file in the storage directory!");
//		                    System.out.println("Encounter a file in the storage directory!");
                        } else if (table.isDirectory()) { // store the table name and count of rows in the map
                            File[] rowsList = table.listFiles();
                            if (rowsList != null) { // count the number of rows within a table
                                for (File rowFile : rowsList) {
                                    if (rowFile.isFile()) {
//		                            	System.out.println(rowFile);
                                        // get Row using Row.readFrom(in)
                                        try (InputStream in = new FileInputStream(rowFile)) {
                                            Row row = Row.readFrom(in);
                                            if (fromRow == null || row.key().compareTo(fromRow) >= 0) {
                                                rowsSet.add(row);
                                            }
                                            for (String col : row.columns()) {
                                                allColumns.add(col);
                                            }
                                        } catch (Exception e) {
                                            logger.warn("cannot read file: " + e.getMessage());
                                            System.out.println("cannot read file: " + e.getMessage());
                                        }
                                    } else { // EC
                                        File[] subRowsList = rowFile.listFiles();
                                        for (File subRowFile : subRowsList) {
                                            // get Row using Row.readFrom(in)
                                            try (InputStream in = new FileInputStream(subRowFile)) {
                                                Row row = Row.readFrom(in);
                                                if (fromRow == null || row.key().compareTo(fromRow) >= 0) {
                                                    rowsSet.add(row);
                                                }
                                                for (String col : row.columns()) {
                                                    allColumns.add(col);
                                                }
                                            } catch (Exception e) {
                                                logger.warn("cannot read file: " + e.getMessage());
                                                System.out.println("cannot read file: " + e.getMessage());
                                            }
                                        }
                                    }
                                }
                            }
//		                    tableNamesWithCountsMap.put(table.getName(), count);
                        }
                    }
                }
            }
        }
    }

    /**
     * Puts a given row R into a table with name T.
     *
     * @param tableName
     * @param row
     */
    protected static void putRow(String tableName, Row row, String storagePath) throws IOException {
        if (tableName.startsWith("pt-")) { // add persistent table
            updateStorage(storagePath, tableName, row, "write");
            return;
        }
        if (!database.containsKey(tableName)) { // check if database contains the table
            database.put(tableName, new HashMap<>());
        }
        Map<String, List<Row>> tableEntry = database.get(tableName);
        if (tableEntry.containsKey(row.key())) {
            tableEntry.get(row.key()).add(row);
        } else {
            List<Row> rows = new ArrayList<>();
            rows.add(row);
            tableEntry.put(row.key(), rows);
        }
//		tableEntry.put(row.key, row);
    }

    private static void updateStorage(String storagePath, String tableName, Row row, String operation) throws IOException {
        // look for the table in disk
        File storageDir = new File(storagePath);
        String key = KeyEncoder.encode(row.key());

        if (storageDir != null) { // storage directory exists
            File[] tablesList = storageDir.listFiles();

            if (tablesList != null) {
                boolean tableExists = false;

                // write logic
                for (File table : tablesList) {
                    if (table.getName().equals(tableName)) {
                        if (table.isFile()) {
                            logger.warn("Encounter a file in the storage directory!");
                            System.out.println("Encounter a file in the storage directory!");
                            // delete the file
//							table.delete();
                        } else if (table.isDirectory()) {
//		                	String key = KeyEncoder.encode(row.key());
                            // EC
                            if (key.length() >= 6) {
                                String rowSubName = getRowSubName(key);
                                String rowSubPath = table.getPath() + File.separator + rowSubName;
                                // looking for a sub-directory
                                if (!lookForSubDirectory(table.getPath(), rowSubName)) {
                                    // create a sub-directory for row
                                    createSubDir(table.getPath() + File.separator + rowSubName);
                                }
                                updateRow(row, rowSubPath + File.separator + key);
                            } else {
                                updateRow(row, table.getPath() + File.separator + key);
                            }
                            tableExists = true;
                            break;
                        }
                    }
                }

                if (!tableExists) { // create a subdirectory
                    String tablePath = storagePath + File.separator + tableName;
                    // EC
                    if (key.length() >= 6) {
                        String rowSubName = getRowSubName(key);
                        String rowSubPath = tablePath + File.separator + rowSubName;
                        // looking for a sub-directory
                        if (!lookForSubDirectory(tablePath, rowSubName)) {
                            // create a sub-directory for row
                            createSubDir(tablePath + File.separator + rowSubName);
                        }
                        updateRow(row, rowSubPath + File.separator + key);
                    } else {
                        createSubDir(tablePath);
                        // insert a row
                        updateRow(row, tablePath + File.separator + key);
                    }
                }
            }
        }
//		if (operation.equals("write")) {
//			
//		}

    }

    /**
     * Creates a sub directory given the path.
     *
     * @param path
     * @return
     */
    private static boolean createSubDir(String path) {
        File file = new File(path);
        // add a sub-directory
        if (file.mkdirs()) {
            logger.info("Table created successfully.");
//            System.out.println("Table created successfully.");
            return true;
        } else {
            logger.warn("Failed to create subdirectory.");
//            System.out.println("Failed to create subdirectory.");
            return false;
        }
    }

    /**
     * Looks for a sub-directory by path and rowSubPath.
     *
     * @param path
     * @param rowSubPath
     * @return
     */
    private static boolean lookForSubDirectory(String path, String rowSubPath) {
        File file = new File(path, rowSubPath);

        return file.exists() && file.isDirectory();
    }

    /**
     * Looks for a file by path and fileName.
     *
     * @param path
     * @return
     */
    private static boolean lookForFile(String path, String fileName) {
        File file = new File(path, fileName);

        return file.exists() && file.isFile();
    }

    /**
     * Gets row's sub directory as a String.
     * Note the key length must be greater or equals to 6.
     *
     * @param key
     * @return
     */
    private static String getRowSubName(String key) {

        return "_" + key.substring(0, 2);
    }

    /**
     * Creates or overwrites (updates) the row.
     *
     * @param row
     * @param filePath
     * @throws IOException
     */
    private static void updateRow(Row row, String filePath) throws IOException {
        try (FileOutputStream fos = new FileOutputStream(filePath)) {
            fos.write(row.toByteArray());
        } catch (IOException e) {
            logger.error("Cannot write bytes to the file: " + e.getMessage());
            e.printStackTrace();
            throw (e);
        }
    }

    /**
     * Returns the row with key from a table with name tableName and a version number.
     * if version number is not specified (null), returns the latest version.
     * if the version number is not valid (< 0 || >= size()) return null.
     * version number should be >= 1 and <= latestVersion.
     *
     * @param tableName
     * @param key
     * @return
     */
    protected static Row getRow(String tableName, String key, Integer versionNo, String storagePath) {
        if (database.containsKey(tableName)) { // database contains table
            List<Row> rowList = database.get(tableName).get(key);
            if (rowList != null) {
                if (versionNo == null) {
                    return rowList.get(rowList.size() - 1);
                } else {
                    try {
                        return rowList.get(versionNo - 1);
                    } catch (IndexOutOfBoundsException e) {
                        System.out.println("invalid version number given");
                    }
                }
            }
        }
        if (tableName.startsWith("pt-")) { // get persistent table's row (not implementing version number)
            // look for the table in disk
            File storageDir = new File(storagePath);

            if (storageDir != null) { // storage directory exists
                File[] tablesList = storageDir.listFiles();

                if (tablesList != null) {
                    boolean tableExists = false;

                    // read logic
                    for (File table : tablesList) {
                        if (table.getName().equals(tableName)) {
                            if (table.isFile()) {
                                logger.warn("Encounter a file in the storage directory!");
//			                    System.out.println("Encounter a file in the storage directory!");
                            } else if (table.isDirectory()) {
                                File[] rowsList = table.listFiles();
                                if (rowsList != null) {
                                    // EC
                                    String fileName = KeyEncoder.encode(key);
                                    if (false) {
                                        String rowSubName = getRowSubName(key);
                                        String rowSubPath = table.getPath() + File.separator + rowSubName;
                                        for (File rowFile : rowsList) {
                                            if (rowFile.isDirectory() && rowFile.getName().equals(rowSubName)) {
                                                if (lookForFile(rowSubPath, fileName)) {
                                                    // get Row using Row.readFrom(in)
                                                    try (InputStream in = new FileInputStream(rowSubPath + File.separator + fileName)) {
                                                        Row row = Row.readFrom(in);
                                                        return row;
                                                    } catch (Exception e) {
                                                        logger.warn("cannot read file: " + e.getMessage());
                                                        System.out.println("cannot read file: " + e.getMessage());
                                                    }
                                                }
                                            }
                                        }
                                    } else {
                                        for (File rowFile : rowsList) {
                                            if (rowFile.isFile() && rowFile.getName().equals(fileName)) {
                                                // get Row using Row.readFrom(in)
                                                try (InputStream in = new FileInputStream(rowFile)) {
                                                    Row row = Row.readFrom(in);
                                                    return row;
                                                } catch (Exception e) {
                                                    logger.warn("cannot read file: " + e.getMessage());
                                                    System.out.println("cannot read file: " + e.getMessage());
                                                }
                                            } else {
                                                logger.warn("Encounter a directory within the table!");
//				                            	System.out.println("Encounter a directory within the table!");
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        return null; // otherwise
    }

    /**
     * Gets table names from in memory and disk and sort them lexicographically.
     *
     * @return the list of sorted table names
     */
    protected static TreeMap<String, Long> getTableNames(String storagePath) {
        TreeMap<String, Long> tableNamesWithCountsMap = new TreeMap<>();

        // add in-memory table names into the list
        for (Map.Entry<String, Map<String, List<Row>>> entry : database.entrySet()) {
            tableNamesWithCountsMap.put(entry.getKey(), (long) entry.getValue().size());
        }

        // add persistent table names into the list
        // check if the storage directory exists
        File storageDir = new File(storagePath);

        if (storageDir != null) { // storage directory exists
            File[] tablesList = storageDir.listFiles();

            if (tablesList != null) {
                for (File table : tablesList) {
                    if (table.isFile()) {
                        logger.warn("Encounter a file in the storage directory!");
//	                    System.out.println("Encounter a file in the storage directory!");
                    } else if (table.isDirectory()) { // store the table name and count of rows in the map
                        File[] rowsList = table.listFiles();
                        long count = 0;
                        if (rowsList != null) { // count the number of rows within a table
                            for (File row : rowsList) {
                                if (row.isFile()) {
                                    count++;
                                } else { // EC
                                    File[] subRowsList = row.listFiles();
                                    for (File subRow : subRowsList) {
                                        if (subRow.isFile()) {
                                            count++;
                                        }
                                    }
                                }
                            }
                        }
                        tableNamesWithCountsMap.put(table.getName(), count);
                    }
                }
            }
        }

        return tableNamesWithCountsMap;
    }


    private static String generateTableHyperLink(String tableName, int workerPortNo) {
        return "http://" + HOST + ":" + workerPortNo + "/view/" + tableName;
    }
}
