/*
 * File:    TransferContext.java
 * Project: Oracle2Postgres
 * Date:    Oct 12, 2018 11:08:18 PM
 * Author:  Igor Morenko <morenko at lionsoft.ru>
 * 
 * Copyright 2005-2018 LionSoft LLC. All rights reserved.
 */
package ru.lionsoft.oracle2postgres;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeSet;

/**
 *
 * @author Igor Morenko <morenko at lionsoft.ru>
 */
public class TransferContext {
    
    // =================== Fields ====================

    // Source Database
    private String srcHost;
    private String srcPort;
    private String srcDatabase;
    private String srcUsername;
    private String srcPassword;

    // Destination Database
    private String destHost;
    private String destPort;
    private String destDatabase;
    private String destUsername;
    private String destPassword;

    // Object for transfer
    private String owner;
    private Set<String> tables = new TreeSet<>();
    private List<String> jobs = new LinkedList<>();
    
    // Transfer options
    private int sampleRows;
    private int chunkSize;
    private boolean createSchema = false;
    private boolean createTable = false;
    private boolean transferRows = false;
    private int threadsNum;
    
    // Output streams
    private PrintStream ddlStream = System.out;
    private PrintStream logStream = System.out;
    
    // =================== Constructors ===========================

    // =================== Getters and Setters ====================

    public String getSrcHost() {
        return srcHost;
    }

    public String getSrcPort() {
        return srcPort;
    }

    public String getSrcDatabase() {
        return srcDatabase;
    }

    public String getSrcUsername() {
        return srcUsername;
    }

    public String getSrcPassword() {
        return srcPassword;
    }

    public String getDestHost() {
        return destHost;
    }

    public String getDestPort() {
        return destPort;
    }

    public String getDestDatabase() {
        return destDatabase;
    }

    public String getDestUsername() {
        return destUsername;
    }

    public String getDestPassword() {
        return destPassword;
    }

    public String getOwner() {
        return owner;
    }

    public int getSampleRows() {
        return sampleRows;
    }

    public int getChunkSize() {
        return chunkSize;
    }

    public int getThreadsNum() {
        return threadsNum;
    }

    public boolean isCreateSchema() {
        return createSchema;
    }

    public boolean isCreateTable() {
        return createTable;
    }

    public boolean isTransferRows() {
        return transferRows;
    }

    public Set<String> getTables() {
        return tables;
    }

    public void setTables(Set<String> tables) {
        this.tables = tables;
    }
    
    // ========== Equals Objects ==================

    // =================== Cast to String ====================

    // =================== Bussiness Methods ====================

    private final Scanner scanner = new Scanner(System.in);

    private String input(String message, String defaultValue) {
        System.out.print(message);
        String line = scanner.nextLine();
        return (line.isEmpty() ? defaultValue : line);
    }
    
    private boolean inputYesNo(String message, String defaultValue) {
        return input(message, defaultValue).toUpperCase().startsWith("Y");
    }
    
    public void readPropertiesFromConsole() throws FileNotFoundException {
//        Console console = System.console();

        System.out.println("\n------------------------------------------");
        System.out.println("Enter source database settings:");
        System.out.println("------------------------------------------");
        srcHost = input("- Hostname for source database (default 'localhost'): ", "localhost");
        srcPort = input("- Port for source database (default '1521'): ", "1521");
        srcDatabase = input("- Name of source database (default 'orcl'): ", "orcl");
        srcUsername = input("- Username on source database (default 'scott'): ", "scott");
        srcPassword = input("- Password for source database: ", ""); // getpass

        owner = input("\n- Owner of source database (default: 'scott'): ", "scott").toUpperCase();
        String tablist = input("- Table list on source database by comma (default: '*'): ", "*").toUpperCase();
        if (!tablist.equals("*")) {
          tables.addAll(Arrays.asList(tablist.split(",")).stream().map(String::trim).collect(Collectors.toList()));
        }
        
        createSchema = inputYesNo("\n- Create target schema (default: 'no'): ", "no");
        if (createSchema) {
            createTable = true;
        } else {
            createTable = inputYesNo("\n- Create target tables (default: 'no'): ", "no");
        }
        transferRows = inputYesNo("- Transfer rows to target tables (default: 'no'): ", "no");
        if (transferRows) {
            sampleRows  = Integer.parseInt(input("- Sample rows for transfer (default: 200): ", "200"));
            chunkSize   = Integer.parseInt(input("- Chunk size for transfer (default: 2000): ", "2000"));
            threadsNum = Integer.parseInt(input("- Treads number (default: 1): ", "1"));
        }
        
        if (createTable || transferRows) {
            System.out.println("\n------------------------------------------");
            System.out.println("Enter target database settings:");
            System.out.println("------------------------------------------");
            destHost = input("- Hostname for target database (default 'localhost'): ", "localhost");
            destPort = input("- Port for target database (default '5432'): ", "5432");
            destDatabase = input("- Name of target database (default 'postgres'): ", "postgres");
            destUsername = input("- Username on target database (default 'postgres'): ", "postgres");
            destPassword = input("- Password for target database: ", ""); // getpass
        }
        
        String ddlFilename = input("\n- Filename for DDL script (default: 'stdout'): ", null);
        if (ddlFilename != null) {
            ddlStream = new PrintStream(ddlFilename);
        }
        String logFilename = input("- Filename for logging (default: 'stdout'): ", null);
        if (logFilename != null) {
            logStream = new PrintStream(logFilename);
        }
    }

/*
    // Parse connect string: username/password@host[:port]/database
    private void parseConnectString(String str) {
        int slash = str.indexOf('/');
        int etta = str.indexOf('@');
        int comma = str.indexOf(':');
        int slash2 = str.lastIndexOf('/');

        srcUsername = str.substring(0, slash);
        srcPassword = str.substring(slash + 1, etta);
        if (comma == -1) {
            // not port
            srcHost = str.substring(etta + 1, slash2);
            srcPort = "1521";
        } else {
            srcHost = str.substring(etta + 1, comma);
            srcPort = str.substring(comma + 1, slash2);
        }
        srcDatabase = str.substring(slash2 + 1);
   }
*/
    private boolean nvl(String value, boolean defaultValue) {
        if (value == null) return defaultValue;
        return value.toLowerCase().startsWith("y") || value.toLowerCase().equals("true");
    }
    
    private int nvl(String value, int defaultValue) {
        if (value == null) return defaultValue;
        return Integer.parseInt(value);
    }

    public void readPropertiesFromFile(String filename) throws FileNotFoundException, IOException {
        Properties properties = new Properties();
        
        properties.load(new FileInputStream(filename));
        
        // source
        srcHost = properties.getProperty("source.host", "localhost");
        srcPort = properties.getProperty("source.port", "1521");
        srcDatabase = properties.getProperty("source.database", "orcl");
        srcUsername = properties.getProperty("source.username", "scott");
        srcPassword = properties.getProperty("source.password", "");
        
        owner = properties.getProperty("source.owner", "scott");
        String tablist = properties.getProperty("source.tables", "*");
        if (!tablist.equals("*")) {
            for (String tableName : tablist.split(",")) {
                tables.add(tableName.trim());
            }
        }
        
        // target
        destHost = properties.getProperty("target.host", "localhost");
        destPort = properties.getProperty("target.port", "5432");
        destDatabase = properties.getProperty("target.database", "postgres");
        destUsername = properties.getProperty("target.username", "postgres");
        destPassword = properties.getProperty("target.password", "");
        
        // options
        createSchema = nvl(properties.getProperty("target.createSchema"), false);
        if (createSchema) {
            createTable = true;
        } else {
            createTable = nvl(properties.getProperty("target.createTable"), false);
        }
        transferRows = nvl(properties.getProperty("target.transferRows"), false);
        
        // transfer options
        sampleRows = nvl(properties.getProperty("transfer.sampleRows"), 200);
        chunkSize = nvl(properties.getProperty("transfer.chunkSize"), 2000);
        threadsNum = nvl(properties.getProperty("transfer.threadsNum"), 1);
        
        // output properties
        String ddlFilename = properties.getProperty("ddl.filename");
        if (ddlFilename != null) {
            ddlStream = new PrintStream(ddlFilename);
        }
        String logFilename = properties.getProperty("log.filename");
        if (logFilename != null) {
            logStream = new PrintStream(logFilename);
        }
    }

    public void printParameters() {
        logStream.println("\nSource Oracle database:");
        logStream.println("  Hostname: " + srcHost);
        logStream.println("  Port: " + srcPort);
        logStream.println("  Database name: " + srcDatabase);
        logStream.println("  Username: " + srcUsername);
        logStream.println("  Password: " + StringUtils.rpad("", srcPassword.length(), '*'));
        
        logStream.println("\nOwner: " + owner);
        logStream.println("Tables: " + tables);

        if (createTable || transferRows) {
            logStream.println("\nTarget PostgreSQL database:");
            logStream.println("  Hostname: " + destHost);
            logStream.println("  Port: " + destPort);
            logStream.println("  Database name: " + destDatabase);
            logStream.println("  Username: " + destUsername);
            logStream.println("  Password: " + StringUtils.rpad("", destPassword.length(), '*'));
        }

        logStream.println("\nOptions:");
        logStream.println("  Create target schema: " + createSchema);
        logStream.println("  Create target tables: " + createTable);
        logStream.println("  Transfer table  rows: " + transferRows);
        if (transferRows) {
            logStream.println("  Sample rows: " + sampleRows);
            logStream.println("  Chunk  size: " + chunkSize);
            logStream.println("  Threads num: " + threadsNum);
        }
        logStream.println();
    }

    // Logging message
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
    private String getCurrentDateTime() {
        return DATE_FORMAT.format(new Date());
    }

    public synchronized void log(String message) {
        logStream.println(getCurrentDateTime() + " " + message);
    }
    
    public synchronized void info(String message) {
        log("INFO: " + message);
    }
    
    public synchronized void warning(String message) {
        System.err.println("WARNING: " + message);
        log("WARNING: " + message);
    }

    public synchronized void error(String message) {
        System.err.println("ERROR: " + message);
        log("ERROR: " + message);
    }
    
    // Write DDL
    public synchronized void writeDDL(String ddl) {
        ddlStream.println(ddl);
    }
    
    // Jobs
    public void initializeJobs(){
        jobs.clear();
        for (String tableName : tables) {
            jobs.add(tableName);
        }
    }
    
    public synchronized String getJob() {
        if (jobs.isEmpty()) return null; // stop job
        
        String tableName = jobs.get(0); // pop job
        jobs.remove(0);
        
        return tableName;
    }
    
    public void close() {
        logStream.close();
        ddlStream.close();
    }
}
