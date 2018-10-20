/*
 * File:    Oracle2Postgres.java
 * Project: Oracle2Postgres
 * Date:    Oct 12, 2018 11:07:39 PM
 * Author:  Igor Morenko <morenko at lionsoft.ru>
 * 
 * Copyright 2005-2018 LionSoft LLC. All rights reserved.
 */
package ru.lionsoft.oracle2postgres;

import java.io.IOException;

/**
 *
 * @author Igor Morenko <morenko at lionsoft.ru>
 */
public class Oracle2Postgres extends Thread {
    
    private final TransferContext context;

    public Oracle2Postgres(TransferContext context) {
        this.context = context;
    }
    
    public static void main(String[] args) {
        if (args.length > 1 || (args.length > 0 && args[0].equals("--help"))) 
            usage();
        
        TransferContext ctx = new TransferContext();
        try {
            if (args.length > 0) {
                ctx.readPropertiesFromFile(args[0]);
            } else {
                ctx.readPropertiesFromConsole();
            }
        } catch (IOException ex) {
            System.err.println("ERROR: " + ex.getLocalizedMessage());
            return;
        }
        ctx.printParameters();
        
        try (TransferManager manager = new TransferManager(ctx);) {
            // if all tables
            if (ctx.getTables().isEmpty()) {
                ctx.setTables(manager.getSchemaTables());
            }

            manager.extractSchemaDDL();

            ctx.log("Tables of Schema " + ctx.getOwner());
            ctx.writeDDL("\n--\n-- Tables of Schema " + ctx.getOwner() + "\n--\n");

            ctx.initializeJobs();
            //...
            if (ctx.getThreadsNum() > 1) {
                // TODO: multithread transfer data
                // run thread
                Oracle2Postgres[] threads = new Oracle2Postgres[ctx.getThreadsNum() - 1];
                for (int i = 0; i < ctx.getThreadsNum() - 1; i++) {
                    threads[i] = new Oracle2Postgres(ctx);
                    threads[i].start();
                }
                // Extract DDL for tables in main thread
                manager.extractSchemaTablesDDL();
                // wait stop all threads
                int alive = threads.length;
                while (alive > 0) {
                    alive = 0;
                    for (int i = 0; i < threads.length; i++) {
                        Oracle2Postgres thread = threads[i];
                        if (thread.isAlive()) alive++;
                    }
                }
            } else {
                // Extract DDL for tables
                manager.extractSchemaTablesDDL();
            }
            // Extract DDL Foreign Key of tables
            manager.extractSchemaForeignKeysDDL();
            // End
            ctx.log("Finish");
            ctx.writeDDL("\n--\n-- End of Script\n--");
        } catch (Exception ex) {
            ctx.error(ex.getLocalizedMessage());
        }
        ctx.close();
    }
    
    public static void usage() {
        System.out.println("Usage: oracle2postgres [<properties_file>]");
        System.exit(0);
    }

    @Override
    public void run() {
        context.log("Thread #" + getId() + ": Start");
        try (TransferManager manager = new TransferManager(context);) {
            manager.extractSchemaTablesDDL();
        } catch (Exception ex) {
            context.error("Thread #" + getId() + ": " + ex.getLocalizedMessage());
        } 
        context.log("Thread #" + getId() + ": Stop");
    }
}
