/*
 * File:    TransferManager.java
 * Project: Oracle2Postgres
 * Date:    Oct 12, 2018 11:08:32 PM
 * Author:  Igor Morenko <morenko at lionsoft.ru>
 * 
 * Copyright 2005-2018 LionSoft LLC. All rights reserved.
 */
package ru.lionsoft.oracle2postgres;

import java.io.IOException;
import java.io.StringReader;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Set;
import java.util.TreeSet;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

/**
 *
 * @author Igor Morenko <morenko at lionsoft.ru>
 */
public class TransferManager implements AutoCloseable {
    
    // =================== Constants ====================

    private final static int COLUMN_NAME_LENGTH = 20;
    private final static String ORACLE_DRIVER = "oracle.jdbc.OracleDriver";
    private final static String POSTGRES_DRIVER = "org.postgresql.Driver";

    // =================== Fields ====================

    // Transfer Context (config and workspace)
    private final TransferContext ctx;

    // Connections
    private Connection srcConnection;  // Source Oracle
    private Connection destConnection; // Destination PostgreSQL

    // =================== Constructors ===========================

    public TransferManager(TransferContext ctx) throws ClassNotFoundException, SQLException {
        this.ctx = ctx;
        
        connectToDatabases();
    }
    
    // =================== Getters and Setters ====================

    // ========== Equals Objects ==================

    // =================== Cast to String ====================
    
    // =================== Bussiness Methods ====================

    private void connectToDatabases() throws ClassNotFoundException, SQLException {
        Class.forName(ORACLE_DRIVER);
        String srcUrl = "jdbc:oracle:thin:@" + ctx.getSrcHost() + ':' + ctx.getSrcPort() + ':' + ctx.getSrcDatabase();
        ctx.log("-- Source URL: " + srcUrl);
        srcConnection = DriverManager.getConnection(srcUrl, ctx.getSrcUsername(), ctx.getSrcPassword());
        ctx.log("Connecting to source database");

        if (ctx.isCreateTable() || ctx.isTransferRows()) {
            Class.forName(POSTGRES_DRIVER);
            String destUrl = "jdbc:postgresql://" + ctx.getDestHost() + ':' + ctx.getDestPort() + '/' + ctx.getDestDatabase();
            ctx.log("-- Target URL: " + destUrl);
            destConnection = DriverManager.getConnection(destUrl, ctx.getDestUsername(), ctx.getDestPassword());
            ctx.log("Connecting to target database");
        }
    }

    // Disconnect from databases
    @Override
    public void close() {
        if (srcConnection != null) {
            try {
                srcConnection.close();
            } catch (SQLException ex) {
                ctx.error(ex.getLocalizedMessage());
            }
            srcConnection = null;
            ctx.log("Disconnect from source database");
        }
        if (destConnection != null) {
            try {
                destConnection.close();
            } catch (SQLException ex) {
                ctx.error(ex.getLocalizedMessage());
            }
            destConnection = null;
            ctx.log("Disconnect from target database");
        }
    }

    private void executeDDL(String sql, String message) {
        if (destConnection == null) return;

        if (sql.startsWith("--")) {
            // comment
            ctx.log(message + " ... Skip");
            return;
        } 

        try (Statement stmt = destConnection.createStatement();) {
            stmt.executeUpdate(sql);
            ctx.log(message + " ... Ok");
        } catch (SQLException ex) {
            ctx.error(message + " ... Failed");
            ctx.error("Execute SQL: {" + sql + "}");
            ctx.error("Failed: " + ex.getLocalizedMessage());
        }
    }

    private String constraintColumns(String owner, String constraintName) throws SQLException {
        StringBuilder sb = new StringBuilder();
        try (PreparedStatement pstmt = srcConnection.prepareStatement(
                "SELECT column_name FROM all_cons_columns WHERE owner = ? AND constraint_name = ?")) {
            pstmt.setString(1, owner);
            pstmt.setString(2, constraintName);
            int i = 0;
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    if (i++ > 0) sb.append(", ");
                    sb.append(rs.getString(1));
                }
            }
        }
        return sb.toString();
    }
    
    private boolean existConstraint(String owner, String constraintName) throws SQLException {
        try (PreparedStatement pstmt = srcConnection.prepareStatement(
                "SELECT 1 FROM all_constraints WHERE owner = ? AND constraint_name = ?")) {
            pstmt.setString(1, owner);
            pstmt.setString(2, constraintName);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) return true;
            }
        }
        return false;
    }

    private String referencesTable(String owner, String constraintName) throws SQLException {
        StringBuilder sb = new StringBuilder();
        String tableName;

        try (PreparedStatement pstmt = srcConnection.prepareStatement("SELECT owner||'.'||table_name FROM all_constraints WHERE owner = ? AND constraint_name = ?")) {
            pstmt.setString(1, owner);
            pstmt.setString(2, constraintName);
            try (ResultSet rs = pstmt.executeQuery()) {
                rs.next();
                tableName = rs.getString(1);
            }
        }

        sb.append(tableName);
        sb.append('(');
        sb.append(constraintColumns(owner, constraintName));
        sb.append(')');
        return sb.toString();
    }

    private void extractTableConstraintsPUC(String owner, String tableName) throws SQLException {

        ctx.log("-- Constraints for table " + owner + '.' + tableName);
        ctx.writeDDL("\n-- Constraints for table " + owner + '.' + tableName);
        try (PreparedStatement pstmt = srcConnection.prepareStatement(
                          "SELECT owner, constraint_name, constraint_type, search_condition "
                        + "FROM all_constraints "
                        + "WHERE owner = ? AND table_name = ? AND constraint_type != 'R'")) {
            pstmt.setString(1, owner);
            pstmt.setString(2, tableName);
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    String constraint;
                    String constraintOwner = rs.getString("owner");
                    String constraintName = rs.getString("constraint_name");
                    String searchCondition = rs.getString("search_condition");
                    switch (rs.getString("constraint_type")) {
                        case "P":
                            constraint = " PRIMARY KEY (" + constraintColumns(constraintOwner, constraintName) + ')';
                            break;
                        case "U":
                            constraint = " UNIQUE (" + constraintColumns(constraintOwner, constraintName) + ')';
                            break;
                        case "C":
                            constraint = " CHECK (" + searchCondition + ')';
                            break;
                        default:
                            constraint = "???";
                    }
                    String sql =
                        (searchCondition != null && searchCondition.matches(".* IS NOT NULL") ? "--" : "") +
                        "ALTER TABLE " + owner + '.' + tableName +
                        " ADD CONSTRAINT " + constraintName + constraint;
                    ctx.writeDDL(sql + ';');
                    if (ctx.isCreateTable()) {
                        executeDDL(sql, "Create constraint " + constraintOwner + '.' + constraintName);
                    }
                }
            }
        }
    }

    private void extractTableConstraintsFK(String owner, String tableName) throws SQLException {

        ctx.log("-- Constraints FK for table " + owner + '.' + tableName);
        ctx.writeDDL("\n-- Constraints for table " + owner + '.' + tableName);
        try (PreparedStatement pstmt = srcConnection.prepareStatement(
                          "SELECT owner, constraint_name, r_owner, r_constraint_name, delete_rule "
                        + "FROM all_constraints "
                        + "WHERE owner = ? AND table_name = ? AND constraint_type='R'")) {
            pstmt.setString(1, owner);
            pstmt.setString(2, tableName);
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    String constraint;
                    String constraintOwner = rs.getString("owner");
                    String constraintName = rs.getString("constraint_name");
                    String refOwner = rs.getString("r_owner");
                    String refConstraintName = rs.getString("r_constraint_name");
                    String deleteRule = rs.getString("delete_rule");
                    constraint = " FOREIGN KEY (" + constraintColumns(constraintOwner, constraintName) + ") REFERENCES " +
                        referencesTable(refOwner, refConstraintName) +
                        (deleteRule.equals("NO ACTION") ? "" : " ON DELETE " + deleteRule);
                    String sql =
                        "ALTER TABLE " + owner + '.' + tableName +
                        " ADD CONSTRAINT " + constraintName + constraint;

                    // published
                    ctx.writeDDL(sql + ';');
                    if (ctx.isCreateTable()) {
                        executeDDL(sql, "Create constraint " + constraintOwner + '.' + constraintName);
                    }
                }
            }
        }
    }

    public void extractSchemaForeignKeysDDL() throws SQLException {
        ctx.log("Constraints Foreign Key of Schema " + ctx.getOwner());
        ctx.writeDDL("\n--\n-- Constraints Foreign Key of Schema " + ctx.getOwner() + "\n--\n");
        for (String tableName : ctx.getTables()) {
            extractTableConstraintsFK(ctx.getOwner(), tableName);
        }
    }
    
    private String indexColumns(String owner, String indexName) throws SQLException {
      StringBuilder sb = new StringBuilder();
        try (PreparedStatement pstmt = srcConnection.prepareStatement(
                          "SELECT column_name "
                        + "FROM all_ind_columns "
                        + "WHERE index_owner = ? AND index_name = ?")) {
            pstmt.setString(1, owner);
            pstmt.setString(2, indexName);
            int i = 0;
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    if (i++ > 0) sb.append(", ");
                    sb.append(rs.getString(1));
                }
            }
        }
        return sb.toString();
    }

    private void extractTableIndexesDDL(String owner, String tableName) throws SQLException {
        ctx.log("-- Indexes for table " + owner + '.' + tableName);
        ctx.writeDDL("\n-- Indexes for table " + owner + '.' + tableName);
        try (PreparedStatement pstmt = srcConnection.prepareStatement(
                          "SELECT owner, index_name, index_type, uniqueness "
                        + "FROM all_indexes "
                        + "WHERE table_owner = ? AND table_name = ?")) {
            pstmt.setString(1, owner);
            pstmt.setString(2, tableName);
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    String indexType = rs.getString("index_type");
                    String indexOwner = rs.getString("owner");
                    String indexName = rs.getString("index_name");
                    String uniqueness = rs.getString("uniqueness");
                    if (indexType.equals("NORMAL")) {
                        String sql =
                            (existConstraint(indexOwner, indexName) ? "--" : "") +
                            "CREATE " + (uniqueness.equals("UNIQUE") ? "UNIQUE " : "") + "INDEX " + indexName +
                            " ON " + owner + '.' + tableName + '(' + indexColumns(indexOwner, indexName) + ")";

                        // published
                        ctx.writeDDL(sql + ';');
                        if (ctx.isCreateTable()) {
                            executeDDL(sql, "Create index " + indexOwner + '.' + indexName);
                        }
                    } else {
                        ctx.log("-- Index " + indexName + " of type '" + indexType + "' ????");
                    }
                }
            }
        }
    }
    
    // PostgreSQL Data Type Convert
    private String postgresColumnType(String dataType, String dataLength, int dataScale, int dataPrecision) {
        String pgType;
        
        switch (dataType) {
            case "CHAR":
            case "NCHAR":
                pgType = "char(" + dataLength + ')';
                break;

            case "VARCHAR2":
            case "VARCHAR":
            case "NVARCHAR2":
            case "NVARCHAR":
                pgType = "varchar(" + dataLength + ')';
                break;

            case "NUMBER":
                // number
                if (dataScale == 0) {
                    // integer
                    if (dataPrecision < 5) {
                        pgType = "smallint";
                    } else if (dataPrecision >= 5 && dataPrecision <= 8) {
                        pgType = "int";
                    } else if (dataPrecision >= 9 && dataPrecision <= 18) {
                        pgType = "bigint";
                    } else {
                        // data_precision > 18
                        pgType = "decimal(" + dataPrecision + ')';
                    }
                } else {
                    // data_scale > 0
                    pgType = "decimal(" + dataPrecision + ',' + dataScale + ')'; // or numeric ?
                }   
                break;

            case "DATE":
                pgType = "timestamp(0)";
                break;

            case "TIMESTAMP":
                pgType = "timestamp(" + dataLength + ')';
                break;

            case "TIMESTAMP(6)":
                pgType = "timestamp(6)";
                break;

            case "TIMESTAMP(6) WITH TIME ZONE":
                pgType = "timestamp(6) with time zone";
                break;

            case "ROWID":
                pgType = "char(10)";
                break;

            case "LONG":
            case "CLOB":
            case "NCLOB":
                pgType = "text";
                break;

            case "BLOB":
            case "RAW":
            case "LONG RAW":
                pgType = "bytea";
                break;

            case "BFILE":
                pgType = "varchar(255)"; // or bytea (read-only)
                break;

            case "BINARY_INTEGER":
                pgType = "integer"; // ??
                break;

            case "BINARY_FLOAT":
                pgType = "real";
                break;

            case "BINARY_DOUBLE":
                pgType = "double precision";
                break;

            case "XMLTYPE":
                pgType = "xml";
                break;

            default:
                pgType = "???";
                break;
        }

        return pgType;
    }

    private void extractTableDDL(String owner, String tableName) throws SQLException {

        ctx.log("-- Table " + owner + '.' + tableName);
        ctx.writeDDL("\n--\n-- Table " + owner + '.' + tableName + "\n--\n");

        // Drop table
        extractDropTableDDL(owner, tableName);

        // Create table
        extractCreateTableDDL(owner, tableName);

        // Comments for table
        ctx.log("-- Comments for table " + owner + '.' + tableName);
        ctx.writeDDL("\n-- Comments for table " + owner + '.' + tableName);
        extractTableCommentDDL(owner, tableName);

        // Comments for table columns
        extractTableColumnCommentsDDL(owner, tableName);

        // Constraints for Table (Primary Key, Unique, Check)
        extractTableConstraintsPUC(owner, tableName);

        // Indexes for Table
        extractTableIndexesDDL(owner, tableName);

        // Sequence ???
    }

    private void extractDropTableDDL(String owner, String tableName) {
        String sql = "DROP TABLE " + owner + '.' + tableName + " CASCADE";
        ctx.writeDDL(sql + ';');
        if (ctx.isCreateTable()) {
            executeDDL(sql, "Drop table " + owner + '.' + tableName);
        }
    }

    private void extractCreateTableDDL(String owner, String tableName) throws SQLException {
        StringBuilder sb = new StringBuilder("CREATE TABLE " + owner + '.' + tableName + " (\n");
        // Columns
        try (PreparedStatement pstmt = srcConnection.prepareStatement(
                "SELECT column_name, data_type, data_length, data_scale, data_precision, nullable, data_default "
                        + "FROM all_tab_columns "
                        + "WHERE owner = ? AND table_name = ? "
                        + "ORDER BY column_id")) {
            pstmt.setString(1, owner);
            pstmt.setString(2, tableName);
            try (ResultSet rs = pstmt.executeQuery()) {
                int i = 0;
                while (rs.next()) {
                    // Line Prefix
                    if (i++ > 0) sb.append(", ");
                    else         sb.append("  ");

                    // Column
                    String columnName = rs.getString("column_name");
                    sb.append(StringUtils.rpad(columnName, COLUMN_NAME_LENGTH))
                            .append(' ');
                    
                    // PostgreSQL Data Type Convert
                    String dataType = rs.getString("data_type");
                    String dataLength = rs.getString("data_length");
                    int dataScale = rs.getInt("data_scale");
                    int dataPrecision = rs.getInt("data_precision");
                    sb.append(postgresColumnType(dataType, dataLength, dataScale, dataPrecision));

                    // Nullable
                    if (rs.getString("nullable").equals("N")) sb.append(" NOT NULL");

                    // Default
                    String dataDefault = rs.getString("data_default");
                    if (dataDefault != null && !dataDefault.isEmpty()) {
                        // normalize
                        dataDefault = StringUtils.rtrim(dataDefault, " \n");
                        // System.out.println("'"+ dataDefault + "'");

                        sb.append(" DEFAULT ");
                        switch (dataDefault.toUpperCase()) {
                            case "SYSDATE":
                            case "SYSTIMESTAMP":
                                sb.append("now()::timestamp");
                                break;

                            case "EMPTY_BLOB()":
                            case "EMPTY_CLOB()":
                                sb.append("''");
                                break;

                            default:
                                sb.append(dataDefault);
                        }
                    }
                    // End Column defenition
                    sb.append('\n');
                } // end while
            } // end fetch
        } // end select
        sb.append(")");
        String sql = sb.toString();
        ctx.writeDDL(sql + ';');
        if (ctx.isCreateTable()) {
            executeDDL(sql, "Create table " + owner + '.' + tableName);
        }
    }

    /**
     * Extract Comments for table
     * @param owner schema owner
     * @param tableName table name
     * @exception SQLException when sql error execute
     */
    private void extractTableCommentDDL(String owner, String tableName) throws SQLException {
        try (PreparedStatement pstmt = srcConnection.prepareStatement(
                "SELECT comments "
                        + "FROM all_tab_comments "
                        + "WHERE owner = ? AND table_name = ?")) {
            pstmt.setString(1, owner);
            pstmt.setString(2, tableName);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    String comments = rs.getString(1);
                    if (comments != null && !comments.isEmpty()) {
                        comments = comments.replaceAll("'", "''"); // quoted apostrof
                        String sql = "COMMENT ON TABLE " + owner + '.' + tableName + " IS '" + comments + '\'';
                        ctx.writeDDL(sql + ';');
                        if (ctx.isCreateTable()) {
                            executeDDL(sql, "Create comment for table " + owner + '.' + tableName);
                        }
                    }
                }
            }
        }
    }

    /**
     * Extract Comments for table columns
     * @param owner schema owner
     * @param tableName table name
     * @exception SQLException when sql error execute
     */
    private void extractTableColumnCommentsDDL(String owner, String tableName) throws SQLException {
        try (PreparedStatement pstmt = srcConnection.prepareStatement(
                "SELECT column_name, comments "
                        + "FROM all_col_comments "
                        + "WHERE owner = ? AND table_name = ?")) {
            pstmt.setString(1, owner);
            pstmt.setString(2, tableName);
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    String comments = rs.getString("comments");
                    if (comments != null && !comments.isEmpty()) {
                        comments = comments.replaceAll("'", "''"); // quoted apostrof
                        String columnName = rs.getString("column_name");
                        String sql = "COMMENT ON COLUMN "
                                + owner + '.' + tableName + '.' + StringUtils.rpad(columnName, COLUMN_NAME_LENGTH) 
                                + " IS '" + comments + '\'';
                        ctx.writeDDL(sql + ';');
                        if (ctx.isCreateTable()) {
                            executeDDL(sql, "Create comment for column " + owner + '.' + tableName + '.' + columnName);
                        }
                    }
                }
            }
        }
    }
    
    public void extractSchemaDDL() {
        String schema = ctx.getOwner();
        ctx.log("-- Schema: " + schema);
        ctx.writeDDL("--\n-- Schema " + schema + "\n--\n");
        String sql = "DROP SCHEMA " + schema + " CASCADE";
        ctx.writeDDL(sql + ';');
        if (ctx.isCreateSchema()) {
            executeDDL(sql, "Drop schema " + schema);
        }
        sql = "CREATE SCHEMA " + schema + "\n  AUTHORIZATION " + ctx.getDestUsername();
        ctx.writeDDL(sql + ';');
        if (ctx.isCreateSchema()) {
            executeDDL(sql, "Create schema " + schema);
        }
        sql = "COMMENT ON SCHEMA " + schema + "\n  IS 'schema " + schema + "'";
        ctx.writeDDL(sql + ';');
        if (ctx.isCreateSchema()) {
            executeDDL(sql, "Comment on schema " + schema);
        }
        ctx.writeDDL("--GRANT ALL ON SCHEMA " + schema + " TO postgres;");
        ctx.writeDDL("--GRANT ALL ON SCHEMA " + schema + " TO public;");
    }
    
    public void extractSchemaTablesDDL() throws SQLException {
        String tableName;
        while ((tableName = ctx.getJob()) != null) {
            extractTableDDL(ctx.getOwner(), tableName);
            if (ctx.isTransferRows()) {
                transferData(ctx.getOwner(), tableName);
            }
        }
    }
    
    public Set<String> getSchemaTables() throws SQLException {
        Set<String> tables = new TreeSet<>();

        try (PreparedStatement pstmt = srcConnection.prepareStatement(
                          "SELECT table_name "
                        + "FROM all_tables "
                        + "WHERE owner = ?")) {
            pstmt.setString(1, ctx.getOwner());
            // table of Schema
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    tables.add(rs.getString(1));
                }
            }
        }
        
        return tables;
    }

    /**
     * Transfer table data
     * @author Alexey Novikov <anovikov9004 at inbox.ru>
     */
    private void transferData(String owner, String tableName) {
        ctx.log("Transfer data for table " + owner + '.' + tableName);
        
        try (Statement srcStmt  = srcConnection.createStatement();) {
            // source select
            srcStmt.setFetchSize(ctx.getChunkSize());
            try (ResultSet rs = srcStmt.executeQuery("SELECT * FROM "  + owner + '.' + tableName)) {
                long rowCount;
                if (isLobField(rs.getMetaData())) {
                    rowCount = transferWithInsert(owner, tableName, rs);
                } else {
                    rowCount = transferWithCopyManager(owner, tableName, rs);
                }
                ctx.log(owner + '.' + tableName + " Copied " + rowCount + " rows");
            } 
        } catch (SQLException | IOException ex) {
            ctx.error("transferData for table " + owner + '.' + tableName + ": " + ex.getLocalizedMessage());
        }
    }

    private boolean isLobField(ResultSetMetaData metaData) throws SQLException {
        boolean isLobField = false;
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            int columnType = metaData.getColumnType(i);
            if (columnType == Types.BLOB || columnType == Types.CLOB) {
                isLobField = true;
                break;
            }
        }
        return isLobField;
    }

    private String createInsertSql(String owner, String tableName, ResultSetMetaData metaData) throws SQLException {
        StringBuilder sb = new StringBuilder("INSERT INTO " + owner + '.' + tableName + "(");
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            if (i > 1) sb.append(",");
            sb.append(metaData.getColumnName(i));
        }
        sb.append(") VALUES (");
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            if (i > 1) sb.append(",");
            sb.append('?');
        }
        sb.append(')');
        return sb.toString();
    }

    private long transferWithInsert(String owner, String tableName, final ResultSet rs) throws SQLException {
        ResultSetMetaData metaData = rs.getMetaData();
        String destSql = createInsertSql(owner, tableName, metaData);
        ctx.info("Using Insert SQL: {" + destSql + "}");
        long rowCount = 0;
        try (PreparedStatement pstmt = destConnection.prepareStatement(destSql);) {
            while (rs.next()) {
                if (ctx.getSampleRows() > 0 && rowCount >= ctx.getSampleRows()) break;
                // set parameters
                pstmt.clearParameters();
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    switch(metaData.getColumnType(i)) {
                        case Types.BLOB:
                            Blob srcBlob = rs.getBlob(i);
                            if (srcBlob != null) {
                                pstmt.setBinaryStream(i, srcBlob.getBinaryStream());
                            } else {
                                pstmt.setNull(i, Types.BINARY);
                            }
                            break;
                            
                        case Types.CLOB:
                            Clob srcClob = rs.getClob(i);
                            if (srcClob != null) {
                                pstmt.setCharacterStream(i, srcClob.getCharacterStream());
                            } else {
                                pstmt.setNull(i, Types.LONGVARCHAR);
                            }
                            break;
                            
                        case Types.NCLOB:
                            NClob srcNClob = rs.getNClob(i);
                            if (srcNClob != null) {
                                pstmt.setCharacterStream(i, srcNClob.getCharacterStream());
                            } else {
                                pstmt.setNull(i, Types.LONGVARCHAR);
                            }
                            break;
                            
                        default:
                            pstmt.setObject(i, rs.getObject(i));
                    }
                }
                // add record
                pstmt.addBatch();
                if (++rowCount % ctx.getChunkSize() == 0) {
                    // insert records
                    pstmt.executeBatch();
                }
            }
            pstmt.executeBatch(); // insert remaining records
        }
        return rowCount;
    }

    private long transferWithCopyManager(String owner, String tableName, final ResultSet rs) throws SQLException, IOException {
        ctx.info("Using Copy Manager");
        // target copy manager
        CopyManager copyMgr = new CopyManager((BaseConnection) destConnection);
        String destSql = "COPY " + owner + '.' + tableName + " FROM STDIN WITH DELIMITER ',' NULL 'null' CSV";
        ResultSetMetaData metaData = rs.getMetaData();
        StringBuilder csvBuffer = new StringBuilder();
        long rowCount = 0; 
        while (rs.next()) {
            if (ctx.getSampleRows() > 0 && rowCount >= ctx.getSampleRows()) break;
            // save record to csv buffer
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                if (i > 1) csvBuffer.append(',');
                String val = rs.getString(i);
                int valType = metaData.getColumnType(i);
                if (val != null &&
                        (valType == Types.VARCHAR || valType == Types.CHAR || valType == Types.CLOB
                        || valType == Types.NVARCHAR || valType == Types.NCHAR || valType == Types.NCLOB)) {
                    val = val.replaceAll("\"", "\"\""); // quoted quotes
                    csvBuffer.append('"').append(val).append('"');
                } else {
                    csvBuffer.append(val);
                }
            }
            csvBuffer.append('\n');
            
            if (++rowCount % ctx.getChunkSize() == 0) {
                // copy records
                csvBuffer.deleteCharAt(csvBuffer.length() - 1); // delete last eol
                copyMgr.copyIn(destSql, new StringReader(csvBuffer.toString()));
                
                // clear csv buffer
                csvBuffer = new StringBuilder();
            }
        }
        if (csvBuffer.length() > 0) {
            // copy remaining records
            csvBuffer.deleteCharAt(csvBuffer.length() - 1);
            copyMgr.copyIn(destSql, new StringReader(csvBuffer.toString()));
        }
        return rowCount;
    }
}
