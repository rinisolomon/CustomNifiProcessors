package JobControl.processors.GetBatchId;

/**
 * Created by solor031 on 2/28/17.
 */

import org.apache.nifi.annotation.behavior.InputRequirement;
        import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.FlowFileFilter;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.logging.ComponentLog;

import org.apache.nifi.util.StopWatch;
import org.apache.nifi.stream.io.StreamUtils;


import javax.xml.bind.DatatypeConverter;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;

@SupportsBatching
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"sql", "get", "batch id", "database",  "select", "relational"})
@CapabilityDescription("Queries the Job Control Table and returns the batch id to be associated with a job. ")

@WritesAttributes({
        @WritesAttribute(attribute = "batch.id", description = "The Batch id for the job")
})
public class GetBatchId extends AbstractProcessor {

    static final PropertyDescriptor CONNECTION_POOL = new PropertyDescriptor.Builder()
            .name("JDBC Connection Pool ")
            .description("Specifies the JDBC Connection Pool to use to query the JobControl table")
            .identifiesControllerService(DBCPService.class)
            .required(true)
            .build();
   /* static final PropertyDescriptor CONNECTION_POOL_NO_FASTLOAD = new PropertyDescriptor.Builder()
            .name("ptdfl-connection-no-fl")
            .displayName("JDBC Connection Non-FastLoad")
            .description("Specifies the JDBC Connection Pool to use for Non-FastLoad operations (create/drop table).")
            .identifiesControllerService(DBCPService.class)
            .required(true)
            .build();*/
    static final PropertyDescriptor SQL_QUERY= new PropertyDescriptor.Builder()
            .name("SQl Ouery")
            .description("The query to get the batch id ")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("A FlowFile is routed to this relationship after the database is successfully updated")
            .build();
    static final Relationship REL_RETRY = new Relationship.Builder()
            .name("retry")
            .description("A FlowFile is routed to this relationship if the database cannot be updated but attempting the operation again may succeed")
            .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile is routed to this relationship if the database cannot be updated and retrying the operation will also fail, "
                    + "such as an invalid query or an integrity constraint violation")
            .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(CONNECTION_POOL);
        properties.add(SQL_QUERY);


        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> rels = new HashSet<>();
        rels.add(REL_SUCCESS);
        rels.add(REL_RETRY);
        rels.add(REL_FAILURE);
        return rels;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        final ComponentLog logger = getLogger();
        FlowFile fileToProcess = null;
        if (flowFile == null) {
            return;
        }

        final long startNanos = System.nanoTime();

       // final String targetTable = context.getProperty(TARGET_TABLE).evaluateAttributeExpressions(flowFile).getValue();
        final String query = context.getProperty(SQL_QUERY).evaluateAttributeExpressions(flowFile).getValue();
        int batchid =0;



        //save temp table name to attribute


        final DBCPService dbcpService = context.getProperty(CONNECTION_POOL).asControllerService(DBCPService.class);
       // final DBCPService dbcpServiceNoFL = context.getProperty(CONNECTION_POOL_NO_FASTLOAD).asControllerService(DBCPService.class);


        try (final Connection conn = dbcpService.getConnection()) {
            flowFile = session.putAttribute(flowFile, "batch.id",  getBatchId( conn,query));

              final  FlowFile finalFlowFile = flowFile;
                session.read(finalFlowFile, new InputStreamCallback() {
                    @Override
                    public void process(final InputStream in) throws IOException {
                        try {
                            // session.putAttribute(flowFile, "batch.id", getBatchId(conn, query));


                        } catch (Exception e) {
                            throw new ProcessException("Failed to get Batch id" + e.getMessage(), e);
                        }
                    }
                });


                // Determine the database URL
                String url = "jdbc://unknown-host";
                try {
                    url = conn.getMetaData().getURL();
                } catch (final SQLException sqle) {
                }

                // Emit a Provenance SEND event
                final long transmissionMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
                session.getProvenanceReporter().send(finalFlowFile, url, transmissionMillis, true);
                session.transfer(flowFile, REL_SUCCESS);
            } catch (SQLException e1) {
                session.transfer(flowFile, REL_FAILURE);
                getLogger().error("Failed to process TD FastLoadCSV.", e1);
            }

    }



    private String getBatchId( Connection conn, final String query) throws SQLException {
        conn.setAutoCommit(false);
        int batchid =0;
        Statement stmt = conn.createStatement();

        //final PreparedStatement pstmtFld = conn.prepareStatement('select MAX(ETL_TARGET_REC_SET_ID) from  SB_DMI_DB.JOB_CONTROL;');
       // ResultSet cols = conn;
        ResultSet rs = stmt.executeQuery("select MAX(ETL_TARGET_REC_SET_ID) from  SB_DMI_DB.JOB_CONTROL; ");

        if(rs.next())
            batchid =rs.getInt(1)+2;
        stmt.executeUpdate("insert into SB_DMI_DB.JOB_CONTROL (etl_target_rec_set_id ,etl_row_id,etl_source, etl_target_table, etl_start_time , etl_end_time) values("+
        batchid+",0,'WEBAPI','SBI_DMI_DB.S_DIM_ADDRESS_TEST_NEW','2017-02-27 15:53:20.460000','2017-02-27 15:53:20.460000');");
        stmt.close();
        conn.commit();
        return String.valueOf(batchid) ;

    }






}
