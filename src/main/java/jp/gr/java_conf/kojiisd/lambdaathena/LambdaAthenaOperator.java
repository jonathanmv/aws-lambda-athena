package jp.gr.java_conf.kojiisd.lambdaathena;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import jp.gr.java_conf.kojiisd.lambdaathena.dto.Request;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.lang.RuntimeException;

/**
 * Accessing to operate Athena
 *
 * @author kojiisd
 */
public class LambdaAthenaOperator implements RequestHandler<Request, Object> {
    private Map<String, String> regionMap = new HashMap();

    public LambdaAthenaOperator() {
        this.regionMap.put("us-east-1", "us-east-1");
        this.regionMap.put("us-west-2", "us-west-2");
    }

    private void validateRequest(Request request) throws RuntimeException {
      boolean isNotNull = request != null;
      boolean hasRegion = !StringUtils.isBlank(request.region);
      boolean hasS3Path = !StringUtils.isBlank(request.s3Path);
      boolean hasSql = !StringUtils.isBlank(request.sql);
      boolean hasColumns = !StringUtils.isBlank(request.columnListStr);

      boolean isValid = isNotNull && hasRegion && hasS3Path && hasSql && hasColumns;
      if (!isValid) {
        throw new RuntimeException("400. Invalid Request");
      }
    }

    private String getConnectionUrl(String region) {
      String athenaUrl = "jdbc:awsathena://athena." + region + ".amazonaws.com:443";
      return athenaUrl;
    }

    private Properties getConnectionProperties(String s3OutputPath) {
      Properties properties = new Properties();
      properties.put("s3_staging_dir", s3OutputPath);
      properties.put("aws_credentials_provider_class", "com.amazonaws.auth.PropertiesFileCredentialsProvider");
      properties.put("aws_credentials_provider_arguments", "config/credential");

      return properties;
    }

    public Object handleRequest(Request request, Context context) {
        LambdaLogger logger = context.getLogger();
        Connection connection = null;
        Statement statement = null;
        ResultSet rs = null;
        StringBuilder results = new StringBuilder();
        try {
            this.validateRequest(request);
            logger.log("Request is valid");

            connection = this.getConnectionFromRequest(request);
            statement = connection.createStatement();
            rs = statement.executeQuery(request.sql);
            ResultSetMetaData rsmd = rs.getMetaData();
            int numberOfColumns = rsmd.getColumnCount();
            while (rs.next()) {
                StringBuilder row = new StringBuilder();
                for (int column = 1; column <= numberOfColumns; column++) {
                  row.append(rs.getString(column));
                  if (column < numberOfColumns) {
                    row.append(",");
                  }
                }
                row.append(System.getProperty("line.separator"));
                results.append(row.toString());
            }
            rs.close();
            connection.close();
        } catch (Exception ex) {
            ex.printStackTrace();

            return "Exception happened, aborted.";
        } finally {
            try {
                if (statement != null)
                    statement.close();
            } catch (Exception ex) {

            }
            try {
                if (connection != null)
                    connection.close();
            } catch (Exception ex) {

                ex.printStackTrace();
            }
        }

        String result = results.toString();
        logger.log(result);
        return result;
    }

    private Connection getConnectionFromRequest(Request request) throws RuntimeException {
        try {
            Class.forName("com.amazonaws.athena.jdbc.AthenaDriver");

            String athenaUrl = this.getConnectionUrl(request.region);
            String s3OutputPath = request.s3Path;
            Properties properties = this.getConnectionProperties(s3OutputPath);

            return DriverManager.getConnection(athenaUrl, properties);
        } catch (Exception ex) {
            throw new RuntimeException("Couldn't open connection", ex);
        }
    }

}
