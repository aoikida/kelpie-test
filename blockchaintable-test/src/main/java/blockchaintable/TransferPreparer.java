package blockchaintable;

import com.scalar.kelpie.config.Config;
import com.scalar.kelpie.modules.PreProcessor;
import oracle.ucp.jdbc.PoolDataSource;
import oracle.ucp.jdbc.PoolDataSourceFactory;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;



public class TransferPreparer extends PreProcessor {

  

  PoolDataSource pds = PoolDataSourceFactory.getPoolDataSource();
  Connection connection = null;
  
  public TransferPreparer(Config config) throws Exception{ 
    super(config);

    String DB_URL = config.getUserString("blockchaintable_test", "url");
    String DB_USER = config.getUserString("blockchaintable_test", "user");
    String DB_PASSWORD = config.getUserString("blockchaintable_test", "password");
    String CONN_FACTORY_CLASS_NAME = config.getUserString("blockchaintable_test", "conn_factory_class_name");
    int INITIAL_POOL_SIZE = (int) config.getUserLong("blockchaintable_test", "initial_pool_size");
    int MIN_POOL_SIZE = (int) config.getUserLong("blockchaintable_test", "min_pool_size");
    int MAX_POOL_SIZE = (int) config.getUserLong("blockchaintable_test", "max_pool_size");
    int TIMEOUT_CHECK_INTERVAL = 5;
    int INACTIVE_CONNECTION_TIMEOUT = 10;
    
    pds.setConnectionFactoryClassName(CONN_FACTORY_CLASS_NAME);
    pds.setURL(DB_URL);
    pds.setUser(DB_USER);
    pds.setPassword(DB_PASSWORD);
    pds.setConnectionPoolName("JDBC_UCP_POOL");

    pds.setInitialPoolSize(INITIAL_POOL_SIZE);
    pds.setMinPoolSize(MIN_POOL_SIZE);
    pds.setMaxPoolSize(MAX_POOL_SIZE);
    pds.setTimeoutCheckInterval(TIMEOUT_CHECK_INTERVAL);
    pds.setInactiveConnectionTimeout(INACTIVE_CONNECTION_TIMEOUT);

    connection = pds.getConnection();

    try {
      logInfo("create Table");
      createBank(connection);
    } catch (SQLException e) {
    // ignore if table already exists
    } finally{
      connection.close();
    }

  }

  @Override
  public void execute() {

    logInfo("insert initial values... ");

    try {
      connection = pds.getConnection();
      for (int i = 0; i < 1000; i++) {
        //transactSavings(connection, i, 500);
        //depositChecking(connection, i, 500);
      } 
    } catch (SQLException e) {
      e.printStackTrace();
    } catch (Exception e){
      try {
        connection.rollback();
      } catch (SQLException e1) {
        // TODO Auto-generated catch block
        e1.printStackTrace();
      }
      logWarn("table creation error");
    } finally {
        try {
          if (connection != null) {
            // データベースを切断
            connection.close();
          }
      } catch (SQLException e) {
          e.printStackTrace();
      }
    }
    logInfo("All records are inserted ");
  }

  @Override
  public void close() {

  }


  public void createBank(Connection connection) throws SQLException {
    connection.setAutoCommit(false);
  
    Statement statement = connection.createStatement();
  
    statement.execute(
        "CREATE BLOCKCHAIN TABLE savings_test(account number, balance number, sequence_number number) NO"
            + " DROP NO DELETE HASHING USING \"SHA2_512\" VERSION \"v1\"");
    statement.execute(
        "CREATE BLOCKCHAIN TABLE checking_test(account number, balance number, sequence_number number)"
            + " NO DROP NO DELETE HASHING USING \"SHA2_512\" VERSION \"v1\"");

  }

  public void transactSavings(Connection connection, int account, int amount) throws Exception {
    try {
      connection.setAutoCommit(false);

      PreparedStatement prepared =
          connection.prepareStatement(
              "SELECT balance, sequence_number FROM savings_test WHERE account = ? ORDER BY"
                  + " sequence_number DESC OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY");
      prepared.setInt(1, account);
      ResultSet result = prepared.executeQuery();

      int balance = 0;
      int sequenceNumber = 0;
      while (result.next()) {
        balance = result.getInt(1);
        sequenceNumber = result.getInt(2);
      }

      if (balance + amount < 0) {
        throw new Exception("Insufficient balance");
      }

      prepared =
          connection.prepareStatement(
              "INSERT INTO savings_test (account, balance, sequence_number) VALUES (?, ?, ?)");
      prepared.setInt(1, account);
      prepared.setInt(2, balance + amount);
      prepared.setInt(3, sequenceNumber + 1);
      prepared.execute();

      connection.commit();
      //System.out.println("TransactSavings: " + account + " savings -> " + (balance + amount));
    } catch (SQLException e) {
      connection.rollback();
    }
  }

  public void depositChecking(Connection connection, int account, int amount) throws Exception {
    if (amount < 0) {
      throw new Exception("Invalid amount");
    }

    try {
      connection.setAutoCommit(false);

      PreparedStatement prepared =
          connection.prepareStatement(
              "SELECT balance, sequence_number FROM checking_test WHERE account = ? ORDER BY"
                  + " sequence_number DESC OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY");
      prepared.setInt(1, account);
      ResultSet result = prepared.executeQuery();

      int balance = 0;
      int sequenceNumber = 0;

      while (result.next()) {
        balance = result.getInt(1);
        sequenceNumber = result.getInt(2);
      }

      prepared =
          connection.prepareStatement(
              "INSERT INTO checking_test (account, balance, sequence_number) VALUES (?, ?, ?)");
      prepared.setInt(1, account);
      prepared.setInt(2, balance + amount);
      prepared.setInt(3, sequenceNumber + 1);
      prepared.execute();

      connection.commit();
      //System.out.println("DepositChecking: " + account + " checking -> " + (balance + amount));
    } catch (SQLException e) {
      connection.rollback();
    }
  }


}