package blockchaintable;

import com.scalar.kelpie.config.Config;
import com.scalar.kelpie.modules.TimeBasedProcessor;
import oracle.ucp.jdbc.PoolDataSource;
import oracle.ucp.jdbc.PoolDataSourceFactory;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ThreadLocalRandom;


public class TransferProcessor extends TimeBasedProcessor {

  private final int numAccounts;

  static final String DB_URL = "jdbc:oracle:thin:@//localhost/XEPDB1";
  static final String DB_USER = "scalar";
  static final String DB_PASSWORD = "scalar";
  static final String CONN_FACTORY_CLASS_NAME = "oracle.jdbc.pool.OracleDataSource";

  PoolDataSource pds = PoolDataSourceFactory.getPoolDataSource();
  Connection connection = null;
  
  public TransferProcessor(Config config) throws SQLException{ 
    super(config);
    this.numAccounts = (int) config.getUserLong("blockchaintable_test", "num_accounts");

    pds.setConnectionFactoryClassName(CONN_FACTORY_CLASS_NAME);
    pds.setURL(DB_URL);
    pds.setUser(DB_USER);
    pds.setPassword(DB_PASSWORD);
  
    pds.setInitialPoolSize(5);
    pds.setMinPoolSize(5);
    pds.setMaxPoolSize(20);
    pds.setTimeoutCheckInterval(5);
    pds.setInactiveConnectionTimeout(10);
  }

  @Override
  public void executeEach() throws Exception{

    int fromId = ThreadLocalRandom.current().nextInt(numAccounts);
    int toId = ThreadLocalRandom.current().nextInt(numAccounts);
    int amount = ThreadLocalRandom.current().nextInt(1000) + 1;

      try {
        connection = pds.getConnection();
        try{
          sendPayment(connection, fromId, toId, amount);
        } catch(Exception e){
          connection.rollback();
        } 
      } catch (SQLException e) {
        e.printStackTrace();
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

  }

  @Override
  public void close() {
    
  }

  public void sendPayment(Connection connection, int fromAccount, int toAccount, int amount) throws Exception {
    try {
      connection.setAutoCommit(false);

      PreparedStatement prepared =
          connection.prepareStatement(
              "SELECT balance, sequence_number FROM checking_test WHERE account = ? ORDER BY"
                  + " sequence_number DESC OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY");
      prepared.setInt(1, fromAccount);
      ResultSet result = prepared.executeQuery();

      int balance1 = 0;
      int sequenceNumber1 = 0;
      while (result.next()) {
        balance1 = result.getInt(1);
        sequenceNumber1 = result.getInt(2);
      }

      if (balance1 < amount) {
        throw new Exception("Insufficient balance");
      }

      prepared.clearParameters();
      prepared.setInt(1, toAccount);
      result = prepared.executeQuery();

      int balance2 = 0;
      int sequenceNumber2 = 0;
      while (result.next()) {
        balance2 = result.getInt(1);
        sequenceNumber2 = result.getInt(2);
      }

      prepared =
          connection.prepareStatement(
              "INSERT INTO checking_test (account, balance, sequence_number) VALUES (?, ?, ?)");

      prepared.setInt(1, fromAccount);
      prepared.setInt(2, balance1 - amount);
      prepared.setInt(3, sequenceNumber1 + 1);
      prepared.execute();

      prepared.clearParameters();
      prepared.setInt(1, toAccount);
      prepared.setInt(2, balance2 + amount);
      prepared.setInt(3, sequenceNumber2 + 1);
      prepared.execute();

      connection.commit();
      //System.out.println("SendPayment: " + fromAccount + " checking -> " + (balance1 - amount));
      //System.out.println("SendPayment: " + toAccount + " checking -> " + (balance2 + amount));
    } catch (SQLException e) {
      connection.rollback();
    }
  }




}



