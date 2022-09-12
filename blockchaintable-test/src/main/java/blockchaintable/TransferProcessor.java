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

  PoolDataSource pds = PoolDataSourceFactory.getPoolDataSource();
  Connection connection = null;
  
  public TransferProcessor(Config config) throws SQLException{ 
    super(config);
    this.numAccounts = (int) config.getUserLong("blockchaintable_test", "num_accounts");

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

    pds.setInitialPoolSize(INITIAL_POOL_SIZE);
    pds.setMinPoolSize(MIN_POOL_SIZE);
    pds.setMaxPoolSize(MAX_POOL_SIZE);
    pds.setTimeoutCheckInterval(TIMEOUT_CHECK_INTERVAL);
    pds.setInactiveConnectionTimeout(INACTIVE_CONNECTION_TIMEOUT);
  }

  @Override
  public void executeEach() throws Exception{

    int fromId = ThreadLocalRandom.current().nextInt(numAccounts);
    int toId = ThreadLocalRandom.current().nextInt(numAccounts);
    int amount = ThreadLocalRandom.current().nextInt(1000) + 1;
    int operation_num = ThreadLocalRandom.current().nextInt(5);

    while(true){
      //enumに変える。
      try {
        connection = pds.getConnection(); 
        connection.setAutoCommit(false);

        switch(operation_num){
          case 0:
            writeCheck(connection, fromId, amount);
            break;
          case 1:
            sendPayment(connection, fromId, toId, amount);
            break;
          case 2:
            transactSavings(connection, fromId, amount);
            break;
          case 3:
            depositChecking(connection, fromId, amount);
            break;
          case 4:
            amalgamate(connection, fromId, toId);
            break;
        }
        break;
        //sendPayment(connection, fromId, toId, amount);
        //transactSavings(connection, fromId, amount);
        //depositChecking(connection, fromId, amount);
        //writeCheck(connection, fromId, amount);
        //amalgamate(connection, fromId, toId);
      } catch (SQLException e) {
        e.printStackTrace();
      } catch(Exception e){
          e.printStackTrace();
          connection.rollback();
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

  }

  @Override
  public void close() {
  }

  public void transactSavings(Connection connection, int account, int amount) throws Exception {
    try {

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
      System.out.println("TransactSavings: " + account + " savings -> " + (balance + amount));
    } catch (SQLException e) {
      connection.rollback();
    }
  }

  public void depositChecking(Connection connection, int account, int amount) throws Exception {
    if (amount < 0) {
      throw new Exception("Invalid amount");
    }

    try {

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
      System.out.println("DepositChecking: " + account + " checking -> " + (balance + amount));
    } catch (SQLException e) {
      connection.rollback();
    }
  }

  public void writeCheck(Connection connection, int account, int amount) throws Exception {
    try {

      PreparedStatement prepared =
          connection.prepareStatement(
              "SELECT balance FROM savings_test WHERE account = ? ORDER BY"
                  + " sequence_number DESC OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY");
      prepared.setInt(1, account);
      ResultSet result = prepared.executeQuery();

      int savings = 0;
      while (result.next()) {
        savings = result.getInt(1);
      }

      prepared =
          connection.prepareStatement(
              "SELECT balance, sequence_number FROM checking_test WHERE account = ? ORDER BY"
                  + " sequence_number DESC OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY");
      prepared.setInt(1, account);
      result = prepared.executeQuery();

      int checking = 0;
      int sequenceNumber = 0;
      while (result.next()) {
        checking = result.getInt(1);
        sequenceNumber = result.getInt(2);
      }
      /* 
      if (savings + checking < amount) {
        throw new Exception("Insufficient balance");
      }
      */

      prepared =
          connection.prepareStatement(
              "INSERT INTO checking_test (account, balance, sequence_number) VALUES (?, ?, ?)");
      prepared.setInt(1, account);
      prepared.setInt(2, checking - amount);
      prepared.setInt(3, sequenceNumber + 1);
      prepared.execute();

      connection.commit();
      System.out.println("WriteCheck: " + account + " checking -> " + (checking - amount));
    } catch (SQLException e) {
      connection.rollback();
    }
  }

  public void sendPayment(Connection connection, int fromAccount, int toAccount, int amount) throws Exception {
    try {

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
      /* 
      if (balance1 < amount) {
        throw new Exception("Insufficient balance");
      }
      */

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
      System.out.println("SendPayment: " + fromAccount + " checking -> " + (balance1 - amount));
      System.out.println("SendPayment: " + toAccount + " checking -> " + (balance2 + amount));
    } catch (SQLException e) {
      connection.rollback();
    }
  }

  public void amalgamate(Connection connection, int fromAccount, int toAccount) throws Exception {
    try {


      PreparedStatement prepared =
          connection.prepareStatement(
              "SELECT balance, sequence_number FROM savings_test WHERE account = ? ORDER BY"
                  + " sequence_number DESC OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY");
      prepared.setInt(1, fromAccount);
      ResultSet result = prepared.executeQuery();

      int savings1 = 0;
      int sequenceNumberSavings1 = 0;
      while (result.next()) {
        savings1 = result.getInt(1);
        sequenceNumberSavings1 = result.getInt(2);
      }

      prepared =
          connection.prepareStatement(
              "SELECT balance, sequence_number FROM checking_test WHERE account = ? ORDER BY"
                  + " sequence_number DESC OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY");
      prepared.setInt(1, fromAccount);
      result = prepared.executeQuery();

      int checking1 = 0;
      int sequenceNumberChecking1 = 0;
      while (result.next()) {
        checking1 = result.getInt(1);
        sequenceNumberChecking1 = result.getInt(2);
      }

      prepared.clearParameters();
      prepared.setInt(1, toAccount);
      result = prepared.executeQuery();

      int checking2 = 0;
      int sequenceNumberChecking2 = 0;
      while (result.next()) {
        checking2 = result.getInt(1);
        sequenceNumberChecking2 = result.getInt(2);
      }

      prepared =
          connection.prepareStatement(
              "INSERT INTO savings_test (account, balance, sequence_number) VALUES (?, ?, ?)");

      prepared.setInt(1, fromAccount);
      prepared.setInt(2, 0);
      prepared.setInt(3, sequenceNumberSavings1 + 1);
      prepared.execute();

      prepared =
          connection.prepareStatement(
              "INSERT INTO checking_test (account, balance, sequence_number) VALUES (?, ?, ?)");
      prepared.clearParameters();
      prepared.setInt(1, fromAccount);
      prepared.setInt(2, 0);
      prepared.setInt(3, sequenceNumberChecking1 + 1);
      prepared.execute();

      prepared.clearParameters();
      prepared.setInt(1, toAccount);
      prepared.setInt(2, savings1 + checking1 + checking2);
      prepared.setInt(3, sequenceNumberChecking2 + 1);
      prepared.execute();

      connection.commit();
      System.out.println("Amalgamate: " + fromAccount + " savings -> 0");
      System.out.println("Amalgamate: " + fromAccount + " checking -> 0");
      System.out.println(
          "Amalgamate: " + toAccount + " checking -> " + (savings1 + checking1 + checking2)); 
    } catch (SQLException e) {
      connection.rollback();
    }
  }




}



