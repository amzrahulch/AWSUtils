import java.sql.*;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
 
public class Redshifttest implements Runnable {
 
        static final String dbURL = "jdbc:redshift://test.c6bdgjcn5dr2.us-east-1.redshift.amazonaws.com:5439/test";
        static final String MasterUsername = "rahul";
        static final String pwd = "Rahul1234";
 
        static final String SQL = "SELECT col1,col2 FROM testredshift where col1 = ? and col2=?";
        int index = 0;
        int count = 10;
        long sleep = 10;
 
        public Redshifttest(int index) {
                this.index = index;
        }
 
        public Connection connect() throws SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException {
                System.out.println("Connecting to database...");
                Properties props = new Properties();
                // Uncomment the following line if using a keystore.
                // props.setProperty("ssl", "true");
                props.setProperty("user", MasterUsername);
                props.setProperty("password", pwd);
               return DriverManager.getConnection(dbURL, props);
               // return ((Driver)Class.forName("com.amazon.redshift.jdbc.Driver").newInstance()).connect(dbURL, props);
        }
 
        public void test(Connection conn) throws SQLException, InterruptedException {
              // try (PreparedStatement stmt = conn.prepareStatement(SQL)) {
                        for (int i = 0; i < count; i++) {
                                Thread.currentThread().sleep((long) (Math.random() * 1000));
                                long n = 1242517235 + index + i;
                                String s = Long.toString(n);
                                System.out.println("Executing " + Thread.currentThread().getName() + " for " + s);
                                long start = 0;
                                //synchronized (TestRedshift.class) {
                               PreparedStatement stmt = conn.prepareStatement(SQL);
                                        stmt.setString(1, s);
                                        stmt.setString(2, "casefile." + s + ".t2");
                                        System.out.println("Set parameters " + Thread.currentThread().getName());
                                        start = System.currentTimeMillis();
                                          try (ResultSet rs = stmt.executeQuery()) {
                                                while (rs.next()) {
                                                        System.out.println("id=" + rs.getString(1) + " dt=" + rs.getString(2));
                                                }
                                        }
                                
                                long end = System.currentTimeMillis();
                                long rt = (end - start) / 1000;
                                System.out.println("Done " + Thread.currentThread().getName() + " time = " + rt + "sec");
                                Thread.currentThread().sleep(sleep);
                        }
                }
                //    }
          //}
 
        @Override
        public void run() {
                // TODO Auto-generated method stub
                System.out.println("Started connectivity test." + Thread.currentThread().getName());
               try  (Connection conn = connect()) {
                        test(conn);
                } catch (Exception ex) {
                        // For convenience, handle all errors here.
                        System.out.println("Failed :" + Thread.currentThread().getName() + ex);
                        ex.printStackTrace();
                 } finally {
                        System.out.println("Finished connectivity test." + Thread.currentThread().getName());
                }
        }
 
        public static void main(String[] args) {
                // TODO Auto-generated method stub
                try {
                        Class.forName("com.amazon.redshift.jdbc.Driver");
                   //    Class.forName("org.postgresql.Driver");
                        int nT = 2;
                        ExecutorService pool = Executors.newFixedThreadPool(nT);
                        for (int i = 0; i < nT; i++) {
                                pool.execute(new Redshifttest(i));
                        }
                        pool.shutdown();
                        pool.awaitTermination(1000, TimeUnit.DAYS);
                } catch (Exception e) {
                        // TODO Auto-generated catch block
                        System.out.println("Failed main:" + e);
                        e.printStackTrace();
                }
 
        }
}