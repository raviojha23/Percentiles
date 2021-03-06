import java.sql.Timestamp;
import java.util.Date;
import java.util.Random;
import java.util.ArrayList;
import java.util.Vector;
import java.util.List;
import java.sql.*;

public class Percentile{
	public static List messages = new ArrayList();
	//public static Vector messages = new Vector(3,2);
	public Connection getConnection(){
		Connection conn = null;
		try{
			DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
			String dbUrl = "jdbc:oracle:thin:@192.168.140.62:1521:orcl";
			conn = DriverManager.getConnection(dbUrl, "c##perf","c##perf");
			conn.setAutoCommit(false);
		}
		catch(Exception e){
			e.printStackTrace();
		}
		return conn;
	}
	
	public static void main(String []args){
		try{
			if(args.length == 3){
				//Thread tFeeder = new Thread(new Feeder(messages,Long.parseLong(args[0]),Integer.parseInt(args[2])));
				//tFeeder.start();
				Percentile objPercentile = new Percentile();
				
				Feeder objFeeder = new Feeder(messages,Long.parseLong(args[0]),Integer.parseInt(args[2]));
				objFeeder.runner();
				
				Thread tReader = new Thread(new Reader(messages,Integer.parseInt(args[1])));
				tReader.start();
				Thread tCalculate = new Thread(new Calculate());
				tCalculate.start();
				
				
				Thread tCalculateMedian = new Thread(new CalculateMedian());
				tCalculateMedian.start();
				
				Thread tCalculateVariance = new Thread(new CalculateVariance());
				tCalculateVariance.start();
				
				Thread tCalculatestddev = new Thread(new Calculatestddev());
				tCalculatestddev.start();
				/**/
				
				//tFeeder.join();
				tReader.join();
				tCalculate.join();
				tCalculateMedian.join();
				tCalculateVariance.join();
				tCalculatestddev.join();/**/
			}
			else{
				System.out.println("CORRECT USAGE: java -cp . Percentile <injectionRate> <batchSize> <runtime>");
			}
		}
		catch(Exception e){
			System.out.println("Error from Percentile");
		}
		//Feeder objFeeder = new Feeder(messages);
		//objFeeder.start();
	}

}

class Feeder extends Thread{
    int serialNo;
	//StringBuffer message;
	String message;
	String FS;
	public List messages = new ArrayList();
	//public Vector messages = new Vector(3,2);
	Date date;
	double value;
	Random rnd = new Random();
	long injectionRate;
	int endTime;
	
	public Feeder(List messages,long injectionRate, int endTime){
		//message = new StringBuffer();
		message = "";
		serialNo = 1;
		FS = "|";
		this.messages = messages;
		this.injectionRate = injectionRate;
		this.endTime = endTime;
	}

	public void runner(){
		try{
			boolean flag = true;
			long start = System.currentTimeMillis();
			long Time = start + endTime*60*1000;
			//long delta=0;
			
			while(System.currentTimeMillis() < Time){
				//while(flag){
					for(int i=0;i<50000;i++){
						int randomInt = rnd.nextInt(1000000);
						double randomDouble = Math.random();
						date = new Date();
						Timestamp st = new java.sql.Timestamp(date.getTime());
						value = randomDouble*randomInt;
						//message.append(serialNo).append(FS).append(new java.sql.Timestamp(date.getTime())).append(FS).append(randomDouble*randomInt);
						//message = serialNo+FS+st+FS+value;
						message = serialNo+FS+value;
						//serialNo++;
						//delta++;
						if(message!=null && message.length()>0){
							messages.add(message);
							serialNo++;
						}
						//if(message.length()>0)
							//message.delete(0,message.length());
						//if(serialNo%10000==0)
							//System.out.println(message+"Feeder");
					}
					System.out.println(serialNo);
					//Thread.sleep(900);
					if(injectionRate == 100000)                          //Given that 100000 msgs take 200 ms
						Thread.sleep(770);
					if(injectionRate == 200000)
						Thread.sleep(270);
					if(injectionRate == 300000)
						Thread.sleep(100);
					
				//}
			}
			long end = System.currentTimeMillis();
			System.out.println("Runtime of Feeder thread for "+serialNo+" messages is "+(end-start));
		
		}
		catch(Exception e){
			System.out.println("Error from Feeder");
			e.printStackTrace();
		}
	}
}

class Reader extends Thread{

	private List messages = new ArrayList();
	//private Vector messages = new Vector(3,2);
	int batchSize;
	public Reader(List message,int batchSize){
		this.messages = message;
		this.batchSize = batchSize;
	}
	
	public void run(){
		boolean flag = true;
		//int i =0;
		//int batchSize = 5000;
		String[] msgs ;
		try{
			
			Percentile objPercentile = new Percentile();
			Connection conn = objPercentile.getConnection();
						
			String Query = "";
			//Statement statement = conn.createStatement();
			//statement.executeQuery("ALTER SESSION FORCE PARALLEL DML");
			Date date ;
			PreparedStatement pst = null;
			//pst = conn.prepareStatement("insert /*+ append */ into utable (id, value)  values (?,?)");
			pst = conn.prepareStatement("insert into utable (id, value)  values (?,?)");
			int j = 0;
			while(flag){
				int i = 0;
				//long DBstart = System.currentTimeMillis();
				for(i =0 ;i<batchSize ;i++){
					//long DBstart = System.currentTimeMillis();
					if(!messages.isEmpty() && j<messages.size()){
					//for( i = 0;i< messages.size();i++){
						//String msg = (String)messages.remove(0);
						String msg = (String)messages.get(j);
						//System.out.println(msg);
						if(msg == null)
							continue;
						msgs = msg.split("\\|");
						date = new Date();
						//System.out.println(msgs[0]+" "+msgs[1]+" "+msgs[2]+" ");
						try {
							pst.setInt(1,Integer.parseInt(msgs[0]));
							//pst.setTimestamp(2, new java.sql.Timestamp(date.getTime()));
							pst.setDouble(2,Double.parseDouble(msgs[1]));
							pst.addBatch();
						} catch (Exception e) {
							e.printStackTrace();
							flag = true;
							continue;
							
						}				
						j++;
					//}
					}
					else{
						Thread.sleep(100);	
					}
					//long DBend = System.currentTimeMillis();
					//System.out.println("Time for DB stat "+(DBend-DBstart));
				}
				
				pst.executeBatch();
				conn.commit();
				//long DBend = System.currentTimeMillis();
				//System.out.println("Time for DB stat "+(DBend-DBstart));
			}
			
				conn.close();
		}
		catch(Exception e){
			e.printStackTrace();
			
			System.out.println("Error from Reader");
		}
	}
}


class Calculate extends Thread{

	private List messages = new ArrayList();
	public Calculate(){
		
	}
	
	public void run(){
		try{
			
			Percentile objPercentile = new Percentile();
			Connection conn = objPercentile.getConnection();
			Date date = new Date();
			List uniqueRows = new ArrayList();
			
			//Statement statement = conn.createStatement();
			//statement.executeQuery("ALTER SESSION SET ISOLATION_LEVEL READ COMMITTED");
			
			String sql = "select /*+Parallel(10)*/ ' ',  count(value), PERCENTILE_CONT(0.9) within group (order by value desc) from utable";
			//String sql = "select count(value),PERCENTILE_CONT(0.9) within group (order by value desc) from utable";
			//String sql = "select max(ID),PERCENTILE_CONT(0.9) within group (order by value desc) from utable";
				
			PreparedStatement preStatement = conn.prepareStatement(sql);
			ResultSet result;
			boolean flag = true;
			int serialNo = 0 ;
			while(flag){
				
				//Thread.sleep(5);
				//long PercentileStart = System.currentTimeMillis();
				result = preStatement.executeQuery();
				
				//System.out.println("Here"+result.next());
				while(result.next()){
					serialNo = result.getInt(2);
					double percentile = result.getDouble(3);
					
					if(!uniqueRows.contains(serialNo)){
						System.out.println("Index @ " + serialNo+" Percentile is "+percentile);
						insertInDB(serialNo,percentile);
						uniqueRows.add(serialNo);
					}	
				}
				//long PercentileEnd = System.currentTimeMillis();
				//System.out.println("Time for DB stat of  "+serialNo+" rows is "+(PercentileStart-PercentileEnd));
			
			}
			
			conn.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	
	}
	public void insertInDB(int serialNo,double percentile){
		Percentile objPercentile = new Percentile();
		Date date = new Date();
				
		PreparedStatement pstInsert = null;
		try{
			
			Connection conn = objPercentile.getConnection();
			if(conn==null)
				System.out.println("conn is null");
			pstInsert = conn.prepareStatement("insert into vtable (id, ts, value)  values (?,?,?)");
			if(pstInsert==null)
				System.out.println("pstInsert is null");
			pstInsert.setInt(1,serialNo);
			pstInsert.setTimestamp(2, new Timestamp(date.getTime()));
			pstInsert.setDouble(3,percentile);
			pstInsert.executeUpdate();
			
			pstInsert.close();
			conn.close();
			
		}catch(Exception e){
			e.printStackTrace();
			System.out.println("serial no is "+serialNo+" percentile is "+percentile+ "Time is "+new Timestamp(date.getTime()));
		}
	}
}


class CalculateMedian extends Thread{

	private List messages = new ArrayList();
	public CalculateMedian(){
		
	}
	
	public void run(){
		try{
			
			Percentile objPercentile = new Percentile();
			Connection conn = objPercentile.getConnection();
			Date date = new Date();
			List uniqueRows = new ArrayList();
			String sql = "select /*+ Parallel(10) */ ' ' , count(value), median(value) from utable";
			//String sql = "select count(value),PERCENTILE_CONT(0.9) within group (order by value desc) from utable";
			//String sql = "select max(ID),PERCENTILE_CONT(0.9) within group (order by value desc) from utable";
				
			PreparedStatement preStatement = conn.prepareStatement(sql);
			ResultSet result;
			boolean flag = true;
			int serialNo = 0 ;
			double med = 0;
			while(flag){
				
				//Thread.sleep(5);
				//long PercentileStart = System.currentTimeMillis();
				result = preStatement.executeQuery();
				
				//System.out.println("Here"+result.next());
				while(result.next()){
					serialNo = result.getInt(2);
					med = result.getDouble(3);
					
					if(!uniqueRows.contains(serialNo)){
						insertInDB(serialNo,med);
						System.out.println("Index @ " + serialNo+" Median is "+med);
						uniqueRows.add(serialNo);
					}	
				}
				//long PercentileEnd = System.currentTimeMillis();
				//System.out.println("Time for DB stat of  "+serialNo+" rows is "+(PercentileStart-PercentileEnd));
			
			}
			
			conn.close();
		}
		catch(Exception e){
			e.printStackTrace();
		}
	
	}
	
	public void insertInDB(int serialNo,double med){
		Percentile objPercentile = new Percentile();
		Date date = new Date();
				
		PreparedStatement pstInsert = null;
		try{
			
			Connection conn = objPercentile.getConnection();
			pstInsert = conn.prepareStatement("insert into MEDIAN (id, ts, value)  values (?,?,?)");
			pstInsert.setInt(1,serialNo);
			pstInsert.setTimestamp(2, new Timestamp(date.getTime()));
			pstInsert.setDouble(3,med);
			pstInsert.executeUpdate();
			
			pstInsert.close();
			conn.close();
			
		}catch(Exception e){
			e.printStackTrace();
			//insertInDB(serialNo,med);
			System.out.println("serial no is "+serialNo+" med is "+med+ "Time is "+new Timestamp(date.getTime()));
		}
	}
	
}


class CalculateVariance extends Thread{

	private List messages = new ArrayList();
	public CalculateVariance(){
		
	}
	
	public void run(){
		try{
			
			Percentile objPercentile = new Percentile();
			Connection conn = objPercentile.getConnection();
			Date date = new Date();
			List uniqueRows = new ArrayList();
			String sql = "select /*+ Parallel(10) */ ' ' , count(value), variance(value) from utable";
			//String sql = "select count(value),PERCENTILE_CONT(0.9) within group (order by value desc) from utable";
			//String sql = "select max(ID),PERCENTILE_CONT(0.9) within group (order by value desc) from utable";
				
			PreparedStatement preStatement = conn.prepareStatement(sql);
			ResultSet result;
			boolean flag = true;
			int serialNo = 0 ;
			while(flag){
				
				//Thread.sleep(5);
				//long PercentileStart = System.currentTimeMillis();
				result = preStatement.executeQuery();
				
				//System.out.println("Here"+result.next());
				while(result.next()){
					serialNo = result.getInt(2);
					double variance = result.getDouble(3);
					
					if(!uniqueRows.contains(serialNo)){
						insertInDB(serialNo,variance);
						System.out.println("Index @ " + serialNo+" Variance is "+variance);
						uniqueRows.add(serialNo);
					}	
				}
				//long PercentileEnd = System.currentTimeMillis();
				//System.out.println("Time for DB stat of  "+serialNo+" rows is "+(PercentileStart-PercentileEnd));
			
			}
			
			conn.close();
		}
		catch(Exception e){
		e.printStackTrace();
		}
	
	}
	
	public void insertInDB(int serialNo,double variance){
		Percentile objPercentile = new Percentile();
		Date date = new Date();
				
		PreparedStatement pstInsert = null;
		try{
			
			Connection conn = objPercentile.getConnection();
			pstInsert = conn.prepareStatement("insert into variance (id, ts, value)  values (?,?,?)");
			pstInsert.setInt(1,serialNo);
			pstInsert.setTimestamp(2, new Timestamp(date.getTime()));
			pstInsert.setDouble(3,variance);
			pstInsert.executeUpdate();
			
			pstInsert.close();
			conn.close();
			
		}catch(Exception e){
			e.printStackTrace();
			System.out.println("serial no is "+serialNo+" variance is "+variance+ "Time is "+new Timestamp(date.getTime()));
		}
	}
}


class Calculatestddev extends Thread{

	private List messages = new ArrayList();
	public Calculatestddev(){
		
	}
	
	public void run(){
		try{
			
			Percentile objPercentile = new Percentile();
			Connection conn = objPercentile.getConnection();
			Date date = new Date();
			List uniqueRows = new ArrayList();
			String sql = "select /*+ Parallel(10) */ ' ' , count(value), stddev(value) from utable";
			//String sql = "select count(value),PERCENTILE_CONT(0.9) within group (order by value desc) from utable";
			//String sql = "select max(ID),PERCENTILE_CONT(0.9) within group (order by value desc) from utable";
				
			PreparedStatement preStatement = conn.prepareStatement(sql);
			ResultSet result;
			boolean flag = true;
			int serialNo = 0 ;
			while(flag){
				
				//Thread.sleep(5);
				long PercentileStart = System.currentTimeMillis();
				result = preStatement.executeQuery();
				
				//System.out.println("Here"+result.next());
				while(result.next()){
					serialNo = result.getInt(2);
					double dev = result.getDouble(3);
					
					if(!uniqueRows.contains(serialNo)){
						insertInDB(serialNo,dev);
						System.out.println("Index @ " + serialNo+" Standard deviation is "+dev);
						uniqueRows.add(serialNo);
					}	
				}
				long PercentileEnd = System.currentTimeMillis();
				//System.out.println("Time for DB stat of  "+serialNo+" rows is "+(PercentileStart-PercentileEnd));
			
			}
			
			conn.close();
		}
		catch(Exception e){
		e.printStackTrace();
		}
	
	}
	
	public void insertInDB(int serialNo,double dev){
		Percentile objPercentile = new Percentile();
		Date date = new Date();
				
		PreparedStatement pstInsert = null;
		try{
			
			Connection conn = objPercentile.getConnection();
			pstInsert = conn.prepareStatement("insert into STDDEV (id, ts, value)  values (?,?,?)");
			pstInsert.setInt(1,serialNo);
			pstInsert.setTimestamp(2, new Timestamp(date.getTime()));
			pstInsert.setDouble(3,dev);
			pstInsert.executeUpdate();
			
			pstInsert.close();
			conn.close();
			
		}catch(Exception e){
			e.printStackTrace();
		}
	}
}
