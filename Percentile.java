import java.sql.Timestamp;
import java.util.Date;
import java.util.Random;
import java.util.ArrayList;
import java.util.Vector;
import java.util.List;
import java.sql.*;

public class Percentile{
	//public static List messages = new ArrayList();
	public static Vector messages = new Vector(3,2);
	public Connection getConnection(){
		Connection conn = null;
		try{
			DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
			String dbUrl = "jdbc:oracle:thin:@192.168.140.62:1521:orcl";
			conn = DriverManager.getConnection(dbUrl, "c##perf","c##perf");
			conn.setAutoCommit(false);
		}
		catch(Exception e){
		}
		return conn;
	}
	
	public static void main(String []args){
		try{
			if(args.length == 3){
				Thread tFeeder = new Thread(new Feeder(messages,Long.parseLong(args[0]),Integer.parseInt(args[2])));
				tFeeder.start();
				Percentile objPercentile = new Percentile();
				
				Thread tReader = new Thread(new Reader(messages,Integer.parseInt(args[1])));
				tReader.start();
				Thread tCalculate = new Thread(new Calculate());
				tCalculate.start();
				
				tFeeder.join();
				tReader.join();
				tCalculate.join();
			
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
	//public List messages = new ArrayList();
	public Vector messages = new Vector(3,2);
	Date date;
	double value;
	Random rnd = new Random();
	long injectionRate;
	int endTime;
	
	public Feeder(Vector messages,long injectionRate, int endTime){
		//message = new StringBuffer();
		message = "";
		serialNo = 1;
		FS = "|";
		this.messages = messages;
		this.injectionRate = injectionRate;
		this.endTime = endTime;
	}

	public void run(){
		try{
			boolean flag = true;
			long start = System.currentTimeMillis();
			long Time = start + endTime*60*1000;
			while(System.currentTimeMillis() < Time){
				//while(flag){
					for(int i=0;i<50000;i++){
						int randomInt = rnd.nextInt(5000);
						double randomDouble = Math.random();
						date = new Date();
						Timestamp st = new java.sql.Timestamp(date.getTime());
						value = randomDouble*randomInt;
						//message.append(serialNo).append(FS).append(new java.sql.Timestamp(date.getTime())).append(FS).append(randomDouble*randomInt);
						message = serialNo+FS+st+FS+value;
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
					Thread.sleep(900);
					if(injectionRate == 100000)                          //Given that 100000 msgs take 230 ms
						Thread.sleep(770);
					if(injectionRate == 200000)
						Thread.sleep(270);
					if(injectionRate == 300000)
						Thread.sleep(100);
					
				//}
			}
		}
		catch(Exception e){
			System.out.println("Error from Feeder");
			e.printStackTrace();
		}
	}
}

class Reader extends Thread{

	//private List messages = new ArrayList();
	private Vector messages = new Vector(3,2);
	int batchSize;
	public Reader(Vector message,int batchSize){
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
			Statement statement = conn.createStatement();
			Date date = new Date();
			PreparedStatement pst = null;
			pst = conn.prepareStatement("insert into utable (id, ts, value)  values (?,?,?)");
			while(flag){
				int i = 0;
				
				for(i =0 ;i<batchSize ;i++){
					if(!messages.isEmpty()){
					//for( i = 0;i< messages.size();i++){
						String msg = (String)messages.remove(0);
						//String msg = messages.remove(0).toString();
						if(msg == null)
							continue;
						msgs = msg.split("\\|");
						//System.out.println(msgs[0]+" "+msgs[1]+" "+msgs[2]+" ");
						try {
						/*
							Query = "insert into utable (ID, TS, VALUE)  values ("
							+ msgs[0] + ",'" + Timestamp.valueOf(msgs[1]) + "',"
							+ msgs[2] + ")";	
						*/	
							pst.setInt(1,Integer.parseInt(msgs[0]));
							pst.setTimestamp(2, Timestamp.valueOf(msgs[1]));
							pst.setDouble(3,Double.parseDouble(msgs[2]));
							pst.addBatch();
						} catch (Exception e) {
							e.printStackTrace();
							flag = true;
							continue;
							
						}				
					//}
					}
					else{
						Thread.sleep(100);	
					}	
				}
				pst.executeBatch();
				conn.commit();
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
			String sql = "select count(value),PERCENTILE_CONT(0.9) within group (order by value desc) from utable";
			//String sql = "select max(ID),PERCENTILE_CONT(0.9) within group (order by value desc) from utable";
				
			PreparedStatement preStatement = conn.prepareStatement(sql);
			ResultSet result;
			boolean flag = true;
			while(flag){
				
				Thread.sleep(5);
				result = preStatement.executeQuery();
				//System.out.println("Here"+result.next());
				while(result.next()){
					int serialNo = result.getInt(1);
					double percentile = result.getDouble(2);
					System.out.println("Index @ " + serialNo+" Percentile is "+percentile);
					if(!uniqueRows.contains(serialNo)){
						insertInDB(serialNo,percentile);
						uniqueRows.add(serialNo);
					}	
				}
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
			pstInsert = conn.prepareStatement("insert into vtable (id, ts, value)  values (?,?,?)");
			pstInsert.setInt(1,serialNo);
			pstInsert.setTimestamp(2, new Timestamp(date.getTime()));
			pstInsert.setDouble(3,percentile);
			pstInsert.executeUpdate();
			
			pstInsert.close();
			conn.close();
			
		}catch(Exception e){
			e.printStackTrace();
		}
	}
}
