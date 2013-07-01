package db_processor;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class DbProcessor
{
	static final String DB_JDBC_DRIVER = "com.mysql.jdbc.Driver";  
	static final String DB_URL = "jdbc:mysql://127.0.0.1:3306/gnosis";
	static final String DB_USER = "root";
	static final String DB_PASS = "";
	
	static final String PROCESSOR_CLASS = "ProcessorCleanHtml";

	public static void main(String[] args)
	{
		if (args.length < 1)
		{
			System.out.println("Please specify the processor to run.");
			return;
		}
		
		String processor_class = args[0];
		String host = "127.0.0.1";
		String port = "3306";
		String db = "";
		String user = "";
		String password = "";
		String table = "";
		int threads = 16;
		String chunk_size = "1000";
		
		int i = 1;
		int c = args.length - 1;
		while (i < c)
		{
			if (args[i].equals("-h") || args[i].equals("--host"))
			{
				host = args[++i];
			}
			else if (args[i].equals("-p") || args[i].equals("--port"))
			{
				port = Integer.toString(Integer.parseInt(args[++i]));
			}
			else if (args[i].equals("-d") || args[i].equals("--db"))
			{
				db = args[++i];
			}
			else if (args[i].equals("-u") || args[i].equals("--user"))
			{
				user = args[++i];
			}
			else if (args[i].equals("-w") || args[i].equals("--password"))
			{
				password = args[++i];
			}
			else if (args[i].equals("-t") || args[i].equals("--table"))
			{
				table = args[++i];
			}
			else if (args[i].equals("-h") || args[i].equals("--threads"))
			{
				threads = Integer.parseInt(args[++i]);
			}
			else if (args[i].equals("-c") || args[i].equals("--chunk"))
			{
				chunk_size = Integer.toString(Integer.parseInt(args[++i]));
			}
			else
			{
				System.out.println("Warning: Unrecognized argument \"" + args[i] + "\"");
			}
			i++;
		}
		
		if (db.isEmpty())
		{
			System.out.println("Please specify a database with -d or --db");
			return;
		}
		if (user.isEmpty())
		{
			System.out.println("Please specify a user with -u or --user");
			return;
		}
		if (table.isEmpty())
		{
			System.out.println("Please specify a table with -t or --table");
			return;
		}
		
		try
		{
			Class.forName(DB_JDBC_DRIVER);
	        Connection conn = DriverManager.getConnection("jdbc:mysql://" + host + ":" + port + "/" + db, user, password);
	        System.out.println("Connected to database...");

		    Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
		    if (stmt.execute("SELECT * FROM " + table + " LIMIT " + chunk_size))
		    {
		        ResultSet rs = stmt.getResultSet();
		        rs.next();
		        System.out.println("Success");
		    }
		    else
		    {
		    	System.out.println("Failed");
		    }
		}
        catch (ClassNotFoundException e)
        {
        	System.out.println("Class " + DB_JDBC_DRIVER + " not found");
		}
		catch (SQLException ex)
		{
		    System.out.println("SQLException: " + ex.getMessage());
		    System.out.println("SQLState: " + ex.getSQLState());
		    System.out.println("VendorError: " + ex.getErrorCode());
		}
	}
}