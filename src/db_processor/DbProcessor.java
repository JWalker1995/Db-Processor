package db_processor;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicIntegerArray;

public class DbProcessor
{
	static final String DB_JDBC_DRIVER = "com.mysql.jdbc.Driver";  

	public static void main(String[] args)
	{
		// Test args:
		// CleanHtml -d piqa -t node -u root -c 10 -l 100 -x 8
		
		if (args.length < 1)
		{
			System.out.println("Please specify the processor to run.");
			return;
		}
		
		String filter_class = args[0];
		if (filter_class.toLowerCase() == "list")
		{
			System.out.println("CleanHtml");
			return;
		}
		
		String host = "127.0.0.1";
		String port = "3306";
		String db = "";
		String user = "";
		String password = "";
		String table = "";
		int threads = 16;
		int chunk_size = 1000;
		int limit = Integer.MAX_VALUE;
		
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
			else if (args[i].equals("-x") || args[i].equals("--threads"))
			{
				threads = Integer.parseInt(args[++i]);
			}
			else if (args[i].equals("-c") || args[i].equals("--chunk"))
			{
				chunk_size = Integer.parseInt(args[++i]);
			}
			else if (args[i].equals("-l") || args[i].equals("--limit"))
			{
				limit = Integer.parseInt(args[++i]);
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
			try
			{
				Class.forName(DB_JDBC_DRIVER);
			}
			catch (ClassNotFoundException e)
			{
		        System.out.println("Class " + DB_JDBC_DRIVER + " not found");
		        return;
			}
			
			String url = "jdbc:mysql://" + host + ":" + port + "/" + db;
			
			int[] statuses = new int[threads];
			LinkedBlockingQueue<Status> messenger = new LinkedBlockingQueue<Status>();
			
			i = 0;
			while (i < threads)
			{
				System.out.println("Starting thread " + Integer.toString(i) + "...");
		        Connection conn = DriverManager.getConnection(url, user, password);
		        Manager manager = new Manager(conn, table, filter_class, messenger, i, chunk_size, threads, limit);
				manager.start();
				Thread.sleep((int)(Math.random() * 1000));
				i++;
			}
			
			System.out.println("Running...");
			
			int running = threads;
			int errors = 0;
			int chunks = 0;
			while (running > 0)
			{
				Status status = messenger.take();
				switch (status.offset)
				{
				case -1:
					// Thread complete
					running--;
					break;
				case -2:
					// Thread complete with errors
					running--;
					errors++;
					break;
				default:
					// Offset update
					statuses[status.id] = status.offset;
					chunks++;
					System.out.print(Integer.toString(chunks) + "\r");
				}
			}
			
			if (errors == 0)
			{
				System.out.println("All threads completed successfully!");
			}
			else
			{
				System.out.println("All threads finished, " + Integer.toString(errors) + " because of errors :(");
			}
			
			// slow: 16 -> 18
			// fast: 18 -> 18
			
			// If the fastest thread's chunk offset is more than 9/8 of the slowest thread's chunk offset and the slowest thread's chunk offset is more than 16:
			// Signal slowest child thread to skip next chunk
			// Child thread sends chunk id to main thread
			// Main thread sends chunk id to fastest child thread
		}
		catch (SQLException ex)
		{
		    System.out.println("SQLException: " + ex.getMessage());
		    System.out.println("SQLState: " + ex.getSQLState());
		    System.out.println("VendorError: " + ex.getErrorCode());
			ex.printStackTrace();
		}
		catch (InterruptedException ex)
		{
			System.out.println("InterruptedException: " + ex.getMessage());
			ex.printStackTrace();
		}
	}
}