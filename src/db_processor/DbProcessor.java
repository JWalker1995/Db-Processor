package db_processor;
import java.io.Console;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

public class DbProcessor
{
	static final String DB_JDBC_DRIVER = "com.mysql.jdbc.Driver";  

	@SuppressWarnings("unchecked")
	public static void main(String[] args)
	{
		if (args.length < 1)
		{
			System.out.println("Please specify the processor to run, or append \"help\".");
			return;
		}
		
		String filter_class = args[0];
		if (filter_class.equals("list"))
		{
			print_list();
			return;
		}
		else if (filter_class.equals("help"))
		{
			print_help();
			return;
		}
		
		Class<Filter> filter;
		try
		{
			filter = (Class<Filter>) Class.forName("db_processor.filter.Filter" + filter_class);
		}
		catch (ClassNotFoundException e)
		{
	        System.out.println("Class " + "Filter" + filter_class + " not found");
	        return;
		}
		
		HashMap<String, String> opts = new HashMap<String, String>();
		try
		{
			opts = filter.newInstance().get_params();
		}
		catch (InstantiationException e)
		{
			e.printStackTrace();
		}
		catch (IllegalAccessException e)
		{
			e.printStackTrace();
		}
		
		String host = "127.0.0.1";
		String port = "3306";
		String db = "";
		String user = "";
		String table = "";
		String where = "";
		String sql = "";
		String primary = "";
		int threads = 16;
		int offset = 0;
		int chunk_size = 1000;
		long limit = Long.MAX_VALUE;
		boolean update = false;
		
		int i = 1;
		int c = args.length - 1;
		while (i < c)
		{
			boolean unrecognized = false;
			
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
			else if (args[i].equals("-t") || args[i].equals("--table"))
			{
				table = args[++i];
			}
			else if (args[i].equals("-w") || args[i].equals("--where"))
			{
				where = args[++i];
			}
			else if (args[i].equals("-q") || args[i].equals("--sql"))
			{
				sql = args[++i];
			}
			else if (args[i].equals("-r") || args[i].equals("--primary"))
			{
				primary = args[++i];
			}
			else if (args[i].equals("-x") || args[i].equals("--threads"))
			{
				threads = Integer.parseInt(args[++i]);
			}
			else if (args[i].equals("-o") || args[i].equals("--offset"))
			{
				offset = Integer.parseInt(args[++i]);
			}
			else if (args[i].equals("-c") || args[i].equals("--chunk"))
			{
				chunk_size = Integer.parseInt(args[++i]);
			}
			else if (args[i].equals("-l") || args[i].equals("--limit"))
			{
				limit = Integer.parseInt(args[++i]);
			}
			else if (args[i].equals("--update-db"))
			{
				update = !args[++i].isEmpty();
			}
			else
			{
				unrecognized = true;
			}
			
			if (opts.containsKey(args[i]))
			{
				opts.put(args[i], args[++i]);
			}
			else if (unrecognized)
			{
				System.out.println("Warning: Unrecognized argument \"" + args[i] + "\"");
			}
			i++;
		}
		
		boolean stop = false;
		if (user.isEmpty())
		{
			System.out.println("Please specify a user with -u or --user");
			stop = true;
		}
		if (db.isEmpty())
		{
			System.out.println("Please specify a database with -d or --db");
			stop = true;
		}
		if (table.isEmpty() && sql.isEmpty())
		{
			System.out.println("Please specify a table with -t or --table");
			stop = true;
		}
		
		Iterator<Map.Entry<String, String>> it = opts.entrySet().iterator();
		while (it.hasNext())
		{
			Map.Entry<String, String> opt = it.next();
			if (opt.getValue() == null)
			{
				System.out.println("Please specify the " + opt.getKey() + " parameter");
				stop = true;
			}
		}
		
		if (stop) {return;}
		
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
			
			char password[];
			Console console = System.console();
			if (console == null)
			{
				password = new char[] {};
			}
			else
			{
				password = console.readPassword("Db password: ");
			}
	        Connection conn = DriverManager.getConnection("jdbc:mysql://" + host + ":" + port + "/" + db, user, new String(password));
	        System.out.println("Connected to database...");
	        
	        if (primary.isEmpty())
	        {
	        	ResultSet res = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY).executeQuery("SHOW KEYS FROM node WHERE Key_name = \"PRIMARY\"");
	    		if (res.next())
	    		{
	    			primary = res.getString("Column_name");
	    		}
	        }
	        
			if (sql.isEmpty())
			{
				sql = "SELECT * FROM " + table + " WHERE " + primary + " >= [min] AND " + primary + " < [max]" + (where.isEmpty() ? "" : " AND " + where) + " ORDER BY " + primary;
			}
			
			// Trim and remove trailing ";"
			while ((sql = sql.trim()).endsWith(";"))
			{
				sql = sql.substring(0, sql.length() - 1);
			}

	        String max_sql = "SELECT MAX(" + primary + ")+1 AS max FROM " + table + (where.isEmpty() ? "" : " WHERE " + where);
			
			System.out.println("Please verify these parameters:");
			System.out.println("	processor: " + filter_class);
			System.out.println("	host: " + host);
			System.out.println("	port: " + port);
			System.out.println("	user: " + user);
			System.out.println("	using password: " + (password.length > 0 ? "YES" : "NO"));
			System.out.println("	database: " + db);
			System.out.println("	sql: " + sql);
			System.out.println("	offset: " + Integer.toString(offset));
			System.out.println("	chunk size: " + Integer.toString(chunk_size));
			System.out.println("	limit: " + Long.toString(limit));
			System.out.println("	threads: " + Integer.toString(threads));
			System.out.println("	memory limit: " + Long.toString(Runtime.getRuntime().maxMemory() / 1024 / 1024) + "MB (change with -Xmx[gigabytes]g)");
			System.out.println("	update database: " + (update ? "YES" : "NO"));
			
			it = opts.entrySet().iterator();
			while (it.hasNext())
			{
				Map.Entry<String, String> opt = it.next();
				System.out.println("	" + opt.getKey() + ": " + (opt.getValue() == null ? "null" : opt.getValue()));
			}
			
			System.out.print("Continue (Y/N)? ");
			
		    Scanner input = new Scanner(System.in);
		    String answer = input.nextLine();
		    if (!answer.startsWith("Y") && !answer.startsWith("y"))
		    {
		    	System.out.println("Cancelled :(");
		    	return;
		    }
			
		    String start_date = new Date().toString();
		    
	        Manager manager = new Manager(conn, sql, max_sql, threads, filter);
	        manager.run(opts, update, offset, chunk_size, limit);
	        
	        String end_date = new Date().toString();
	        
	        System.out.println("Finished!");
	        
	        if (!manager.counts.isEmpty())
	        {
	        	System.out.println("Counts:");
	        	List<Map.Entry<String, int[]>> counts = new LinkedList<Map.Entry<String, int[]>>(manager.counts.entrySet());
	            Collections.sort(counts, new Comparator<Map.Entry<String, int[]>>()
	            {
	                public int compare(Map.Entry<String, int[]> m1, Map.Entry<String, int[]> m2)
	                {
	                    return m2.getValue()[0] - m1.getValue()[0];
	                }
	            });

		        PrintWriter file = new PrintWriter("counts.txt");
	            i = 0;
	            Iterator<Map.Entry<String, int[]>> it2 = counts.iterator();
	        	while (it2.hasNext())
	        	{
	        		Map.Entry<String, int[]> e = it2.next();
	        		String str = e.getKey() + ": " + Integer.toString(e.getValue()[0]);
	        		file.println(str);
	        		i++;
	        		if (i <= 100)
	        		{
	        			System.out.println(str);
	        		}
	        	}
	        	file.close();
	        	
	        	if (i > 100)
	        	{
	        		System.out.println("Output (" + Integer.toString(i) + " lines) truncated to 100 lines, see counts.txt for a the full counts.");
	        	}
	        }
	        
        	PrintWriter file = new PrintWriter("log.txt");
        	file.println("Started at: " + start_date);
        	file.println("Ended at: " + end_date);
        	file.println();
        	file.print(manager.log);
        	file.close();
        	System.out.println("Log written to log.txt");
        	
	        if (!update)
	        {
	        	System.out.println("Add \"--update-db 1\" to update the database.");
	        }
		}
		catch (SQLException ex)
		{
			ex.printStackTrace();
		    System.out.println("SQLException: " + ex.getMessage());
		    System.out.println("SQLState: " + ex.getSQLState());
		    System.out.println("VendorError: " + ex.getErrorCode());
		}
		catch (InterruptedException e)
		{
			e.printStackTrace();
		}
		catch (FileNotFoundException e)
		{
			e.printStackTrace();
		}
	}
	
	private static void print_list()
	{
		System.out.println("Processors:");
		System.out.println("	CleanHtml");
		System.out.println("	StripAnswerPlaceholders");
		System.out.println("	StripAnswcdnGraphics");
	}
	
	private static void print_help()
	{
		System.out.println("java -Xmx[gigabytes]g -jar DbProcessor.jar [processor name] [options]");
		System.out.println("java -jar DbProcessor.jar list");
		System.out.println("java -jar DbProcessor.jar help");
		System.out.println();
		System.out.println("Options:");
		System.out.println("	-h, --host");
		System.out.println("	-p, --port");
		System.out.println("	-d, --db");
		System.out.println("	-u, --user");
		System.out.println("	-t, --table");
		System.out.println("	-w, --where");
		System.out.println("	-q, --sql");
		System.out.println("	-r, --primary");
		System.out.println("	-x, --threads");
		System.out.println("	-o, --offset");
		System.out.println("	-c, --chunk");
		System.out.println("	-l, --limit");
		System.out.println("	--update-db");
	}
}