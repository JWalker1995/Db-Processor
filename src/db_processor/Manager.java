package db_processor;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import db_processor.filter.FilterCleanHtml;

public class Manager
{
	private Connection conn;
	private String sql;
	private int threads;
	private int max_queue;
	
	private ThreadPoolExecutor exec;
	
	public volatile StringBuilder error = new StringBuilder();
	public volatile boolean cont = true;
	
	public Manager(Connection conn, String sql, int threads, Class processor_class)
	{
		this.conn = conn;
		this.sql = sql;
		this.threads = threads;
		this.max_queue = threads * 2;
		
		exec = new ThreadPoolExecutor(threads, threads, 1, TimeUnit.SECONDS, null);
	}
	
	public void run(int offset, int chunk_size) throws SQLException, InterruptedException
	{
		ResultSet res;
		do
		{
		    Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
			if (stmt.execute(sql + " LIMIT " + Integer.toString(offset) + "," + Integer.toString(chunk_size)))
		    {
		        res = stmt.getResultSet();
		        Filter filter = new FilterCleanHtml();
		        
		        if (threads == 0)
		        {
		        	
		        }
		    }
		    else
		    {
		    	cont = false;
		    }

			offset += chunk_size;
			
			while (exec.getQueue().size() >= max_queue)
			{
				wait();
			}
		} while (cont);
		
		exec.shutdown();
		if (!exec.awaitTermination(60, TimeUnit.SECONDS))
		{
			error.append("Threads not ter\n");
		}
	}
}
