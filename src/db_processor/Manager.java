package db_processor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import db_processor.filter.FilterCleanHtml;

public class Manager
{
	private Connection conn;
	private int threads;
	private int max_queue;
	
	private ThreadPoolExecutor exec;
	
	private String count_sql;
	private String select_sql;
	
	public volatile StringBuilder error = new StringBuilder();
	public volatile boolean cont = true;
	
	public Manager(Connection conn, String sql, int threads, Class processor_class) throws SQLException
	{
		this.conn = conn;
		this.threads = threads;
		this.max_queue = threads * 2;
		
		exec = new ThreadPoolExecutor(threads, threads, 1, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());

		count_sql = sql.replace("SELECT * FROM", "SELECT COUNT(*) FROM");
		select_sql = sql + " LIMIT ";
	}
	
	public StringBuilder run(int offset, int chunk_size, int limit) throws SQLException, InterruptedException
	{
		ProgressBar progress = new ProgressBar(Math.min(get_max(), limit), 50, " rows");
		
		do
		{
			int get = Math.min(chunk_size, limit - offset);
			if (get <= 0) {cont = false; continue;}

			ResultSet res = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE).executeQuery(select_sql + Integer.toString(offset) + "," + Integer.toString(get));
			
			Filter filter = new FilterCleanHtml();
			filter.init(this, res);
			exec.execute(filter);

			offset += chunk_size;
			progress.set_cur(offset);
			
			synchronized(this)
			{
				while (exec.getQueue().size() >= max_queue)
				{
					wait();
				}
			}
		} while (cont);
		
		progress.end();
		
		exec.shutdown();
		exec.awaitTermination(100, TimeUnit.DAYS);
		
		return error;
	}
	
	private int get_max() throws SQLException
	{
		ResultSet res = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY).executeQuery(count_sql);
		if (res.next())
		{
			return res.getInt("COUNT(*)");
		}
		else
		{
			return 0;
		}
	}
}
