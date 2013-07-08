package db_processor;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class Manager
{
	private Connection conn;
	private int max_queue;
	private Class<Filter> filter;
	
	private ThreadPoolExecutor exec;
	
	private String count_sql;
	private String select_sql;
	
	public volatile boolean cont = true;
	
	// int[] will only ever have one item. The reason it's not just an Integer is because it needs to be mutable.
	public volatile ConcurrentHashMap<String, int[]> counts = new ConcurrentHashMap<String, int[]>();
	
	public Manager(Connection conn, String sql, int threads, Class<Filter> filter) throws SQLException
	{
		this.conn = conn;
		this.max_queue = threads * 2;
		this.filter = filter;
		
		exec = new ThreadPoolExecutor(threads, threads, 1, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());

		count_sql = sql.replace("SELECT * FROM", "SELECT COUNT(*) FROM");
		select_sql = sql + " LIMIT ";
	}
	
	public void run(int offset, int chunk_size, long limit) throws SQLException, InterruptedException
	{
		ProgressBar progress = new ProgressBar(Math.min(get_max(), limit), 50, " rows");
		
		do
		{
			long get = Math.min(chunk_size, limit - offset);
			if (get <= 0) {cont = false; continue;}

			ResultSet res = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE).executeQuery(select_sql + Integer.toString(offset) + "," + Long.toString(get));
			
			Filter filter_inst;
			try
			{
				filter_inst = filter.newInstance();
				filter_inst.init(this, res);
				exec.execute(filter_inst);
			}
			catch (InstantiationException e)
			{
				e.printStackTrace();
			}
			catch (IllegalAccessException e)
			{
				e.printStackTrace();
			}

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
		exec.awaitTermination(1, TimeUnit.DAYS);
	}
	
	private long get_max() throws SQLException
	{
		ResultSet res = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY).executeQuery(count_sql);
		if (res.next())
		{
			return res.getLong("COUNT(*)");
		}
		else
		{
			return 0;
		}
	}
}
