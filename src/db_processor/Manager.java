package db_processor;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
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
	
	public volatile HashMap<String, int[]> counts = new HashMap<String, int[]>();
	public volatile StringBuffer log = new StringBuffer();
	
	public Manager(Connection conn, String sql, int threads, Class<Filter> filter) throws SQLException
	{
		this.conn = conn;
		this.max_queue = threads * 2;
		this.filter = filter;
		
		exec = new ThreadPoolExecutor(threads, threads, 1, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());

		count_sql = sql.replace("SELECT * FROM", "SELECT COUNT(*) FROM");
		select_sql = sql + " LIMIT ";
	}
	
	public void run(HashMap<String, String> opts, boolean update, int offset, int chunk_size, long limit) throws SQLException, InterruptedException
	{
		// Make limit absolute
		limit += offset;
		
		ProgressBar progress = new ProgressBar(offset, Math.min(get_max(), limit), 64, " rows");

		do
		{
			long get = Math.min(chunk_size, limit - offset);
			if (get <= 0) {cont = false; continue;}

			ResultSet res = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, update ? ResultSet.CONCUR_UPDATABLE : ResultSet.CONCUR_READ_ONLY).executeQuery(select_sql + Integer.toString(offset) + "," + Long.toString(get));
			
			Filter filter_inst;
			try
			{
				filter_inst = filter.newInstance();
				filter_inst.init(this, res, opts, update);
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
		exec.awaitTermination(1, TimeUnit.HOURS);
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
