package db_processor;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

public abstract class Filter implements Runnable
{
	private Manager manager;
	private ResultSet rows;
	protected HashMap<String, String> opts;
	protected boolean update;
	
	private StringBuilder local_log = new StringBuilder();
	
	final public void init(Manager manager, ResultSet rows, HashMap<String, String> opts, boolean update)
	{
		this.manager = manager;
		this.rows = rows;
		this.opts = opts;
		this.update = update;
	}
	
	@Override
	final public void run()
	{
		try
		{
			while (rows.next())
			{
				try // This sub try/catch is so that exceptions thrown by processing a single row don't prevent processing the rest of the chunk.
				{
					process(rows);
				}
				catch (SQLException ex)
				{
					System.out.println("SQLException: " + ex.getMessage());
					System.out.println("SQLState: " + ex.getSQLState());
					System.out.println("VendorError: " + ex.getErrorCode());
				}
			}
			rows.close();
		}
		catch (SQLException ex)
		{
			System.out.println("SQLException: " + ex.getMessage());
			System.out.println("SQLState: " + ex.getSQLState());
			System.out.println("VendorError: " + ex.getErrorCode());
			manager.cont = false;
		}
		synchronized(manager)
		{
			manager.notify();
		}
		
		if (local_log.length() > 0)
		{
			manager.log.append(local_log);
		}
	}

	abstract protected HashMap<String, String> get_params();
	abstract protected void process(ResultSet row) throws SQLException;
	
	protected void count(String key)
	{
		count(key, 1);
	}
	protected void count(String key, int count)
	{
		int[] val = manager.counts.get(key);
		if (val == null)
		{
			manager.counts.put(key, new int[] {count});
		}
		else
		{
			synchronized (val)
			{
				val[0] += count;
			}
		}
	}
	protected void log()
	{
		local_log.append("\n");
	}
	protected void log(String line)
	{
		local_log.append(line + "\n");
	}
	protected void stop()
	{
		manager.cont = false;
	}
	protected void stop(String line)
	{
		log(line);
		manager.cont = false;
	}
}
