package db_processor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicIntegerArray;

public class Manager extends Thread
{
	private Connection conn;
	private String sql;
	private String filter_class;
	private LinkedBlockingQueue<Status> messenger;
	private int id;
	
	private volatile Integer offset;
	private int chunk_size;
	private int step;
	private int limit;

	private LinkedBlockingQueue<Integer> extra;
	
	public volatile StringBuilder error = new StringBuilder();
	public volatile boolean cont = true;
	
	public Manager(Connection conn, String table, String filter_class, LinkedBlockingQueue<Status> messenger, int offset, int chunk_size, int step, int limit)
	{
		this.conn = conn;
		this.sql = "SELECT * FROM " + table + " LIMIT ?,?";
		this.filter_class = filter_class;
		this.messenger = messenger;
		this.id = offset;

		this.offset = offset * chunk_size;
		this.chunk_size = chunk_size;
		this.step = step * chunk_size;
		this.limit = limit;
		
		extra = new LinkedBlockingQueue<Integer>();
	}
	
	public int skip_next()
	{
		return next_offset();
	}
	
	public void add_extra(int offset) throws InterruptedException
	{
		extra.offer(offset);
	}
	
	public void run()
	{
		// This method runs in a child thread
		
		Filter filter;
		try
		{
			filter = (Filter) Class.forName("db_processor.filter.Filter" + filter_class).newInstance();
		}
		catch (ClassNotFoundException e)
		{
	        System.out.println("ClassNotFoundException for class " + "Filter" + filter_class);
	        error(); return;
		}
		catch (InstantiationException e)
		{
			System.out.println("InstantiationException for class " + "Filter" + filter_class);
			error(); return;
		}
		catch (IllegalAccessException e)
		{
			System.out.println("IllegalAccessException for class " + "Filter" + filter_class);
			error(); return;
		}
		
		PreparedStatement stmt;
		try
		{
			stmt = conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
		}
		catch (SQLException ex)
		{
		    System.out.println("SQLException: " + ex.getMessage());
		    System.out.println("SQLState: " + ex.getSQLState());
		    System.out.println("VendorError: " + ex.getErrorCode());
			ex.printStackTrace();
			error(); return;
		}
		
		while (true)
		{
			Integer cur_offset = extra.poll();
			if (cur_offset == null)
			{
				cur_offset = next_offset();
			}
			int get = Math.min(chunk_size, limit - cur_offset);
			if (get <= 0) {complete(); return;}

			try
			{
				stmt.setInt(1, cur_offset);
				stmt.setInt(2, get);
				
				ResultSet res = stmt.executeQuery();
				
				int i = 0;
				while (res.next())
				{
					try // This sub try/catch is so that exceptions thrown by processing a single row don't prevent processing the rest of the chunk.
					{
						filter.process(res);
					}
					catch (SQLException ex)
					{
					    System.out.println("SQLException: " + ex.getMessage());
					    System.out.println("SQLState: " + ex.getSQLState());
					    System.out.println("VendorError: " + ex.getErrorCode());
						ex.printStackTrace();
					}
					i++;
				}
				res.close();
				if (i == 0) {complete(); return;}
			}
			catch (SQLException ex)
			{
			    System.out.println("SQLException: " + ex.getMessage());
			    System.out.println("SQLState: " + ex.getSQLState());
			    System.out.println("VendorError: " + ex.getErrorCode());
				ex.printStackTrace();
			}
		}
	}
	
	private int next_offset()
	{
		synchronized (offset)
		{
			int res = offset;
			messenger.offer(new Status(id, res));
			offset += step;
			return res;
		}
	}

	private void complete()
	{
		messenger.offer(new Status(id, -1));
	}
	private void error()
	{
		messenger.offer(new Status(id, -2));
	}
	
	/*
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
	*/
}
