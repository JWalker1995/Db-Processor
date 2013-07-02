package db_processor;

import java.sql.ResultSet;
import java.sql.SQLException;

public abstract class Filter implements Runnable
{
	private Manager manager;
	private ResultSet rows;
	
	final public void init(Manager manager, ResultSet rows)
	{
		this.manager = manager;
		this.rows = rows;
	}
	
	@Override
	public void run()
	{
		try
		{
			int i = 0;
			while (rows.next())
			{
				process(rows);
				i++;
			}
			//if (i == 0) {manager.cont = false;}
		}
		catch (SQLException e)
		{
			synchronized(this)
			{
				manager.error.append(e.toString()).append("\n");
				manager.cont = false;
			}
		}
		manager.notify();
	}
	
	abstract protected void process(ResultSet row);
}
