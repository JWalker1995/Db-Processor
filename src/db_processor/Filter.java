package db_processor;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.Callable;

public abstract class Filter implements Callable<Integer>
{
	private Manager manager;
	
	public Integer run(ResultSet rows) throws SQLException
	{
		int i = 0;
		while (rows.next())
		{
			process(rows);
			i++;
		}
		//manager.notify();
		
		return i;
	}
	
	abstract public void process(ResultSet row);
}
