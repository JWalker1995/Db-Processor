package db_processor;

import java.sql.ResultSet;

public abstract class Processor implements Runnable
{
	@Override
	public void run()
	{
		
	}
	
	abstract public void process(ResultSet row);
}
