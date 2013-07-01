package db_processor;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

public class Manager
{
	Connection conn;
	String sql;
	int threads;
	
	public Manager(Connection conn, String sql, int threads)
	{
		this.conn = conn;
		this.sql = sql;
		this.threads = threads;
	}
	
	public void run(int offset, int chunk_size)
	{
		ResultSet res;
		do
		{
		    Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
		    if (stmt.execute(sql + " LIMIT " + Integer.toString(offset) + "," + Integer.toString(chunk_size)))
		    {
		        res = stmt.getResultSet();
		        if (threads == 0)
		        {
		        	
		        }
		    }
		    else
		    {
		    }

			offset += chunk_size;
		} while (res.is)
	}
}
