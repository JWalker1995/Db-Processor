package db_processor.filter;

import java.sql.ResultSet;
import java.sql.SQLException;

import db_processor.Filter;

public class FilterCountStartWords extends Filter
{
	public FilterCountStartWords()
	{
		// Will be run less often than process()
	}
	
	@Override
	public void process(ResultSet row) throws SQLException
	{
		String str = row.getString("text");
		int i = str.indexOf(" ");
		if (i == -1)
		{
			count(str);
		}
		else
		{
			count(str.substring(0, i));
		}
	}
}