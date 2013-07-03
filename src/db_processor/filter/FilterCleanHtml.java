package db_processor.filter;

import java.sql.ResultSet;
import java.sql.SQLException;

import db_processor.Filter;

public class FilterCleanHtml extends Filter
{
	public FilterCleanHtml()
	{
		// Will be run less often than process()
	}
	
	@Override
	public void process(ResultSet row) throws SQLException
	{
		// Do something with row
		boolean changed = false;
		String str = row.getString("text");
		
		while (str.endsWith("$") || str.endsWith("!"))
		{
			str = str.substring(0, str.length() - 1);
			changed = true;
		}
		
		if (Math.random() < 0.1)
		{
			str = str + "!";
			changed = true;
		}
		
		if (changed)
		{
			row.updateString("text", str);
			row.updateRow();
		}
	}
}
