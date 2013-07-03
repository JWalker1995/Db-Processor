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
		row.updateString("text", row.getString("text") + "!");
		row.updateRow();
	}
}
