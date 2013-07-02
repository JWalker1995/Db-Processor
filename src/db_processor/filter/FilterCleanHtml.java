package db_processor.filter;

import java.sql.ResultSet;
import java.sql.SQLException;

import db_processor.Filter;

public class FilterCleanHtml extends Filter
{
	@Override
	public void process(ResultSet row) throws SQLException
	{
		// Do something with row
		long j = 0;
		int i = 0;
		while (i < Integer.MAX_VALUE) {i++; j += i;}
		if (j % 2 == 3) {System.out.println(j);}
	}
}
