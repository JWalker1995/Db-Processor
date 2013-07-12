package db_processor.filter;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import db_processor.Filter;

public abstract class FilterRemoveRegex extends Filter
{
	private boolean opts_loaded = false;
	private String column;
	private Pattern regex;
	
	public FilterRemoveRegex()
	{
		regex = Pattern.compile(get_regex(), Pattern.CASE_INSENSITIVE);
	}
	
	protected abstract String get_regex();
	
	@Override
	protected HashMap<String, String> get_params()
	{
		HashMap<String, String> res = new HashMap<String, String>();
		
		// Column to filter
		res.put("--column", null);
		
		return res;
	}

	@Override
	protected void process(ResultSet row) throws SQLException
	{
		if (!opts_loaded)
		{
			column = opts.get("--column");
			
			opts_loaded = true;
		}
		
		String orig_text = row.getString(column);
		Matcher matcher = regex.matcher(orig_text);
		String new_text = matcher.replaceAll("");
		
		if (!orig_text.equals(new_text))
		{
			if (update)
			{
				row.updateString(column, new_text);
				row.updateRow();
			}
			else
			{
				log(orig_text);
				log(new_text);
				log();
			}
			count("Updated");
		}
		count("Processed");
	}

}
