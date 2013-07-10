package db_processor.filter;

public class FilterStripAnswcdnGraphics extends FilterRemoveRegex
{
	@Override
	protected String get_column()
	{
		return "text";
	}

	@Override
	protected String get_regex()
	{
		return "<(\\w+)\\s[^<>]*answcdn\\.com[^<>]*>";
	}

}
