package db_processor.filter;

public class FilterStripAnswerPlaceholders extends FilterRemoveRegex
{
	@Override
	protected String get_column()
	{
		return "text";
	}

	@Override
	protected String get_regex()
	{
		return "\\s*=+\\s*answer\\s*=+\\s*";
	}

}
