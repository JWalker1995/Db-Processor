package db_processor;

import java.sql.ResultSet;
import java.sql.SQLException;

public abstract class Filter
{
	abstract protected void process(ResultSet row) throws SQLException;
}
