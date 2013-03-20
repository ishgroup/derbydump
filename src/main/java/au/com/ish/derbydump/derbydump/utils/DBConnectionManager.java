package au.com.ish.derbydump.derbydump.utils;

import au.com.ish.derbydump.derbydump.config.Configuration;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;


public class DBConnectionManager {

	private Connection connection;

	public DBConnectionManager(String url) throws ClassNotFoundException, IllegalAccessException, InstantiationException, SQLException {
		Class.forName(Configuration.getConfiguration().getDriverClassName()).newInstance();
		connection = DriverManager.getConnection(url);
	}

	public  Connection getConnection() {
		return connection;
	}

}
