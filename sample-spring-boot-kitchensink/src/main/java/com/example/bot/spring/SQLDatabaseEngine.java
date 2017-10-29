package com.example.bot.spring;

import lombok.extern.slf4j.Slf4j;
import javax.annotation.PostConstruct;
import javax.sql.DataSource;
import java.sql.*;
import java.net.URISyntaxException;
import java.net.URI;

@Slf4j
public class SQLDatabaseEngine extends DatabaseEngine {
	private static boolean LOCAL=false;
	
	@Override
	String search(String text) throws Exception {
		//Write your code here
		String result;
		Connection connection=getConnection();
		
		PreparedStatement stmt=connection.prepareStatement("SELECT response FROM chatbotDBTable "
				+ "WHERE LOWER(request) LIKE LOWER( CONCAT('%',?,'%') )");
		
		stmt.setString(1,text);
		
		ResultSet rs = stmt.executeQuery();
		if(!rs.next()) {	// not found
			rs.close();
			stmt.close();
			connection.close();
			throw new Exception("NOT FOUND");
		}else {				//found
			result=rs.getString(1);
		}
		rs.close();
		stmt.close();
		connection.close();
		return result;
	}
	
	/**
	 * Search the exact value, a 1-to-1 search.
	 * e.g. searchExact("faq", "keyword", "answer", "function"), which returns "This is a chatbot that provides booking service"
	 * @param table	the name of the table
	 * @param colInput	the name of the search key's field 
	 * @param colOutput	the name of the result's field
	 * @param key	the search key
	 * @return	the resulted value
	 * @throws Exception
	 */
	String searchExact(String table, String colInput, String colOutput, String key) throws Exception {
		//Write your code here
		String result;
		Connection connection=getConnection();
		
		String sql = String.format("SELECT %s FROM %s WHERE LOWER(%s) LIKE LOWER(CONCAT(?))", colOutput, table, colInput);
		PreparedStatement stmt=connection.prepareStatement(sql);

		stmt.setString(1,key);
		
		ResultSet rs = stmt.executeQuery();
		if(!rs.next()) {	// not found
			rs.close();
			stmt.close();
			connection.close();
			throw new Exception("NOT FOUND");
		}else {				//found
			result=rs.getString(1);
		}
		rs.close();
		stmt.close();
		connection.close();
		return result;
	}
	
	/**
	 * Insert a record.
	 * e.g. e.insertRecord("faq", "keyword", "answer", "COMP3111", "is love")
	 * @param table	the name of the table
	 * @param args (in order and respectively) the field names, then the new record's value
	 * @return boolean, true if insertion is successful.
	 * @throws Exception
	 */
	boolean insertRecord(String table, String... args) throws Exception {
		//Write your code here
		Connection connection=getConnection();
		
		String sql = "INSERT INTO " + table + " (";
		for (int i = 0; i < args.length / 2; i++) {
			sql += args[i];
			if (i != args.length / 2 - 1)
				sql += ",";
		}
		sql += ") VALUES (";
		for (int i = args.length / 2; i < args.length; i++) {
			sql += "'" + args[i] + "'";
			if (i != args.length - 1)
				sql += ",";
		}
		sql += ")";
		PreparedStatement stmt=connection.prepareStatement(sql);

		int rs = stmt.executeUpdate();
		
		if(rs == 0) {	// error
			stmt.close();
			connection.close();
			throw new Exception("FAILED INSERTION");
		}
		stmt.close();
		connection.close();
		return true;
	}
		
	/**
	 * Update records. Can be 1-to-1 / n-to-n / etc.
	 * e.g. e.updateRecord("COMP3111", 1, 2, "grade", "A+", "group_no", "22", "gender", "dog"), which returns 0.
	 * @param table the name of the table
	 * @param numberOfUpdateField the number of fields to be updated
	 * @param numberOfCondition the number of condition (the "WHERE"-part)
	 * @param args (in order and respectively) 1. the field names of the updated values, 2. the updated values, 3. the field names of the conditions, 4. the condition's key, 
	 * @return the number of updated records 
	 * @throws Exception
	 */
	int updateRecord(String table, int numberOfUpdateField, int numberOfCondition, String... args) throws Exception {
		//Write your code here
		int rs = -1;
		Connection connection=getConnection();
		
		String sql = "UPDATE " + table + " SET ";
		for (int i = 0; i < numberOfUpdateField; i++) {
			sql += args[i] + " = '" + args[i + numberOfUpdateField] + "'";
			if (i != numberOfUpdateField - 1)
				sql += ",";
		}
		sql += " WHERE ";
		for (int i = numberOfUpdateField * 2; i < numberOfUpdateField * 2 + numberOfCondition; i++) {
			sql += args[i] + " = '" + args[i + numberOfCondition] + "'";
			if (i != numberOfUpdateField * 2 + numberOfCondition - 1)
				sql += " AND ";
		}
		PreparedStatement stmt=connection.prepareStatement(sql);

		rs = stmt.executeUpdate();
		
		if(rs == -1) {	// error
			stmt.close();
			connection.close();
			throw new Exception("FAILED UPDATE");
		}
		stmt.close();
		connection.close();
		return rs;
	}
	
	/**
	 * Delete records.
	 * e.g. e.deleteRecord("COMP3111", 2, "gender", "M", "Surname", "Koo")
	 * @param table the name of the table
	 * @param numberOfCondition the number of condition (the "WHERE"-part)
	 * @param args (in order and respectively) 1. the field names of the conditions, 2. the condition's key
	 * @return the number of deleted records
	 * @throws Exception
	 */
	int deleteRecord(String table, int numberOfCondition, String... args) throws Exception {
		//Write your code here
		int rs = -1;
		Connection connection=getConnection();
		
		String sql = "DELETE FROM " + table + " WHERE ";
		for (int i = 0; i < numberOfCondition; i++) {
			sql += args[i] + " = '" + args[i + numberOfCondition] + "'";
			if (i != numberOfCondition - 1)
				sql += " AND ";
		}
		PreparedStatement stmt=connection.prepareStatement(sql);

		rs = stmt.executeUpdate();
		
		if(rs == -1) {	// error
			stmt.close();
			connection.close();
			throw new Exception("FAILED UPDATE");
		}
		stmt.close();
		connection.close();
		return rs;
	}
	
	private Connection getConnection() throws URISyntaxException, SQLException {
		Connection connection;
		URI dbUri = new URI(System.getenv("DATABASE_URL"));
		
		String username;
		String password;
		String dbUrl;
		if(LOCAL) {	//LOCAL HOST
			username = "tlkoo";
			password = "1234";
			dbUrl = "jdbc:postgresql://localhost:5432/chatbotDB";
		}else {//SERVER on HEROKU
			username = dbUri.getUserInfo().split(":")[0];
			password = dbUri.getUserInfo().split(":")[1];
			dbUrl = "jdbc:postgresql://" + dbUri.getHost() + ':' + dbUri.getPort() + dbUri.getPath() +  "?ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory";
		}
				
		log.info("Username: {} Password: {}", username, password);
		log.info ("dbUrl: {}", dbUrl);
		
		connection = DriverManager.getConnection(dbUrl, username, password);

		return connection;
	}

}
