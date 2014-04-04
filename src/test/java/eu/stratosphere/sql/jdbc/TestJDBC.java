package eu.stratosphere.sql.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import junit.framework.Assert;

import org.junit.Test;

public class TestJDBC {
	@Test
	public void testJDBC() {
		try {
			Class.forName("eu.stratosphere.sql.jdbc.Driver");
			Connection connection = DriverManager.getConnection("jdbc:stratosphere:");
			Statement stmt = connection.createStatement();
			String sql = "SELECT COUNT(*) FROM departments";
			ResultSet rs = stmt.executeQuery(sql);
			int cnt = 0;
			while(rs.next()) {
				long r = rs.getLong(1);
				Assert.assertEquals(3L, r);
				cnt++;
			}
			Assert.assertEquals(1, cnt);
			
		} catch (Throwable e) {
			e.printStackTrace();
			Assert.fail("An error occured");
		}
	}
}
