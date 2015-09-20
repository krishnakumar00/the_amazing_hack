package com.amazon.zama_edoc.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

public class CassandraAdapter
{
	private static Session session;

	private static Cluster cluster;

	static
	{
		cluster = Cluster.builder().addContactPoint("192.168.4.207").build();
		session = cluster.connect("item_details");
	}

	public static ResultSet executeQuery(String query)
	{
		try {
			return session.execute(query);
		}
		catch(Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	public static void close()
	{
		session.close();
		cluster.close();
	}

	public static void printResultSet(String query) {
		System.out.println("[exec] : "+query);
		ResultSet rs = executeQuery(query);
		System.out.println(rs.all());
	}

}