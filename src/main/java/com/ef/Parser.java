package com.ef;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;

import java.text.ParseException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;


@SuppressWarnings("deprecation")
public class Parser {

	public void writeLogToMysql(String logFile) throws IOException {
		String url = Properties.url; 
		String user = Properties.user; 
		String password = Properties.password;
		try {
			Connection conn = DriverManager.getConnection(url, user, password);
			
			// hack to keep table out of duplication 
			PreparedStatement statement = conn.prepareStatement("TRUNCATE access_log");
			int row = statement.executeUpdate();
			if(row > 0)
				System.out.println("access_log table cleared");
			
			BufferedReader br = new BufferedReader(new FileReader(logFile));
			String sql = "INSERT INTO " + Properties.accessLogTable + " (date, ip, request, status, useragent) values (?, ?, ?, ?, ?)";
			statement = conn.prepareStatement(sql);
			String line;
			while((line = br.readLine()) != null){
				String [] ar = line.split("\\|");
				String date = ar[0];
				String ip = ar[1];
				String request = ar[2];
				String status = ar[3];
				String uA = ar[4];
				
				statement.setString(1, date);
				statement.setString(2, ip);
				statement.setString(3, request);
				statement.setString(4, status);
				statement.setString(5, uA);
				row = statement.executeUpdate();
//				if(row > 0)
//					System.out.println("inserted record to access_log");
			}
			br.close();
			conn.close();
		} catch (SQLException ex) {
			ex.printStackTrace();
		}    
	}
	
	public List<String> getBlockedIpsFromTable(String start, String end, String threshold) {
		
		List<String> lst = new ArrayList<>();
		try
		{	
			String url = Properties.url; 
			String user = Properties.user; 
			String password = Properties.password;
			
			Connection conn = DriverManager.getConnection(url, user, password);

			String query = "select ip, COUNT(ip) AS requests FROM " + Properties.accessLogTable +  
//					"WHERE date BETWEEN \"2017-01-01 15:00:00\" " +
					" WHERE date BETWEEN " + "\""+start+"\"" + " " +
//					"AND \"2017-01-01 15:59:59\" " + 
					"AND " + "\""+end+"\"" + " " +
					"GROUP BY (ip) " + 
					"HAVING requests > " + threshold;

			Statement st = conn.createStatement();

			ResultSet rs = st.executeQuery(query);

			while (rs.next()){
				String ip = rs.getString("ip");
				lst.add(String.valueOf(ip));
			}
			rs.close();
			st.close();
			conn.close();
		}
		catch (Exception e)
		{
			System.err.println(e.getMessage());
		}	
		return lst;
	}

	public void writeBlockedIpsToMysql(List<String> ips, String startDate, String duration, String threshold) {
		int numHours = duration.equals("hourly") ? 1 : 24;
		String tableName = duration.equals("hourly") ? Properties.blockedIpsHourlyTable : Properties.blockedIpsDailyTable;
		String url = Properties.url; 
		String user = Properties.user; 
		String password = Properties.password;
		try {
			Connection conn = DriverManager.getConnection(url, user, password);
			
			// hack to keep table out of duplication 
			PreparedStatement statement = conn.prepareStatement("TRUNCATE " + tableName);
			int row = statement.executeUpdate();
			if(row > 0)
				System.out.println(tableName + "table cleared");
			
			for(String ip : ips) {
				String sql = "INSERT INTO " + tableName + " (ip, comment) values (?, ?)";
				String comment = "exceeded " + threshold + " within " + String.valueOf(numHours) + 
						" hours starting " + startDate;
			   statement = conn.prepareStatement(sql);
				statement.setString(1, ip);
				statement.setString(2, comment);
				row = statement.executeUpdate();
				if(row > 0)
					System.out.println("inserted record to blocked ip table");
			} 
			conn.close(); 
		} catch (SQLException ex) {
			ex.printStackTrace();
		}     
	}
	
	public String addHours (String start, String plusHours) { //2017-01-01 15:00:10
		String [] ar = start.split("\\s+");
		String [] ar1 = ar[0].split("-");
		String [] ar2 = ar[1].split("\\:");
		LocalDateTime dateTimeStart = LocalDateTime.of(Integer.valueOf(ar1[0]), //year
				                                  Integer.valueOf(ar1[1]), //month
				                                  Integer.valueOf(ar1[2]), //day
				                                  Integer.valueOf(ar2[0]), //hour
				                                  Integer.valueOf(ar2[1]), //min
				                                  Integer.valueOf(ar2[2]));//sec
		
		LocalDateTime dateTimeEnd = dateTimeStart.plusHours(Integer.valueOf(plusHours));
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
		return dateTimeEnd.format(formatter);
	}
	
	@SuppressWarnings({ "static-access" })
	private static Options commandLineOptions() {
        Options options = new Options();
        options.addOption(OptionBuilder.withArgName("accesslog").hasArg()
                .withDescription("accesslog").create("accesslog"));
        options.addOption(OptionBuilder.withArgName("startDate").hasArg()
                .withDescription("startDate").create("startDate"));
        options.addOption(OptionBuilder.withArgName("duration").hasArg()
                .withDescription("duration").create("duration"));
        options.addOption(OptionBuilder.withArgName("threshold").hasArg()
                .withDescription("threshold").create("threshold"));
        return options;
    }

	public static void main(String[] args) throws IOException, ParseException, org.apache.commons.cli.ParseException {
		
		//java -cp "parser.jar" com.ef.Parser --accesslog=/path/to/file --startDate=2017-01-01.13:00:00 --duration=hourly --threshold=100
		
		String filePath = null; 
		String startDate = null;
		String duration = null;
		String threshold = null;
		
		CommandLineParser parser1 = new GnuParser();
		CommandLine line = parser1.parse(commandLineOptions(), args);
		if (line.hasOption("accesslog"))
			filePath = line.getOptionValue("accesslog");
		if (line.hasOption("startDate"))
			startDate = line.getOptionValue("startDate");
		if (line.hasOption("duration"))
			duration = line.getOptionValue("duration");
		if (line.hasOption("threshold"))
			threshold = line.getOptionValue("threshold");	
					
		String numHours = null;
		if(duration.equals("hourly"))
			numHours = "1";
		else if (duration.equals("daily"))
			numHours = "24";
		else {
			System.out.println("wrong duration");
			System.exit(1);
		}
		
		Parser parser = new Parser();
		
		parser.writeLogToMysql(filePath);
		String end = parser.addHours(startDate, numHours);
		List<String> blockedIps = parser.getBlockedIpsFromTable(startDate, end, threshold);
		System.out.println(blockedIps);
		parser.writeBlockedIpsToMysql(blockedIps, startDate, duration, threshold);
	}
}
