import java.sql.*;
import java.util.Scanner;

import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

import java.io.*;

public class myDBApp {

	public static ResultSet executeSelect(Connection connection, String query) {
		Statement st = null;
		
		try {
			st = connection.createStatement();
		} catch (SQLException e) {
			System.out.println("Failed to create statement object. Check output console");
			e.printStackTrace();
		}
		
		ResultSet rs = null;

		try {
			rs = st.executeQuery(query);
		} catch (SQLException e) {
			System.out.println("Failed to execute query object. Check output console");
			e.printStackTrace();
		}
		
		
		return rs;
	}
	
	public static Connection connectToDatabase(String user, String password, String database) {
		System.out.println("-------- PostgreSQL " + "JDBC Connection Testing ------------");
		
		Connection connection = null;
		
		try {
			connection = DriverManager.getConnection("jdbc:postgresql://teachdb.cs.rhul.ac.uk/CS2855/zfac005", user, password);
		} catch (SQLException e) {
			System.out.println("Connection Failed! Check output console");
			e.printStackTrace();
		}
		return connection;
	}
	
	public static void dropTable(Connection connection, String table) {
		Statement st = null;
		
		try {
			st = connection.createStatement();
		} catch (SQLException e) {
			System.out.println("Failed to create statement object. Check output console");
		}
		
		try {
			st.execute("DROP TABLE IF EXISTS " + table);
		} catch (SQLException e) {
			System.out.println("Failed to drop table. Check console output");
			e.printStackTrace();
		}
		
		try {
			st.close();
		} catch (SQLException e) {
			System.out.println("Failed to close the Statement object. Check console output");
			e.printStackTrace();
		}
		
	}
	
	public static void createTable(Connection connection, String tableDescription) {
		Statement st = null;
		
		try {
			st = connection.createStatement();
		} catch (SQLException e) {
			System.out.println("Failed to create statement object. Check output console");
		}

		try {
			st.execute("CREATE TABLE " + tableDescription);
		} catch (SQLException e) {
			System.out.println("Failed to create table. Check console output");
			e.printStackTrace();
		}
		
		try {
			st.close();
		} catch (SQLException e) {
			System.out.println("Failed to close the Statement object. Check console output");
			e.printStackTrace();
		}
		
	}
	
	
	public static long insertMappingTableFromFile(Connection connection, String table, String file) {
		
		BufferedReader br = null;
		int numRows = 0;
		
		String SQL = "INSERT INTO " + table + "(tldID, description) " + "VALUES(?,?)";
		try {
			PreparedStatement pstmt = connection.prepareStatement(SQL);
			String brokenLine[], sCurrentLine = "";
			br = new BufferedReader(new FileReader(file));

			while ((sCurrentLine = br.readLine()) != null) {
				// Insert each line to the DB
				brokenLine = sCurrentLine.split("\t");
				pstmt.setString(1, brokenLine[0]);
				pstmt.setString(2, brokenLine[1]);
				numRows = pstmt.executeUpdate();
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (br != null)
					br.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
		
		return numRows;
	}
	
	public static long insertTopURLsTableFromFile(Connection connection, String table, String file) {	

		BufferedReader br = null;
		int numRows = 0;
		
		try {
			Statement st = connection.createStatement();
			String sCurrentLine, brokenLine[], composedLine = "";
			br = new BufferedReader(new FileReader(file));

			while ((sCurrentLine = br.readLine()) != null) {
				// Insert each line to the DB
				brokenLine = sCurrentLine.split("\t");
				
				if (brokenLine.length > 4) {
					continue;
				}
				composedLine = "INSERT INTO " + table + " VALUES (";
				int i;
				for (i = 0; i < 3; i++) {
					composedLine += "'" + brokenLine[i] + "',";
				}
				if (brokenLine.length == 3) {
					composedLine += "'" + "null" + "',";
					composedLine += "'" + brokenLine[2] + "')";
				} else if (brokenLine.length == 4) {
					composedLine += "'" + brokenLine[3] + "',";
					composedLine += "'" + brokenLine[3] + "')";
				}
				numRows = st.executeUpdate(composedLine);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				if (br != null)
					br.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
		
		return numRows;
	}
	
	public static void main(String[] args) {
		
		String port = "";
		
		System.out.println("Please enter your username: ");
		Scanner out1 = new Scanner(System.in);
		String user = out1.nextLine();
		
		System.out.println("Now enter your password: ");
		Scanner out2 = new Scanner(System.in);
		String password = out2.nextLine();
		
		//the name of the DB we have been using
		String database = "teachdb.cs.rhul.ac.uk";
		
		Connection connection = connectToDatabase(user, password, database);
			
		if (connection != null) {
			System.out.println("You made it, take control your database now!");
		} else {
			System.out.println("Failed to make connection!");
			return;
		}
		//Now we're ready to work on the DB
		
		dropTable(connection, "topURLs");
		dropTable(connection, "mapping");
		
		createTable(connection, "topURLs(ranking int NOT NULL PRIMARY KEY,urlPartOne varchar(80) NOT NULL,urlPartTwo varchar(50),urlPartThree varchar(10), urlEnding varchar(10));");
		createTable(connection, "mapping(tldID varchar(10) NOT NULL PRIMARY KEY,description varchar(120) NOT NULL);");
		
		insertMappingTableFromFile(connection, "mapping", "src/mapping");			
		insertTopURLsTableFromFile(connection, "topURLs", "src/TopURLs");

		ResultSet rs;
		int counter;
		String query;
		
		//1st Query
		System.out.println("\n ################# 1st Query ################");
		query = "SELECT ranking,urlpartone,urlparttwo FROM topURLs ORDER BY ranking";
		rs = executeSelect(connection, query);
		counter = 0;
		try {
			while(rs.next() & counter < 10) {
				System.out.println(rs.getString(1)+"\t"+rs.getString(2)+"\t\t"+rs.getString(3));
				counter++;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		try {
			rs.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		
		//2nd Query
		System.out.println("\n ################# 2nd Query ################");
		
		try {
			//prepare sql statement
			PreparedStatement pstmt = connection.prepareStatement("select urlparttwo, urlpartthree from topurls group by urlparttwo, urlpartthree order by min(ranking)");
		
			//execute SQL
			rs = pstmt.executeQuery();
		} catch(SQLException e) {
			e.printStackTrace();
		}
		
		counter = 0;
		
		try {
			while(rs.next() & counter < 10) {
				System.out.println(rs.getString(1)+"\t"+rs.getString(2));
				counter++;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		try {
			rs.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		
		//3rd query
		System.out.println("################## 3rd Query ###############");
        
		// prepare and execute SQL
		try {
			PreparedStatement pstmt0 = connection.prepareStatement("select urlEnding from topurls group by urlEnding order by min(ranking)");
			rs = pstmt0.executeQuery();
			int i=0;
	        // get all records
	        while (rs.next()) {
	            // read data and print
	            String urlEnding = rs.getString("urlEnding");
	            PreparedStatement pstmt1 = connection.prepareStatement("select description from mapping where tldID=?");
	            pstmt1.setString(1, urlEnding);
	            ResultSet rs1 = pstmt1.executeQuery();
	            if(rs1.next()){
	                System.out.println("\t"+rs1.getString("description"));
	                i++;
	            }
	            if(i==10) break;
	        }
	        rs.close();
	        System.out.println();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		
		//4th Query
		System.out.println("################## 4th Query ############### ");
        // prepare and execute sql
		try {
	        PreparedStatement pstmt = connection.prepareStatement("select urlpartone from topurls  group by urlpartone having count(urlpartone)>1 order by min(ranking)");
	        rs = pstmt.executeQuery();
	        // get all records
	        counter = 0;
	        while (rs.next() && counter < 10) {
	            // read data and print
	            String urlpartone = rs.getString("urlpartone");
	            System.out.println(String.format("\t\t%s", urlpartone));
	            counter++;
	        }
		} catch (SQLException e) {
			e.printStackTrace();
		}
		System.out.println();
		
	}

}
