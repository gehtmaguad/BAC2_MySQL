package test;

import java.math.BigInteger;
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Test {

	public static void main(String[] args) {

		// Connection Details
		String urlNormalized = "jdbc:mysql://192.168.122.244:3306/normalized";
		String urlDenormalized = "jdbc:mysql://192.168.122.244:3306/denormalized";
		String user = "testuser";
		String password = "test1234";

		//executeInsertDenormalized(urlDenormalized, user, password);
		//executeInsertNormalized(urlNormalized, user, password);
	}

	public static String randomText(Integer bits) {

		SecureRandom random = new SecureRandom();
		String randomString = new BigInteger(bits, random).toString(32);
		return randomString;

	}

	public static int randomNumber(Integer min, Integer max) {

		Random rand = new Random();
		int randomNum = rand.nextInt((max - min) + 1) + min;
		return randomNum;
	}
	
	public static void executeInsertDenormalized(String url, String user, String password) {
		
		Connection con = null;
		PreparedStatement pst = null;
		ResultSet rs = null;

		try {
			// Create Connection
			con = DriverManager.getConnection(url, user, password);

			//Helpver Variable
			int count;
			int numberOfUsers = 1000;
			int numberOfBlogs = 1000;
			int numberOfComments = 1000;
			int numberOfLikes = 1000;
			Map<String, String> resultSet = null;
			Map<String, String> resultSetComment = null;
			Map<String, String> resultSetUser = null;
			
			// Insert User
			for (count = 0; count < numberOfUsers; count++) {
				String insertUserStmt = "insert into User(vorname, nachname, email) values(?, ?, ?)";
				PreparedStatement preparedUserStmt = con.prepareStatement(insertUserStmt);
				preparedUserStmt.setString(1, randomText(100));
				preparedUserStmt.setString(2, randomText(150));
				preparedUserStmt.setString(3, randomText(120) + "@"+ randomText(20) + "." + randomText(10));
				preparedUserStmt.execute();
			}

			// Insert Blog
			for (count = 0; count < numberOfBlogs; count++) {
				String insertBlogStmt = "insert into Blog(blogpost, user_id, vorname, nachname, email) values(?,?,?,?,?)";
				PreparedStatement preparedBlogStmt = con.prepareStatement(insertBlogStmt);
				preparedBlogStmt.setString(1, randomText(5000));
				// get existing user
				resultSet = selectUserById(url, user, password, randomNumber(1, numberOfUsers));
				preparedBlogStmt.setInt(2, Integer.valueOf(resultSet.get("id")));
				preparedBlogStmt.setString(3, resultSet.get("vorname"));
				preparedBlogStmt.setString(4, resultSet.get("nachname"));
				preparedBlogStmt.setString(5, resultSet.get("email"));				
				preparedBlogStmt.execute();
			}
			
			// Insert Comment
			for (count = 0; count < numberOfComments; count++) {
				String insertCommentStmt = "insert into Comment(text, user_id, vorname, nachname, email, blog_id, blogpost) values(?,?,?,?,?,?,?)";
				PreparedStatement preparedCommentStmt = con.prepareStatement(insertCommentStmt);
				preparedCommentStmt.setString(1, randomText(2000));
				// get existing blog
				resultSet = selectBlogByIdDenormalized(url, user, password, randomNumber(1, numberOfBlogs));
				preparedCommentStmt.setInt(2, Integer.valueOf(resultSet.get("user_id")));
				preparedCommentStmt.setString(3, resultSet.get("vorname"));
				preparedCommentStmt.setString(4, resultSet.get("nachname"));
				preparedCommentStmt.setString(5, resultSet.get("email"));
				preparedCommentStmt.setInt(6, Integer.valueOf(resultSet.get("id")));
				preparedCommentStmt.setString(7, resultSet.get("blogpost"));
				preparedCommentStmt.execute();				
			}
			
			// Insert Likes
			for (count = 0; count < numberOfLikes; count++) {
				String insertLikeStmt = " insert into Likes(user_id, comment_id, vorname, nachname, email, text, blog_id, blogpost) values(?,?,?,?,?,?,?,?)";
				PreparedStatement preparedLikeStmt = con.prepareStatement(insertLikeStmt);
				// get existing comment
				resultSetComment = selectCommentByIdDenormalized(url, user, password, randomNumber(1, numberOfBlogs));
				// get existing user
				resultSetUser = selectUserById(url, user, password, randomNumber(1, numberOfUsers));
				preparedLikeStmt.setInt(1, Integer.valueOf(resultSetUser.get("id")));
				preparedLikeStmt.setInt(2, Integer.valueOf(resultSetComment.get("id")));
				preparedLikeStmt.setString(3, resultSetUser.get("vorname"));
				preparedLikeStmt.setString(4, resultSetUser.get("nachname"));
				preparedLikeStmt.setString(5, resultSetUser.get("email"));
				preparedLikeStmt.setString(6, resultSetComment.get("text"));
				preparedLikeStmt.setInt(7, Integer.valueOf(resultSetComment.get("blog_id")));
				preparedLikeStmt.setString(8, resultSetComment.get("blogpost"));
				try {
					preparedLikeStmt.execute();
				} catch (SQLException ex) {
					if (ex instanceof SQLIntegrityConstraintViolationException) {
						String dummy = "ignore";
						System.out.println("Duplicate Key Error");
					} else {
						Logger lgr = Logger.getLogger(Test.class.getName());
						lgr.log(Level.SEVERE, ex.getMessage(), ex);
					}
				}			
			}				
			
		} catch (SQLException ex) {
			Logger lgr = Logger.getLogger(Test.class.getName());
			lgr.log(Level.SEVERE, ex.getMessage(), ex);

		} finally {
			try {
				if (rs != null) {
					rs.close();
				}
				if (pst != null) {
					pst.close();
				}
				if (con != null) {
					con.close();
				}

			} catch (SQLException ex) {
				Logger lgr = Logger.getLogger(Test.class.getName());
				lgr.log(Level.WARNING, ex.getMessage(), ex);
			}
		}

	}

	public static void executeInsertNormalized(String url, String user, String password) {

		Connection con = null;
		PreparedStatement pst = null;
		ResultSet rs = null;

		try {
			// Create Connection
			con = DriverManager.getConnection(url, user, password);

			//Helpver Variable
			int count;
			int numberOfUsers = 1000;
			int numberOfBlogs = 1000;
			int numberOfComments = 1000;
			int numberOfLikes = 1000;			
			
			// Insert User
			for (count = 0; count < numberOfUsers; count++) {
				String insertUserStmt = "insert into User(vorname, nachname, email) values(?, ?, ?)";
				PreparedStatement preparedUserStmt = con.prepareStatement(insertUserStmt);
				preparedUserStmt.setString(1, randomText(100));
				preparedUserStmt.setString(2, randomText(150));
				preparedUserStmt.setString(3, randomText(120) + "@"+ randomText(20) + "." + randomText(10));
				preparedUserStmt.execute();
			}

			// Insert Blog
			for (count = 0; count < numberOfBlogs; count++) {
				String insertBlogStmt = "insert into Blog(blogpost, user_id) values(?,?)";
				PreparedStatement preparedBlogStmt = con.prepareStatement(insertBlogStmt);
				preparedBlogStmt.setString(1, randomText(5000));
				preparedBlogStmt.setInt(2, randomNumber(1, numberOfUsers));
				preparedBlogStmt.execute();
			}
			
			// Insert Comment
			for (count = 0; count < numberOfComments; count++) {
				String insertCommentStmt = "insert into Comment(text, user_id, blog_id) values(?,?,?)";
				PreparedStatement preparedCommentStmt = con.prepareStatement(insertCommentStmt);
				preparedCommentStmt.setString(1, randomText(2000));
				preparedCommentStmt.setInt(2, randomNumber(1, numberOfUsers));
				preparedCommentStmt.setInt(3, randomNumber(1, numberOfBlogs));
				preparedCommentStmt.execute();				
			}
			
			// Insert Likes
			for (count = 0; count < numberOfLikes; count++) {
				String insertLikeStmt = "insert into Likes(user_id, comment_id) values(?,?)";
				PreparedStatement preparedLikeStmt = con.prepareStatement(insertLikeStmt);
				preparedLikeStmt.setInt(1, randomNumber(1, numberOfUsers));
				preparedLikeStmt.setInt(2, randomNumber(1, numberOfComments));
				try {
					preparedLikeStmt.execute();
				} catch (SQLException ex) {
					if (ex instanceof SQLIntegrityConstraintViolationException) {
						String dummy = "ignore";
					} else {
						Logger lgr = Logger.getLogger(Test.class.getName());
						lgr.log(Level.SEVERE, ex.getMessage(), ex);
					}
				}			
			}			
			
		} catch (SQLException ex) {
			Logger lgr = Logger.getLogger(Test.class.getName());
			lgr.log(Level.SEVERE, ex.getMessage(), ex);

		} finally {
			try {
				if (rs != null) {
					rs.close();
				}
				if (pst != null) {
					pst.close();
				}
				if (con != null) {
					con.close();
				}

			} catch (SQLException ex) {
				Logger lgr = Logger.getLogger(Test.class.getName());
				lgr.log(Level.WARNING, ex.getMessage(), ex);
			}
		}

	}

	public static Map<String, String> selectUserById(String url, String user, String password, int id) {

		Connection con = null;
		PreparedStatement pst = null;
		ResultSet rs = null;
		Map<String, String> resultSet= null;

		try {
			// Create Connection
			con = DriverManager.getConnection(url, user, password);

			// Create Prepared Query Statement
			pst = con.prepareStatement("select id, vorname, nachname, email from User where id=?;");
			pst.setInt(1, id);	
			
			// Execute Query
			rs = pst.executeQuery();

			// Loop through Result and build Result Set
			resultSet = new HashMap<String, String>();
			while (rs.next()) {
				resultSet.put("id", String.valueOf(rs.getInt(1)));
				resultSet.put("vorname", rs.getString(2));
				resultSet.put("nachname", rs.getString(3));
				resultSet.put("email", rs.getString(4));
			}

		} catch (SQLException ex) {
			Logger lgr = Logger.getLogger(Test.class.getName());
			lgr.log(Level.SEVERE, ex.getMessage(), ex);

		} finally {
			try {
				if (rs != null) {
					rs.close();
				}
				if (pst != null) {
					pst.close();
				}
				if (con != null) {
					con.close();
				}

			} catch (SQLException ex) {
				Logger lgr = Logger.getLogger(Test.class.getName());
				lgr.log(Level.WARNING, ex.getMessage(), ex);
			}
		}
		
		return resultSet;
	}

	public static Map<String, String> selectBlogByIdDenormalized(String url, String user, String password, int id) {
		
		Connection con = null;
		PreparedStatement pst = null;
		ResultSet rs = null;
		Map<String, String> resultSet= null;
		
		try {
			// Create Connection
			con = DriverManager.getConnection(url, user, password);

			// Create Prepared Query Statement
			pst = con.prepareStatement("select id, blogpost, user_id, vorname, nachname, email from Blog where id=?;");
			pst.setInt(1, id);
			
			// Execute Query
			rs = pst.executeQuery();

			// Loop through Result and build Result Set
			resultSet = new HashMap<String, String>();
			while (rs.next()) {
				resultSet.put("id", String.valueOf(rs.getInt(1)));
				resultSet.put("blogpost", rs.getString(2));
				resultSet.put("user_id", String.valueOf(rs.getInt(3)));
				resultSet.put("vorname", rs.getString(4));
				resultSet.put("nachname", rs.getString(5));
				resultSet.put("email", rs.getString(6));
			}

		} catch (SQLException ex) {
			Logger lgr = Logger.getLogger(Test.class.getName());
			lgr.log(Level.SEVERE, ex.getMessage(), ex);

		} finally {
			try {
				if (rs != null) {
					rs.close();
				}
				if (pst != null) {
					pst.close();
				}
				if (con != null) {
					con.close();
				}

			} catch (SQLException ex) {
				Logger lgr = Logger.getLogger(Test.class.getName());
				lgr.log(Level.WARNING, ex.getMessage(), ex);
			}
		}		
		
		return resultSet;
	}

	public static Map<String, String> selectCommentByIdDenormalized(String url, String user, String password, int id) {
		
		Connection con = null;
		PreparedStatement pst = null;
		ResultSet rs = null;
		Map<String, String> resultSet= null;
		
		try {
			// Create Connection
			con = DriverManager.getConnection(url, user, password);

			// Create Prepared Query Statement
			pst = con.prepareStatement("select id, text, user_id, vorname, nachname, email, blog_id, blogpost from Comment where id=?;");
			pst.setInt(1, id);
			
			// Execute Query
			rs = pst.executeQuery();

			// Loop through Result and build Result Set
			resultSet = new HashMap<String, String>();
			while (rs.next()) {
				resultSet.put("id", String.valueOf(rs.getInt(1)));
				resultSet.put("text", rs.getString(2));
				resultSet.put("user_id", String.valueOf(rs.getInt(3)));
				resultSet.put("vorname", rs.getString(4));
				resultSet.put("nachname", rs.getString(5));
				resultSet.put("email", rs.getString(6));
				resultSet.put("blog_id", String.valueOf(rs.getInt(7)));
				resultSet.put("blogpost", rs.getString(8));
			}

		} catch (SQLException ex) {
			Logger lgr = Logger.getLogger(Test.class.getName());
			lgr.log(Level.SEVERE, ex.getMessage(), ex);

		} finally {
			try {
				if (rs != null) {
					rs.close();
				}
				if (pst != null) {
					pst.close();
				}
				if (con != null) {
					con.close();
				}

			} catch (SQLException ex) {
				Logger lgr = Logger.getLogger(Test.class.getName());
				lgr.log(Level.WARNING, ex.getMessage(), ex);
			}
		}		
		
		return resultSet;
	}
}