package test;

import java.awt.List;
import java.math.BigInteger;
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Test {

	public static int numberOfUsers = 1000;
	public static int numberOfBlogs = 1000;
	public static int numberOfComments = 1000;
	public static int numberOfLikes = 1000;

	public static void main(String[] args) {

		// Connection Details
		String urlNormalized = "jdbc:mysql://192.168.122.244:3306/normalized";
		String urlDenormalized = "jdbc:mysql://192.168.122.244:3306/denormalized";
		String user = "testuser";
		String password = "test1234";

		long startTime = System.nanoTime();
		ArrayList<HashMap<String, String>> result = selectBlogWithAssociatesNormalized(
				urlNormalized, user, password);
		long estimatedTime = System.nanoTime() - startTime;
		double seconds = (double) estimatedTime / 1000000000.0;
		System.out.println(result);
		System.out.println("Duration: " + seconds);		
		
//		long startTime = System.nanoTime();
//		ArrayList<HashMap<String, String>> result = selectBlogWithAssociatesDenormalized(
//				urlDenormalized, user, password);
//		long estimatedTime = System.nanoTime() - startTime;
//		double seconds = (double) estimatedTime / 1000000000.0;
//		System.out.println(result);
//		System.out.println("Duration: " + seconds);
		
//		 executeInsertDenormalized(urlDenormalized, user, password);
//		 executeInsertNormalized(urlNormalized, user, password);
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

	public static void executeInsertDenormalized(String url, String user,
			String password) {

		Connection con = null;
		PreparedStatement pst = null;
		ResultSet rs = null;

		try {
			// Create Connection
			con = DriverManager.getConnection(url, user, password);

			// Helper Variable
			int count;
			Map<String, String> resultSet = null;
			Map<String, String> resultSetComment = null;
			Map<String, String> resultSetUser = null;

			try {
				con.setAutoCommit(false);
				con.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
			} catch (SQLException e) {
				e.printStackTrace();
			}

			// Insert User
			for (count = 0; count < numberOfUsers; count++) {
				String insertUserStmt = "insert into User(vorname, nachname, email) values(?, ?, ?)";
				PreparedStatement preparedUserStmt = con
						.prepareStatement(insertUserStmt);
				preparedUserStmt.setString(1, randomText(100));
				preparedUserStmt.setString(2, randomText(150));
				preparedUserStmt.setString(3, randomText(120) + "@"
						+ randomText(20) + "." + randomText(10));
				preparedUserStmt.execute();
			}

			// Insert Blog
			for (count = 0; count < numberOfBlogs; count++) {
				// get existing user
				resultSet = selectUserById(url, user, password,
						randomNumber(1, numberOfUsers), con);
				String insertBlogStmt = "insert into Blog(blogpost, u_id, u_vorname, u_nachname, u_email) values(?,?,?,?,?)";
				PreparedStatement preparedBlogStmt = con
						.prepareStatement(insertBlogStmt);
				preparedBlogStmt.setString(1, randomText(5000));
				preparedBlogStmt.setInt(2, Integer.valueOf(resultSet.get("id")));
				preparedBlogStmt.setString(3, resultSet.get("vorname"));
				preparedBlogStmt.setString(4, resultSet.get("nachname"));
				preparedBlogStmt.setString(5, resultSet.get("email"));
				preparedBlogStmt.execute();

			}

			// Insert Comment
			for (count = 0; count < numberOfComments; count++) {
				String insertCommentStmt = "insert into Comment(comment, u_id, u_vorname, u_nachname, u_email, b_id, b_blogpost, b_user_id, b_vorname, b_nachname, b_email) values(?,?,?,?,?,?,?,?,?,?,?)";
				PreparedStatement preparedCommentStmt = con.prepareStatement(insertCommentStmt);
				preparedCommentStmt.setString(1, randomText(2000));
				// get existing blog and user
				resultSet = selectUserById(url, user, password,randomNumber(1, numberOfUsers), con);
				preparedCommentStmt.setInt(2, Integer.valueOf(resultSet.get("id")));
				preparedCommentStmt.setString(3, resultSet.get("vorname"));
				preparedCommentStmt.setString(4, resultSet.get("nachname"));
				preparedCommentStmt.setString(5, resultSet.get("email"));
				resultSet = selectBlogByIdDenormalized(url, user, password,randomNumber(1, numberOfBlogs), con);
				preparedCommentStmt.setInt(6,Integer.valueOf(resultSet.get("id")));
				preparedCommentStmt.setString(7, resultSet.get("blogpost"));
				preparedCommentStmt.setInt(8, Integer.valueOf(resultSet.get("u_id")));
				preparedCommentStmt.setString(9, resultSet.get("u_vorname"));
				preparedCommentStmt.setString(10, resultSet.get("u_nachname"));
				preparedCommentStmt.setString(11, resultSet.get("u_email"));	
				preparedCommentStmt.execute();
			}

			// Insert Likes
			for (count = 0; count < numberOfLikes; count++) {
				String insertLikeStmt = " insert into Likes(u_id, u_vorname, u_nachname, u_email, c_id, c_comment, c_user_id, c_vorname, c_nachname, c_email, "
						+ "b_id, b_blogpost, b_user_id, b_vorname, b_nachname, b_email) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
				
				PreparedStatement preparedLikeStmt = con.prepareStatement(insertLikeStmt);
				// get existing user
				resultSetUser = selectUserById(url, user, password,randomNumber(1, numberOfUsers), con);
				preparedLikeStmt.setInt(1,Integer.valueOf(resultSetUser.get("id")));
				preparedLikeStmt.setString(2, resultSetUser.get("vorname"));
				preparedLikeStmt.setString(3, resultSetUser.get("nachname"));
				preparedLikeStmt.setString(4, resultSetUser.get("email"));
				// get existing comment
				resultSetComment = selectCommentByIdDenormalized(url, user,password, randomNumber(1, numberOfComments), con);
				preparedLikeStmt.setInt(5,Integer.valueOf(resultSetComment.get("id")));
				preparedLikeStmt.setString(6, resultSetComment.get("comment"));
				preparedLikeStmt.setInt(7,Integer.valueOf(resultSetComment.get("u_id")));
				preparedLikeStmt.setString(8, resultSetComment.get("u_vorname"));
				preparedLikeStmt.setString(9, resultSetComment.get("u_nachname"));
				preparedLikeStmt.setString(10, resultSetComment.get("u_email"));
				preparedLikeStmt.setInt(11,Integer.valueOf(resultSetComment.get("b_id")));
				preparedLikeStmt.setString(12, resultSetComment.get("b_blogpost"));
				preparedLikeStmt.setInt(13,Integer.valueOf(resultSetComment.get("b_user_id")));
				preparedLikeStmt.setString(14, resultSetComment.get("b_vorname"));
				preparedLikeStmt.setString(15, resultSetComment.get("b_nachname"));
				preparedLikeStmt.setString(16, resultSetComment.get("b_email"));
				try {
					preparedLikeStmt.execute();
				} catch (SQLException ex) {
					if (ex instanceof SQLIntegrityConstraintViolationException) {
						System.out.println(preparedLikeStmt.toString());
						System.out.println("Duplicate Key Error --> u_id = " + resultSetUser.get("id") + " c_id = " + resultSetComment.get("id"));
					} else {
						Logger lgr = Logger.getLogger(Test.class.getName());
						lgr.log(Level.SEVERE, ex.getMessage(), ex);
					}
				}
			}

			con.commit();

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

	public static void executeInsertNormalized(String url, String user,
			String password) {

		Connection con = null;
		PreparedStatement pst = null;
		ResultSet rs = null;

		try {
			// Create Connection
			con = DriverManager.getConnection(url, user, password);

			// Helpver Variable
			int count;

			try {
				con.setAutoCommit(false);
				con.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
			} catch (SQLException e) {
				e.printStackTrace();
			}

			// Insert User
			for (count = 0; count < numberOfUsers; count++) {
				String insertUserStmt = "insert into User(vorname, nachname, email) values(?, ?, ?)";
				PreparedStatement preparedUserStmt = con
						.prepareStatement(insertUserStmt);
				preparedUserStmt.setString(1, randomText(100));
				preparedUserStmt.setString(2, randomText(150));
				preparedUserStmt.setString(3, randomText(120) + "@"
						+ randomText(20) + "." + randomText(10));
				preparedUserStmt.execute();
			}

			// Insert Blog
			for (count = 0; count < numberOfBlogs; count++) {
				String insertBlogStmt = "insert into Blog(blogpost, user_id) values(?,?)";
				PreparedStatement preparedBlogStmt = con
						.prepareStatement(insertBlogStmt);
				preparedBlogStmt.setString(1, randomText(5000));
				preparedBlogStmt.setInt(2, randomNumber(1, numberOfUsers));
				preparedBlogStmt.execute();
			}

			// Insert Comment
			for (count = 0; count < numberOfComments; count++) {
				String insertCommentStmt = "insert into Comment(text, user_id, blog_id) values(?,?,?)";
				PreparedStatement preparedCommentStmt = con
						.prepareStatement(insertCommentStmt);
				preparedCommentStmt.setString(1, randomText(2000));
				preparedCommentStmt.setInt(2, randomNumber(1, numberOfUsers));
				preparedCommentStmt.setInt(3, randomNumber(1, numberOfBlogs));
				preparedCommentStmt.execute();
			}

			// Insert Likes
			for (count = 0; count < numberOfLikes; count++) {
				String insertLikeStmt = "insert into Likes(user_id, comment_id) values(?,?)";
				PreparedStatement preparedLikeStmt = con
						.prepareStatement(insertLikeStmt);
				preparedLikeStmt.setInt(1, randomNumber(1, numberOfUsers));
				preparedLikeStmt.setInt(2, randomNumber(1, numberOfComments));
				try {
					preparedLikeStmt.execute();
				} catch (SQLException ex) {
					if (ex instanceof SQLIntegrityConstraintViolationException) {
						System.out.println("Duplicate Key Error");
					} else {
						Logger lgr = Logger.getLogger(Test.class.getName());
						lgr.log(Level.SEVERE, ex.getMessage(), ex);
					}
				}
			}

			con.commit();

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

	// Helper Method
	private static Map<String, String> selectUserById(String url, String user,
			String password, int id, Connection con) {

		// Connection con = null;
		PreparedStatement pst = null;
		ResultSet rs = null;
		Map<String, String> resultSet = null;

		try {
			// Create Connection
			// con = DriverManager.getConnection(url, user, password);

			// Create Prepared Query Statement
			pst = con
					.prepareStatement("select id, vorname, nachname, email from User where id=?;");
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
				// if (con != null) {
				// con.close();
				// }

			} catch (SQLException ex) {
				Logger lgr = Logger.getLogger(Test.class.getName());
				lgr.log(Level.WARNING, ex.getMessage(), ex);
			}
		}

		return resultSet;
	}

	// Helper Method
	private static Map<String, String> selectBlogByIdDenormalized(String url,
			String user, String password, int id, Connection con) {

		// Connection con = null;
		PreparedStatement pst = null;
		ResultSet rs = null;
		Map<String, String> resultSet = null;

		try {
			// Create Connection
			// con = DriverManager.getConnection(url, user, password);

			// Create Prepared Query Statement
			pst = con
					.prepareStatement("select id, blogpost, u_id, u_vorname, u_nachname, u_email from Blog where id=?;");
			pst.setInt(1, id);

			// Execute Query
			rs = pst.executeQuery();

			// Loop through Result and build Result Set
			resultSet = new HashMap<String, String>();
			while (rs.next()) {
				resultSet.put("id", String.valueOf(rs.getInt(1)));
				resultSet.put("blogpost", rs.getString(2));
				resultSet.put("u_id", String.valueOf(rs.getInt(3)));
				resultSet.put("u_vorname", rs.getString(4));
				resultSet.put("u_nachname", rs.getString(5));
				resultSet.put("u_email", rs.getString(6));
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
				// if (con != null) {
				// con.close();
				// }

			} catch (SQLException ex) {
				Logger lgr = Logger.getLogger(Test.class.getName());
				lgr.log(Level.WARNING, ex.getMessage(), ex);
			}
		}

		return resultSet;
	}

	// Helper Method
	private static Map<String, String> selectCommentByIdDenormalized(
			String url, String user, String password, int id, Connection con) {

		// Connection con = null;
		PreparedStatement pst = null;
		ResultSet rs = null;
		Map<String, String> resultSet = null;

		try {
			// Create Connection
			// con = DriverManager.getConnection(url, user, password);

			// Create Prepared Query Statement
			pst = con
					.prepareStatement("select id, comment, u_id, u_vorname, u_nachname, u_email, "
							+ "b_id, b_blogpost, b_user_id, b_vorname, b_nachname, b_email from Comment where id=?;");
			pst.setInt(1, id);

			// Execute Query
			rs = pst.executeQuery();

			// Loop through Result and build Result Set
			resultSet = new HashMap<String, String>();
			while (rs.next()) {
				resultSet.put("id", String.valueOf(rs.getInt(1)));
				resultSet.put("comment", rs.getString(2));
				resultSet.put("u_id", String.valueOf(rs.getInt(3)));
				resultSet.put("u_vorname", rs.getString(4));
				resultSet.put("u_nachname", rs.getString(5));
				resultSet.put("u_email", rs.getString(6));
				resultSet.put("b_id", String.valueOf(rs.getInt(7)));
				resultSet.put("b_blogpost", rs.getString(8));
				resultSet.put("b_user_id", String.valueOf(rs.getInt(9)));
				resultSet.put("b_vorname", rs.getString(10));
				resultSet.put("b_nachname", rs.getString(11));
				resultSet.put("b_email", rs.getString(12));
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
				// if (con != null) {
				// con.close();
				// }

			} catch (SQLException ex) {
				Logger lgr = Logger.getLogger(Test.class.getName());
				lgr.log(Level.WARNING, ex.getMessage(), ex);
			}
		}

		return resultSet;
	}

	public static ArrayList<HashMap<String, String>> selectBlogWithAssociatesDenormalized(
			String url, String user, String password) {

		Connection con = null;
		PreparedStatement pst = null;
		ResultSet rs = null;
		ArrayList<HashMap<String, String>> result = new ArrayList<HashMap<String, String>>();

		try {
			// Create Connection
			con = DriverManager.getConnection(url, user, password);

			int i;
			for (i = 1; i <= numberOfBlogs; i++) {
				
				try {
					// Create Prepared Query Statement
					pst = con
							.prepareStatement("select * from Likes where b_id=?;");
					pst.setInt(1, i);

					// Execute Query
					rs = pst.executeQuery();

					// Loop through Result and build Result Set
					HashMap<String, String> resultSet = new HashMap<String, String>();
					while (rs.next()) {
						resultSet.put("id", String.valueOf(rs.getInt(1)));
					}
					result.add(resultSet);

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

					} catch (SQLException ex) {
						Logger lgr = Logger.getLogger(Test.class.getName());
						lgr.log(Level.WARNING, ex.getMessage(), ex);
					}
				}
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (con != null) {
				try {
					con.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

		}

		return result;

	}

	public static ArrayList<HashMap<String, String>> selectBlogWithAssociatesNormalized(
			String url, String user, String password) {

		Connection con = null;
		PreparedStatement pst = null;
		ResultSet rs = null;
		ArrayList<HashMap<String, String>> result = new ArrayList<HashMap<String, String>>();

		try {
			// Create Connection
			con = DriverManager.getConnection(url, user, password);

			int i;
			for (i = 1; i <= numberOfBlogs; i++) {
				
				try {
					// Create Prepared Query Statement
					pst = con
							.prepareStatement("SELECT id_blog, blogpost as blog_blogpost, user_id as blog_user_id, vorname as blog_vorname, nachname as blog_nachname, email as blog_email, comment_text, comment_user_id, comment_blog_id, comment_vorname, comment_nachname, comment_email, like_user_id, like_comment_id, like_vorname, like_nachname, like_email FROM ( "
									+ " SELECT text as comment_text,user_id as comment_user_id,blog_id as comment_blog_id,vorname as comment_vorname,nachname as comment_nachname,email as comment_email,like_user_id,like_comment_id,like_vorname,like_nachname,like_email FROM ( "
									+ " SELECT user_id as like_user_id,comment_id as like_comment_id,vorname as like_vorname,nachname as like_nachname,email as like_email FROM Likes JOIN User ON Likes.user_id = User.id_user "
									+ " ) AS LU RIGHT JOIN Comment ON LU.like_comment_id = Comment.id_comment JOIN User ON Comment.user_id = User.id_user "
									+ " ) AS CLU RIGHT JOIN Blog ON CLU.comment_blog_id = Blog.id_blog JOIN User ON Blog.user_id = User.id_user "
									+ " where id_blog=?; ");
					pst.setInt(1, i);

					// Execute Query
					rs = pst.executeQuery();

					// Loop through Result and build Result Set
					HashMap<String, String> resultSet = new HashMap<String, String>();
					while (rs.next()) {
						resultSet.put("id", String.valueOf(rs.getInt(1)));
					}
					result.add(resultSet);

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

					} catch (SQLException ex) {
						Logger lgr = Logger.getLogger(Test.class.getName());
						lgr.log(Level.WARNING, ex.getMessage(), ex);
					}
				}
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (con != null) {
				try {
					con.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

		}

		return result;

	}
}