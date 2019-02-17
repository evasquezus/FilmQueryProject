package com.skilldistillery.filmquery.database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import com.skilldistillery.filmquery.entities.Actor;
import com.skilldistillery.filmquery.entities.Film;

public class DatabaseAccessorObject implements DatabaseAccessor {

	private static final String URL = "jdbc:mysql://localhost:3306/sdvid?useSSL=false";
	private static final String user = "student";
	private static final String pass = "student";
	static {
		try {
			Class.forName("com.mysql.jdbc.Driver");

		} catch (ClassNotFoundException e) {
			System.err.println(e);
		}

	}

//	public DatabaseAccessorObject() throws ClassNotFoundException {
//		Class.forName("com.mysql.jdbc.Driver");
//	}

	public Film findFilmById(int filmID) {
		Film film = null;
		try {
			Connection conn = DriverManager.getConnection(URL, user, pass);
			String sql = "SELECT id, title, description, rating, release_year, rental_rate, language_id,replacement_cost, length,rental_duration FROM film WHERE id = ?";
			PreparedStatement stmt = conn.prepareStatement(sql);
			stmt.setInt(1, filmID);
			ResultSet filmResult = stmt.executeQuery();
			while (filmResult.next()) {
				int id = filmResult.getInt("id");
				String title = filmResult.getString("title");
				String description = filmResult.getString("description");
				int release_year = filmResult.getInt("release_year");
				String language_id = filmResult.getString("language_id");
				int rental_duration = filmResult.getInt("rental_duration");
				double rental_rate = filmResult.getDouble("rental_rate");
				int length = filmResult.getInt("length");
				String rating = filmResult.getString("rating");
				double replacement_cost = filmResult.getDouble("replacement_cost");
				List <Actor> actors = new ArrayList<>();
				film = new Film(id, title, description, release_year, language_id, rental_duration, rental_rate, length, replacement_cost, rating, actors);
				return film;
				
			}

			filmResult.close();
			stmt.close();
			conn.close();
		} catch (SQLException e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		return film;
	}

	public Actor findActorById(int actorID) {
		Actor actor = null;
		try {
			Connection conn = DriverManager.getConnection(URL, user, pass);
			String sql = "SELECT id, first_name, last_name FROM actor WHERE id = ?,";
			PreparedStatement stmt = conn.prepareStatement(sql);
			stmt.setInt(1, actorID);
			ResultSet actorResult = stmt.executeQuery();
			if (actorResult.next()) {
				String title = actorResult.getString("title");
				String description = actorResult.getString("description");
				int releaseYear = actorResult.getInt("release_year");
				String languageID = actorResult.getString("language_id");
				int rentalDuration = actorResult.getInt("rental_duration");

			}

			actorResult.close();
			stmt.close();
			conn.close();
		} catch (SQLException e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		return actor;
	}

	public List<Actor> findActorsByFilmId(int actorID) {
		List<Actor> actors = new ArrayList<>();
		Actor actor = null;
		try {
			Connection conn = DriverManager.getConnection(URL, user, pass);
			String sql = "SELECT act.id, act.first_name, act.last_name FROM actor actor JOIN film_actor filmact ON act.id = filmact.actor_id JOIN film ON filmact.film_id = film.id WHERE film.id = ?,";
			PreparedStatement stmt = conn.prepareStatement(sql);
			stmt.setInt(1, actorID);
			ResultSet actorResult = stmt.executeQuery();

			if (actorResult.next()) {
				String firstName = actorResult.getString("actor.first_name");
				String lastName = actorResult.getString("actor.last_name");
				int id = actorResult.getInt("actor.id");
				actor = new Actor(id, firstName, lastName);
				actors.add(actor);

			}

			actorResult.close();
			stmt.close();
			conn.close();
		} catch (SQLException e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		return actors;
	}

	@Override
	public Film findFilmByKW(String filmKW) {
		// TODO Auto-generated method stub
		return null;
	}

}
