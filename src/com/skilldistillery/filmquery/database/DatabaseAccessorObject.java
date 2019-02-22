package com.skilldistillery.filmquery.database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

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
				film = new Film(id, title, description, rating, release_year, language_id, rental_duration, rental_rate, length, replacement_cost, rating, actors);
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
			String sql = "SELECT id, first_name, last_name FROM actor WHERE id = ?";
			PreparedStatement stmt = conn.prepareStatement(sql);
			stmt.setInt(1, actorID);
			ResultSet actorResult = stmt.executeQuery();
			if (!actorResult.next()) {
				System.out.println("error try again");
			}
			while (actorResult.next()) {
				int id = actorResult.getInt("id");
				String firstName = actorResult.getString("first_name");
				String lastName = actorResult.getString("last_name");
				String title = actorResult.getString("title");
				String description = actorResult.getString("description");
				int releaseYear = actorResult.getInt("release_year");
				String languageID = actorResult.getString("language_id");
				int rentalDuration = actorResult.getInt("rental_duration");
				actor = new Actor(id, firstName, lastName);
			}

			actorResult.close();
			stmt.close();
			conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return actor;
	}

	public List<Actor> findActorsByFilmId(int actorID) {
		List<Actor> actors = new ArrayList<>();
		Actor actor = null;
		try {
			Connection conn = DriverManager.getConnection(URL, user, pass);
			String sql = "SELECT act.id, act.first_name, act.last_name FROM actor act JOIN film_actor filmact ON act.id = filmact.actor_id WHERE filmact.film_id  = ?";
			PreparedStatement stmt = conn.prepareStatement(sql);
			stmt.setInt(1, actorID);
			ResultSet actorResult = stmt.executeQuery();
			if (!actorResult.next()) {
				System.out.println("error try again");
			}
			while (actorResult.next()) {
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
			e.printStackTrace();
		}
		return actors;
	}

	public String findFilmLanguage(int filmId) {
		String languageId = null;
		try {
			Connection conn = DriverManager.getConnection(URL, user, pass);
			String sql = "SELECT name From language where id = ?";
			PreparedStatement stmt = conn.prepareStatement(sql);
			stmt.setInt(1, filmId);
			ResultSet filmIDResult = stmt.executeQuery();
			if (filmIDResult.next()) {
				languageId = filmIDResult.getString(1);
			}
			conn.close();
			filmIDResult.close();
			stmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return languageId;
	}

	@Override
	public List<Film> findFilmByKW(String filmKW) {
		List<Film> films = new ArrayList<>();
		Film film = null;
		try {
			Connection conn = DriverManager.getConnection(URL, user, pass);
			String sql = "SELECT id, title, description, rating, release_year, rental_rate, language_id,replacement_cost, length,rental_duration FROM film WHERE title LIKE ? OR description like ?";
			PreparedStatement stmt = conn.prepareStatement(sql);
			stmt.setString(1, "%" + filmKW + "%");
			stmt.setString(2, "%" + filmKW + "%");
			ResultSet filmResult = stmt.executeQuery();

			while (filmResult.next()) {
				int id = filmResult.getInt("id");
				String title = filmResult.getString("title");
				String description = filmResult.getString("description");
				int releaseYear = filmResult.getInt("release_year");
				String languageID = filmResult.getString("language_id");
				int rentalDuration = filmResult.getInt("rental_duration");
				film = new Film(id, title, description, languageID, releaseYear, languageID, rentalDuration,rentalDuration, rentalDuration, rentalDuration, languageID, null);
				films.add(findFilmById(filmResult.getInt("id")));
			}
			conn.close();
			filmResult.close();
			stmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return films;
	}
}
