package com.skilldistillery.filmquery.app;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.InputMismatchException;
import java.util.List;
import java.util.Scanner;
import com.skilldistillery.filmquery.database.DatabaseAccessor;
import com.skilldistillery.filmquery.database.DatabaseAccessorObject;
import com.skilldistillery.filmquery.entities.Film;

public class FilmQueryApp {

	DatabaseAccessor db = new DatabaseAccessorObject();
	Scanner input = new Scanner(System.in);
	DatabaseAccessorObject accessorObj = new DatabaseAccessorObject();

	public static void main(String[] args) throws SQLException {
		FilmQueryApp app = new FilmQueryApp();
//		app.test();
		app.launch();
	}


	private void launch() {
		startUserInterface();
		input.close();
	}

	// method for prompting user multiple times after intial selection
	private void returnToMainMenu() {
		System.out.println("\n");
		System.out.println("Returning to main menu \n");
		startUserInterface();
	}

	// initial user interface
	private void startUserInterface() {
		int userSelection = 0;
		StringBuilder sb = new StringBuilder();
		sb.append("******************************************\n");
		sb.append("**********Film Query Application**********\n");
		sb.append("****Press 1.To Look up film by an ID******\n");
		sb.append("****Press 2 Look up film by keyword*******\n");
		sb.append("****Press 3 To Quit & Exit the program****\n");
		sb.append("******************************************");
		System.out.println(sb);
		try {
			userSelection = input.nextInt();
		} catch (InputMismatchException e) {
			System.err.println("Please try using an integer value as the film ID & a string as the keyword");
		}

		switch (userSelection) {
		case 1:
			findFilmById();
			returnToMainMenu();
			break;
		case 2:
			findFilmByKW();
			returnToMainMenu();
			break;
		case 3:
			System.out.println("Thank you for using the application Exiting out of the program");
			System.exit(0);
			break;
		default:
			System.out.println("Please select one of the following options (1)-(2)-(3)");
			break;
		}

	}

	// layout for the output
	private void findFilmById() {

		try {
			System.out.println("Please enter the ID of the film you wish to find");
			int userChoice = input.nextInt();
			Film film = db.findFilmById(userChoice);
			System.out.println(" Title of this Film--> " + film.getTitle() + "\n" + " Release Date of this Film--> "
					+ film.getRelease_year() + "\n" + " Rating this Film--> " + film.getRating() + "\n"
					+ " Description of this Film -->" + film.getDescription());
		} catch (Exception e) {
			System.err.println("Please try using an integer value as the film ID");
		}

	}

	private void findFilmByKW() {
		System.out.println("Please enter the KW of the film you wish to find");
		String userChoice = input.next();
		List<Film> filmKW = new ArrayList<>();
		filmKW = db.findFilmByKW(userChoice);
		if (filmKW != null) {
			try {
				for (Film film : filmKW) {
					System.out.println(" Title of this Film--> " + film.getTitle() + " Release Date of this Film--> "
							+ film.getDescription() + "Language of this Film-->" + film.getLanguage_id()
							+ "Rating of this film-->" + film.getRating() + " Release Date of this Film-->  "
							+ film.getRelease_year());
				}
			} catch (NullPointerException e) {
				System.err.println("No such film exist");
			}
		}
	}
}
