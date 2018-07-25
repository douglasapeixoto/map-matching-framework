package traminer.spark.mapmatching.gui;

import java.io.IOException;

import javafx.application.Application;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.stage.Stage;
import traminer.io.log.LogWriter;

/**
 * Starts the GUI and binds the GUI Window with 
 * the FXML scene (JavaFX). GUI start point class.
 * 
 * @author douglasapeixoto
 */
public class MapMatchingSparkGUI extends Application {

	@Override
	public void start(Stage stage) {		
		try {
			Parent root = FXMLLoader.load(getClass().getResource("MapMatchingSparkScene.fxml"));
			
			Scene mainScence  = new Scene(root);

	        stage.setTitle("Map-Matching Spark App");
	        stage.setScene(mainScence);
	        stage.setHeight(700.0);
	        stage.setWidth(725.0);
	        stage.setResizable(false);
	        stage.show();
		} catch (IOException e) {
			System.err.println("Error starting GUI.");
			e.printStackTrace();
		}
	}

	/**
	 * Launch GUI.
	 * @param args
	 */
	public static void main(String[] args) {
		// Set up log4j configuration
		LogWriter.startLog();
		// launch app GUI
		launch(args);
	}
}
