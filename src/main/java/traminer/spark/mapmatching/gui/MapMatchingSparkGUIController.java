package traminer.spark.mapmatching.gui;

import java.awt.Desktop;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URL;
import java.util.Observable;
import java.util.Observer;
import java.util.ResourceBundle;

import javafx.beans.value.ChangeListener;
import javafx.beans.value.ObservableValue;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.Alert;
import javafx.scene.control.ChoiceBox;
import javafx.scene.control.TextArea;
import javafx.scene.control.TextField;
import javafx.scene.layout.Pane;
import javafx.scene.control.Alert.AlertType;
import javafx.stage.DirectoryChooser;
import javafx.stage.FileChooser;
import traminer.io.log.ObservableLog;
import traminer.io.params.SparkParameters;
import traminer.spark.mapmatching.MapMatchingParameters;
import traminer.util.map.matching.MapMatchingMethod;
import traminer.util.map.matching.nearest.PointToEdgeMatching;
import traminer.util.map.matching.nearest.PointToNodeMatching;

/**
 * GUI controller class (JavaFX). Handle the events and components 
 * of the GUI {@link MapMatchingSparkScene.fxml}. Binds the GUI 
 * components with the Java code.
 * 
 * @author douglasapeixoto
 */
public class MapMatchingSparkGUIController implements Initializable {
	@FXML
	private Pane rootPane;
	@FXML
	private ChoiceBox<String> techniqueChoice;
	@FXML
	private TextArea logTxt;
	
	@FXML
	private TextField sparkMasterTxt;
	@FXML
	private TextField numPartitionsTxt;
	@FXML
	private TextField trajectoryDataTxt;
	@FXML
	private TextField mapDataTxt;
	@FXML
	private TextField sampleDataTxt;
	@FXML
	private TextField outputDataTxt;
	
	@FXML
	private TextField minXTxt;
	@FXML
	private TextField minYTxt;
	@FXML
	private TextField maxXTxt;
	@FXML
	private TextField maxYTxt;
	@FXML
	private TextField boundaryExtTxt;
	@FXML
	private TextField nodesCapacityTxt;

	/** Application log observer */
	private LogObserver logObserver = new LogObserver();
	
	/** Map-matching techniques */
	private static final String POINT_TO_NODE = "POINT-TO-NODE";
	private static final String POINT_TO_EDGE = "POINT-TO-EDGE";
	
	@Override
	public void initialize(URL location, ResourceBundle resources) {
		log("Appplication Starts.");
		
		// feed choice box
		techniqueChoice.getItems().add(POINT_TO_NODE);
		techniqueChoice.getItems().add(POINT_TO_EDGE);
		techniqueChoice.setValue(POINT_TO_NODE);

		handleNumericField(numPartitionsTxt);
		handleNumericField(minXTxt);
		handleNumericField(minYTxt);
		handleNumericField(maxXTxt);
		handleNumericField(maxYTxt);
		handleNumericField(boundaryExtTxt);
		handleNumericField(nodesCapacityTxt);
		
		ObservableLog.instance().addObserver(logObserver);
	}
	
	@FXML
	private void actionOpenTrajectoryData() {
    	final DirectoryChooser dirChooser = new DirectoryChooser();
    	dirChooser.setTitle("Open Trajectory Data");
        final File selectedDir = dirChooser.showDialog(
        		rootPane.getScene().getWindow());
        if (selectedDir != null) {
        	String dataPath = selectedDir.getAbsolutePath();
        	trajectoryDataTxt.setText(dataPath);
        	trajectoryDataTxt.home();
        }		
	}
	
	@FXML
	private void actionOpenMapData() {
    	final FileChooser fileChooser = new FileChooser();
    	FileChooser.ExtensionFilter extFilter = new FileChooser
    			.ExtensionFilter("OSM files (*.osm)", "*.osm");
    	fileChooser.getExtensionFilters().add(extFilter);
    	fileChooser.setTitle("Open OSM Map Data");
    	final File selectedFile = fileChooser.showOpenDialog(
        		rootPane.getScene().getWindow());
        if (selectedFile != null) {
        	String mapPath = selectedFile.getAbsolutePath();
        	mapDataTxt.setText(mapPath);
        	mapDataTxt.home();
        }	
	}
	
	@FXML
	private void actionOpenSampleData() {
    	final DirectoryChooser dirChooser = new DirectoryChooser();
    	dirChooser.setTitle("Open Sample Trajectory Data");
        final File selectedDir = dirChooser.showDialog(
        		rootPane.getScene().getWindow());
        if (selectedDir != null) {
        	String dataPath = selectedDir.getAbsolutePath();
        	sampleDataTxt.setText(dataPath);
        	sampleDataTxt.home();
        }		
	}
	
	@FXML
	private void actionOpenOutputDirectory() {
    	final DirectoryChooser dirChooser = new DirectoryChooser();
    	dirChooser.setTitle("Open Ouput Data Directory");
        final File selectedDir = dirChooser.showDialog(
        		rootPane.getScene().getWindow());
        if (selectedDir != null) {
        	String dataPath = selectedDir.getAbsolutePath();
        	outputDataTxt.setText(dataPath);
        	outputDataTxt.home();
        }		
	}

	@FXML
	private void actionOpenExtractMap() {
		final String url = "https://extract.bbbike.org/";
		if (Desktop.isDesktopSupported()) {
		    try {
				Desktop.getDesktop().browse(new URI(url));
			} catch (Exception e) {
				showErrorMessage("Unable to open URL: " + url);
				e.printStackTrace();
			} 
		}
	}

	@FXML
	private void actionDoMatching() {
		if (!validateFields()) {
			showErrorMessage("All fields must be provided!");
			return;
		}
		
		// Setup space partitioning parameters
		MapMatchingParameters mapMatchingParams = null;
		try {
			double minX = Double.parseDouble(minXTxt.getText());
			double minY = Double.parseDouble(minYTxt.getText());
			double maxX = Double.parseDouble(maxXTxt.getText());
			double maxY = Double.parseDouble(maxYTxt.getText());
			double boundaryExt = Double.parseDouble(boundaryExtTxt.getText());
			int nodesCapacity = Integer.parseInt(nodesCapacityTxt.getText());
			// spatial partitioning configuration
			mapMatchingParams = new MapMatchingParameters(
					minX, maxX, minY, maxY, boundaryExt, nodesCapacity);
		} catch (NumberFormatException e) {
			showErrorMessage("Ivalid number format.");
			e.printStackTrace();
			return;
		}
		
		// Setup map-matching algorithm
		MapMatchingMethod mapMatchingMethod = null;
		if (techniqueChoice.getValue().equals(POINT_TO_NODE)) {
			mapMatchingMethod = new PointToNodeMatching();
		} else 
		if (techniqueChoice.getValue().equals(POINT_TO_EDGE)) {
			mapMatchingMethod = new PointToEdgeMatching();
		} 
		
		// Setup spark parameters
		SparkParameters sparkParams = new SparkParameters(
				sparkMasterTxt.getText(), "MapMatchingApp");

		// do the matching
		String osmPath = mapDataTxt.getText();
		String dataPath = trajectoryDataTxt.getText();
		String sampleDataPath = sampleDataTxt.getText();
		String outputDataPath = outputDataTxt.getText();
		int numRDDPartitions = Integer.parseInt(numPartitionsTxt.getText());
		
		MapMatchingSparkClient mapMatchingClient = new MapMatchingSparkClient(
				mapMatchingMethod, sparkParams, mapMatchingParams);
		
		// run map-matching process in a separated thread
		Runnable process = new Runnable() {
			@Override
			public void run() {
				try {
					mapMatchingClient.doMatching(osmPath, dataPath, 
							sampleDataPath, outputDataPath, numRDDPartitions);
				} catch (IOException e) {
					showErrorMessage("Path does not exist.");
					e.printStackTrace();
					return;
				}
			}
		};
		// start this process in a separated thread
		new Thread(process).start();

		log("Map-Matching Is Running!");
	}

	/**
	 * Force field to be numeric only
	 * 
	 * @param textField
	 */
	private void handleNumericField(TextField textField) {
		textField.textProperty().addListener(new ChangeListener<String>() {
	        @Override
	        public void changed(ObservableValue<? extends String> observable, 
	        		String oldValue, String newValue) {
	            if (!newValue.matches("\\d+\\.?\\d*")) {
	            	textField.setText(newValue.replaceAll("[^\\d|^\\.]", ""));
	            }
	        }
	    });	
	}
	
	/**
	 * Validate mandatory fields.
	 * @return
	 */
	private boolean validateFields() {
		boolean validate = true;
		if (sparkMasterTxt.getText().isEmpty()) {
			log("Spark Master URL must be provided!");
			validate = false;
		}
		if (numPartitionsTxt.getText().isEmpty()) {
			log("Number of RDD partitions must be provided!");
			validate = false;
		}
		if (trajectoryDataTxt.getText().isEmpty()) {
			log("Trajectory data must be provided!");
			validate = false;
		}
		if (mapDataTxt.getText().isEmpty()) {
			log("OSM Map data must be provided!");
			validate = false;
		}
		if (sampleDataTxt.getText().isEmpty()) {
			log("Sample data must be provided!");
			validate = false;
		}
		if (outputDataTxt.getText().isEmpty()) {
			log("Output data directory must be provided!");
			validate = false;
		}
		if (minXTxt.getText().isEmpty()) {
			log("MinX. Coverage must be provided!");
			return false;
		}
		if (minYTxt.getText().isEmpty()) {
			log("MinY. Coverage must be provided!");
			validate = false;
		}
		if (maxXTxt.getText().isEmpty()) {
			log("MaxX. Coverage must be provided!");
			validate = false;
		}
		if (maxYTxt.getText().isEmpty()) {
			log("MaxY. Coverage must be provided!");
			validate = false;
		}
		if (boundaryExtTxt.getText().isEmpty()) {
			log("Boundary Extension must be provided!");
			validate = false;
		}
		if (nodesCapacityTxt.getText().isEmpty()) {
			log("Nodes Capacity must be provided!");
			validate = false;
		}
		return validate;
	}
	
	/**
	 * Open a simple ERROR alert/dialog with the given message.
	 * 
	 * @param message The message content.
	 */
	private void showErrorMessage(String message) {
		Alert alert = new Alert(AlertType.ERROR);
		alert.setTitle("Error Message");
		alert.setHeaderText(null);
		alert.setContentText(message);

		alert.show();
		log(message);
	}

	/**
	 * Add the given message to the log.
	 * @param msg
	 */
	private void log(String msg) {
		logTxt.appendText("> [LOG] " + msg + "\n");
	}
	
	@SuppressWarnings("serial")
	private class LogObserver implements Observer, Serializable {
		//private ObservableLog observable = ObservableLog.instance();
		@Override
		public void update(Observable obs, Object o) {
			//observable = (ObservableLog)obs;
			log(ObservableLog.instance().getMessage());
		}
	}
}
