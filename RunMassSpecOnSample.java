/*
 * Filename:
 *
 * Description: Runs a batch of samples through the Mass Spec machine.
 *
 * (C) Copyright 2009 Biodesix Inc. All Rights Reserved.
 *
 * Revision History
 *
 * Name vcoleman Date 10/1/2009 Description Initial file
 */
package com.biodesix.plugin;

import java.net.ConnectException;
import java.net.SocketException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import com.biodesix.util.Constants;
import com.biodesix.util.LimsParameters;
import com.biodesix.util.MassSpecClient;
import com.biodesix.util.MassSpecResponseObject;
import com.biodesix.util.Plates;
import com.biodesix.util.QcResultObject;
import com.biodesix.util.ReplicateObject;
import com.biodesix.util.SampleResultObject;
import com.velox.api.datarecord.DataRecord;
import com.velox.api.datarecord.NotFound;
import com.velox.api.plugin.PluginResult;
import com.velox.api.util.ServerException;

/*
 * The Class RunMassSpecOnSample. Entry point for plug-in
 */
public class RunMassSpecOnSample extends BdxDefaultServerPlugin {

	private static final String COLLECTION_PARAMETERS_FILE = "C:\\Biodesix\\MassSpecService\\Config\\Veristrat_Serum\\081029_Veristrat.par";

	private static final String NEGC_METHOD = "VeriStrat_NegativeControl";
	private static String POSC_EXPIRE = null;
	private static final String POSC_METHOD = "VeriStrat_Serum";
	private static final String REQUEST_AVAILABLE = "MSAuto.1.Available";

	private static final String REQUEST_BATCH_INFO = "MSAuto.1.BatchInfo";
	private static final String REQUEST_DOCK_PLATE = "MSAuto.1.DockPlate";
	private static final String REQUEST_PROCESS_BATCH = "MSAuto.1.ProcessBatch";
	private static final String SAMPLE_DATA_ASCII_ROOT_DIR = "c:\\zeta";
	private static final String SAMPLE_METHOD = "VeriStrat_Serum";
	private static int REQUEST_TIMEOUT = 3000;
	private static final String XML_VERSION_PREFIX = "<?xml version='1.0'?>";

	private MassSpecClient client = null;
	private String message = "";
	private Sample sam;
	private List<DataRecord> samples = null;
	private static final Vector<String> INUSE_MS_SERVERS = new Vector<String>();
	String poscVeristratResult = "";
	String poscPreAlignPeaks = "";
	String poscAlignTolerance = "";
	String poscAlignPoints = "";
	String poscIntegratedNoise = "";
	String negcPreAlignPeaks = "";
	// Setup display message for user.
	String qcResultsText = "POSC QC Parameters: ";
	String selectedMSServer = null;
	private String msServerName = null;
	private String msServerLocation = null;
	int numberOfTries = 0;
	String sampleDataLocation = "";

	DataRecord sampleObject;
	int totSamples = 0;

	boolean isDisplayed = false;

	// Prepare the results message for the sample results.
	String samplesPassedText = "Data was successfully acquired for the following samples:\n";
	String samplesFailedText = "Samples in batch that failed mass spec:\n";

	public RunMassSpecOnSample() {

		// Indicate that this is a TaskValidator
		setTaskSubmit(true);
		// request timeout in seconds
		REQUEST_TIMEOUT = LimsParameters.getParamValueAsInt(LimsParameters.MASSSPEC_SEND_REQUEST_TIMEOUT, 6 * 60) * 1000;
	}

	@Override
	public PluginResult executePlugin() {
		try {
			log.logInfo("Executing");

			// Get all samples in the current workflow
			samples = getAllByWorkflowId();

			PluginResult ps = checkServerAvailability(client);
			if (ps != null) {
				return ps;
			}

			PluginResult pr = processBatchInfo();
			if (pr != null) {
				return pr;
			}

			PluginResult pResult = openDock();
			if (pResult != null) {
				return pResult;
			}

			return processBatch();
		} catch (Exception e) {
			try {
				log.logError("Unable run MassSpec", e);
				client.closeConnection();
				clientCallback.displayError("Error in:" + this.getClass().getSimpleName() + " \n" + getStackTrace(e));
			} catch (SocketException se) {
				try {
					log.logError("Error in:" + this.getClass().getSimpleName() + " \n" + e.toString());
					clientCallback.displayError("TCP/IP socket failed to close.\n" + e.toString());
				} catch (ServerException srvex) {
				}
			} catch (Exception ex) {
				log.logError("Error in:" + this.getClass().getSimpleName() + " \n" + ex.toString());
			}

			return new PluginResult(false); // On error do not proceed to next task.
		} finally {
			INUSE_MS_SERVERS.remove(selectedMSServer);
		}
	}

	/**
	 * Gets the error code from the passed in String.
	 *
	 * @param string
	 * @param index
	 *            the starting position in the string to start looking.
	 *
	 * @return the error code. -1 if none found.
	 */
	public int getErrorCode(String string, int index) {
		if ((index + 6) > string.length()) {
			return -1;
		}
		string = string.substring(index);
		int start = string.indexOf("1");// Assumes error codes start with a 1.
		int errorCode;
		try {
			// Error codes are 6 digits in length
			errorCode = Integer.parseInt(string.substring(start, start + 6));
		} catch (Exception e) {
			// If what was found is not a 6 digit code, keep looking.
			errorCode = getErrorCode(string, start + 6);
		}
		return errorCode;
	}

	public String getSpot(DataRecord sample, int i) throws RemoteException, NotFound {
		if ((i < 1) || (i > 3)) {
			return null;
		}
		switch (i) {
		case 1:
			return sample.getStringVal(Constants.DF_PLATE_SPOT1, user);
		case 2:
			return sample.getStringVal(Constants.DF_PLATE_SPOT2, user);
		case 3:
			return sample.getStringVal(Constants.DF_PLATE_SPOT3, user);
		}
		return null;
	}

	/**
	 * @param client2
	 * @return
	 * @throws Exception
	 *
	 */
	private PluginResult checkServerAvailability(MassSpecClient client2) throws Exception {
		// Check if mass spec server is ready.
		clientCallback.updateMessage("Checking Mass Spec server availability...");
		clientCallback.updatePercentComplete(5);
		PluginResult result = null;
		do {

			if (!isServerSelected()) {
				result = new PluginResult(false);
				break;
			}
			message = getAvailableMessage();// Creates XML for server request.

			// Request server's availability.
			String response = null;
			try {
				log.logInfo("Checking Mass Spec server availability.");
				response = client.sendRequest(message, REQUEST_TIMEOUT);
				log.logInfo("MS Server responded with: " + response);
			} catch (Exception e) {
				response = "Error: " + e.getMessage();
			}
			// If the mass spec server responds with an error, let user decide to continue.
			if (response.toLowerCase().contains("available")) {
				message = "Mass Spec server is ready...";
				log.logInfo(message);
				clientCallback.updateMessage(message);
				clientCallback.updatePercentComplete(15);
				INUSE_MS_SERVERS.add(selectedMSServer);
				break;// Mass spec server is ready, move on out of Do.
			} else {
				result = processServerAvailability(response);
				if (result != null) {
					break;
				}
			}
		} while (true);
		return result;

	}

	/**
	 * @param qcro
	 * @return
	 */
	private String getQCResultText(QcResultObject qcro) {
		if (qcro.getResultType().toLowerCase().equals("posc")) {
			poscVeristratResult = qcro.getVeristratResult();
			poscPreAlignPeaks = qcro.getPreAlignPeaks();
			poscAlignTolerance = qcro.getAlignTolerance();
			poscAlignPoints = qcro.getAlignPoints();
			poscIntegratedNoise = qcro.getIntegratedNoise();
			qcResultsText += "Result: " + poscVeristratResult + "\n";
			qcResultsText += "Peaks: " + poscPreAlignPeaks + ",   ";
			qcResultsText += "Tolerance: " + poscAlignTolerance + ",   ";
			qcResultsText += "Points: " + poscAlignPoints + ",   ";
			qcResultsText += "Noise: " + poscIntegratedNoise + "\n";
		}
		if (qcro.getResultType().toLowerCase().equals("negc")) {
			negcPreAlignPeaks = qcro.getPreAlignPeaks();
			qcResultsText += "\nNEGC QC Parameters:\n";
			qcResultsText += "Peaks: " + negcPreAlignPeaks + "\n\n";
		}
		return qcResultsText;
	}

	private String getSelectMSServer() throws Exception {
		String[] massSpecList = limsParams.getParams().get("massSpec.machineList").split(",");
		boolean isRunning = false;
		String selectedMSServer = null;
		do {
			List<String> itemList = new ArrayList<String>();
			for (String item : massSpecList) {
				if (INUSE_MS_SERVERS.contains(item)) {
					itemList.add(item.trim() + " (" + Constants.SERVER_USED_MESSAGE + ")");
				} else {
					itemList.add(item.trim());
				}
			}
			// Create the client dialog based on the list from the parameter file.
			List<Object> selectedList = null;

			selectedList = clientCallback.showListDialog("Select the Mass Spectrometer:", itemList, false, user);
			if (((selectedList == null) || selectedList.isEmpty())) {
				boolean isYes = clientCallback.showYesNoDialog("", "Are you sure, you want to cancel the selection, and start the task again?");
				log.logInfo("user selected " + isYes);
				if (isYes) {
					break;
				} else {
					continue;
				}
			}
			selectedMSServer = selectedList.get(0).toString();
			isRunning = selectedMSServer.contains(Constants.SERVER_USED_MESSAGE);
			if (isRunning || INUSE_MS_SERVERS.contains(selectedMSServer)) {
				selectedMSServer = null;
				clientCallback.displayError("This server is already in use. Please select other server.");
			}
		} while (isRunning);
		return selectedMSServer;
	}

	/**
	 * @return
	 * @throws Exception
	 */
	private boolean isServerSelected() throws Exception {
		selectedMSServer = getSelectMSServer();
		if (selectedMSServer == null) {
			return false;
		}
		String[] massSpecServData = selectedMSServer.split(":");
		if (massSpecServData.length != 4) {
			throw new Exception(
					"Invalid value set for massSpec.machineList in configruation file. It should be in the following format.\n<machine_name>:<ipaddress>:<port>:<location>,<machine_name>:<ipaddress>:<port>:<location>");
		}
		clientCallback.displayInfo("Mass Spectrometer " + selectedMSServer + " will be used.");
		log.logInfo("Mass Spectrometer " + selectedMSServer + " will be used.");

		POSC_EXPIRE = params.get("posc.expires");
		log.logInfo("Pos Expired = " + POSC_EXPIRE);
		clientCallback.updateMessage("Confirming Mass Spec server availability...");
		client = new MassSpecClient();
		sam = new Sample(user);
		String ipAddress;
		int port;
		try {
			ipAddress = massSpecServData[1].trim();
			port = Integer.parseInt(massSpecServData[2].trim());
		} catch (Exception e) {
			throw new Exception(
					"Invalid value set for massSpec.machineList in configruation file. It should be in the following format.\n<machine_name>:<ipaddress>:<port>,<machine_name>:<ipaddress>:<port>");
		}
		try {
			client.openConnection(ipAddress, port);
		} catch (ConnectException ce) {
			clientCallback.displayError("Unable to connect to server " + selectedMSServer + ". Please select a different MassSpecServer.");
			return isServerSelected();
		}
		return true;

	}

	/**
	 * @param dockPlateMessage
	 *
	 */
	/**
	 * LIMS specifies a 5 minute timeout for the "input dialog" box. This is suitable for a reasonable timeout for the
	 * dock on the mass spec to be open. Since there is no programmatic way to close the "input dialog", we capture the
	 * exception, close the dock, cancel the connection to the mass spec server, and log the error. When the user closes
	 * the first "input dialog" a second will appear. The second "input dialog" will tell the user of the timeout. When
	 * the user closes the second dialog, the error message will be displayed in the validating dialog.
	 */

	private PluginResult openDock() throws Exception {
		message = "Insert Plate into Dock on MS.";
		// Create XML to request plate be docked.
		String dockPlateMessage = getDockPlateMessage();
		try {
			while (true) {
				String[] keys = { "Dock", "Back" };
				int choice = clientCallback.showOptionDialog("Confirm...", message, keys, 0);
				log.logInfo("user selected option is " + keys[choice]);
				if (choice == 0) { // dock
					break;
				} else if (choice == 1) { // back
					clientCallback.updateMessage("Closing Dock...");
					client.sendRequest(dockPlateMessage, REQUEST_TIMEOUT);
					client.closeConnection();// Close the client to prepare for next pass.
					return new PluginResult(false);// Pause task.
				}
			}
		} catch (Exception e) {
			client.sendRequest(dockPlateMessage, REQUEST_TIMEOUT);
			client.closeConnection();// Close the client to prepare for next pass.
			message = "Dock open too long. Closed dock and connection.";
			log.logError(message);
			try {
				clientCallback.displayMessage("Dock was out too long. You must restart this task.");
			} catch (Exception e1) {
				clientCallback.displayError(message);
				return new PluginResult(false);
			}
		}

		clientCallback.updateMessage("Closing Dock and Calibrating plate...");
		clientCallback.updatePercentComplete(30);
		do {
			String response;
			try {
				// Dock the plate
				response = client.sendRequest(dockPlateMessage, REQUEST_TIMEOUT);
			} catch (Exception e) {
				response = "Error: " + e.getMessage();
			}
			if (!response.toLowerCase().contains("success") || response.toLowerCase().contains("error")) {
				message = "Mass Spec server failed to dock plate.\n" + response;
				log.logError(message);
				String[] keys = { "Try again", "Back" };
				int choice = clientCallback.showOptionDialog("Confirm...", message, keys, 0);
				log.logInfo("user selected " + keys[choice]);
				switch (choice) {
				case 0: // try again
					continue;
				case 1: // back
					return new PluginResult(false);// Pause task.
				}
			} else {
				message = "Plate docked...";
				clientCallback.updateMessage(message);
				clientCallback.updatePercentComplete(35);
				log.logInfo(message);
				break;
			}
		} while (true);
		return null;

	}

	public String getMSLocation(String selectedMSServer) {

		String[] arrSelectedMSServer = selectedMSServer.split(":");
		if (arrSelectedMSServer.length < 4) {
			return "";
		}
		int sIndex = arrSelectedMSServer[3].indexOf("(");
		int eIndex = arrSelectedMSServer[3].indexOf(")");
		sIndex = (sIndex < 0) ? 0 : 1;
		eIndex = (eIndex < 0) ? arrSelectedMSServer[3].length() : eIndex;
		String loc = arrSelectedMSServer[3].substring(sIndex, eIndex).trim();
		return loc;
	}

	/**
	 *
	 */
	private PluginResult processBatch() throws Exception {
		// Start processing sample(s)
		message = "Acquiring data for all samples in the batch";
		clientCallback.updateMessage(message + "...");
		clientCallback.updatePercentComplete(40);
		log.logInfo(message);
		String processBatchMessage = getProcessBatchMessage();
		String response;
		int processTimePerSample = Integer.parseInt(limsParams.getParams().get(LimsParameters.MASSSPEC_PROCESS_TIME_PER_SAMPLE));

		String[] arrSelectedMSServer = selectedMSServer.split(":");
		msServerName = arrSelectedMSServer[0].trim();
		msServerLocation = getMSLocation(selectedMSServer);
		try {
			// Send batch process command
			response = client.sendRequest(processBatchMessage, samples.size() * processTimePerSample * 1000);
		} catch (Exception e) {
			response = "Error: " + e.getMessage();
		} finally {
			try {
				client.closeConnection();
			} catch (Exception ex) {
				// no need to let user know about this error
			}
		}
		log.logInfo("Response from MS server: " + response);

		/**
		 * If an error is thrown during MS processing, this section allows fine grained error handling.
		 */
		if (!response.toLowerCase().contains("success") || response.toLowerCase().contains("error")) {
			for (DataRecord sampleObject : samples) {
				// if datarecord is locked, the following method would raise the alert.
				storeSamplesProcessInfo(sampleObject);
			}
			return processError(response);
		}

		/**
		 * This section examines each sample's results and displays them to the user. Samples that fail are batch reset.
		 * The main assumption is that this section will not even process if there was a POSC or NEGC failure as checked
		 * in the previous section.
		 */
		// Build a Mass Spec Response object from the XML response received from the Mass Spec server.
		MassSpecResponseObject msro = new MassSpecResponseObject(response);
		String msBatchId = "Batch ID = " + msro.getBatchId() + "\n";

		// Get the QC Results objects from the Mass Spec Response Object.
		List<QcResultObject> qcResults = msro.getQcResults();

		// Each QC Results Object represents a sample in the batch.
		for (QcResultObject qcro : qcResults) {
			qcResultsText = getQCResultText(qcro);

		}

		List<SampleResultObject> sampleResults = msro.getSampleResults();

		// Check each sample result for pass's and failure's.
		int samplesPassed = 0;
		int samplesFailed = 0;
		for (SampleResultObject sro : sampleResults) {
			try {
				boolean sampleResult = updateSampleResult(sro);
				if (sampleResult) {
					samplesPassed++;
				} else {
					samplesFailed++;
				}
			} catch (Exception e) {
				String msg = "Unable to save the MassSpec result onto Sample " + sro.getID();
				log.logDebug(e, msg);
				clientCallback.displayError(msg);
			}
		}
		DataRecord plate = getPlate(samples.get(0), user);
		Plates.setUsed(plate, false, user);
		if (totSamples == samplesFailed) {
			clientCallback.displayInfo("Sample failed to acquire. Exiting Workflow, and plate is opened up.");
		}
		message = "Sample Spectra Processed...\n\n" + msBatchId + qcResultsText;
		if (samplesPassed > 0) {
			message += samplesPassedText;
		}
		if (samplesFailed > 0) {
			message += samplesFailedText;
		}
		clientCallback.displayMessage(message);
		log.logInfo(message);
		return new PluginResult(true);
	}

	/**
	 *
	 */
	private void storeSamplesProcessInfo(DataRecord sampleObject) throws Exception {

		Map<String, Object> dataFields = new HashMap<String, Object>();
		dataFields.put(Constants.DF_BDX_MSSERVER, msServerName);
		dataFields.put(Constants.DF_BDX_SAMPLE_PROCESSED_LOCATION, msServerLocation);
		updateInfo(sampleObject, dataFields);
	}

	/**
	 *
	 */
	/**
	 * Build and send batch information. When the batch information is accepted the dock will open. There is a 5 minute
	 * timeout for the dock to be open. If the timeout is exceeded, the dock will close and the user must start the
	 * RunMassSpecOnSample service over.
	 */
	private PluginResult processBatchInfo() throws Exception {
		clientCallback.updateMessage("Sending batch info to Mass Spec server ... Waiting for dock to open.");
		clientCallback.updatePercentComplete(20);
		do {
			String response;
			message = getXmlBatchInfoMessage(samples);// Creates XML for batch information.
			try {
				response = client.sendRequest(message, REQUEST_TIMEOUT);// Wait 3 min for response.
			} catch (Exception e) {
				response = "Error: " + e.getMessage();
			}

			if (!response.toLowerCase().contains("success") || response.toLowerCase().contains("error")) {
				message = "Mass Spec server rejected batch info.\n" + response;
				log.logError(message);
				String[] keys = { "Reset Batch and Quit", "Back" };
				int choice = clientCallback.showOptionDialog("Confirm...", message, keys, 0);
				log.logInfo("user selected " + keys[choice]);

				switch (choice) {
				case 1: // back
					return new PluginResult(false);
				case 0: // reset batch
					long batchId = sam.getBatchId(samples.get(0));
					sam.resetEntireBatch(this, dataRecordManager, samples, activeTask, clientCallback);
					// Release plate for use by other batches.
					DataRecord plate = getPlate(samples.get(0), user);
					Plates.setUsed(plate, false, user);
					message = "Mass spec was not available so the user terminated the workflow. Batch No. " + batchId + " was reset.";
					dataRecordManager.storeAndCommit(message, user);
					return new PluginResult(true);
				}
			} else {
				message = "Batch info accepted...";
				log.logInfo(message + response);
				clientCallback.updateMessage(message);
				clientCallback.updatePercentComplete(25);
				break;
			}
		} while (true);

		return null;
	}

	/**
	 * @param response
	 */
	private PluginResult processError(String response) throws Exception {

		int errorCode = getErrorCode(response, 0);
		boolean stopTask = false;
		if (errorCode == 111001) {
			message = response;
		} else if ((errorCode >= 111002) && (errorCode <= 111005)) {
			message = "NEGC failed processing. Resetting entire batch!\n" + response;
			sam.resetEntireBatch(this, dataRecordManager, samples, activeTask, clientCallback);
			stopTask = true;
		} else if ((errorCode >= 111006) && (errorCode <= 111017)) {
			message = "POSC failed processing. Resetting entire batch!\n" + response;
			sam.resetEntireBatch(this, dataRecordManager, samples, activeTask, clientCallback);
			stopTask = true;
		} else if ((errorCode >= 121001) && (errorCode <= 121006)) {
			message = "Sequence file failed: " + response;
		} else if ((errorCode >= 131001) && (errorCode <= 131004)) {
			message = "Mass Spec communication error: " + response;
		} else if ((errorCode >= 141001) && (errorCode <= 141015)) {
			message = "TCP Client failed: " + response;
		} else if (errorCode == -1) { // on timeout
			message = "Time out...!!! Resetting Entire Batch. Plate is opened up. Exiting workflow.";
			sam.resetEntireBatch(this, dataRecordManager, samples, activeTask, clientCallback);
			stopTask = true;
		} else {
			message = "Unknown Error processing spectra; " + response;
			log.logError("errorCode=" + errorCode);
		}
		log.logError(message);
		activeWorkflow.commitChanges(user);
		dataRecordManager.storeAndCommit("RunMassSpecOnSample", user);
		clientCallback.displayError(message);
		log.logInfo("processError result=" + stopTask);
		return new PluginResult(stopTask);

	}

	/**
	 *
	 */
	private PluginResult processServerAvailability(String response) throws Exception {
		PluginResult pResult = null;
		message = "Mass Spec server was not available.";
		String detailedMessage = message + "\n" + response;
		log.logError(detailedMessage);
		// Show user an error message.
		int selectedButton = clientCallback.showOptionDialog("", detailedMessage, new String[] { "Try again", "Reset Batch" }, 0);
		if (selectedButton == 1) {// User decided to Reset Batch.
			boolean dialogResponse = clientCallback.showYesNoDialog("", "Are you sure to Reset Batch?");
			if (!dialogResponse) {
				pResult = null;
			} else {
				long batchId = sam.getBatchId(samples.get(0));
				sam.resetEntireBatch(this, dataRecordManager, samples, activeTask, clientCallback);
				// Release plate for use by other batches.
				message = "Mass spec server was not available so the user terminated the workflow. Batch No. " + batchId + " was reset";
				dataRecordManager.storeAndCommit(message, user);
				pResult = new PluginResult(true);// Ends the workflow
			}
		}
		return pResult;

	}

	/**
	 * @param sro
	 */
	private boolean updateSampleResult(SampleResultObject sro) throws Exception {

		totSamples++;
		List<ReplicateObject> replicates = sro.getReplicates();
		boolean samplePassed = true;
		for (ReplicateObject ro : replicates) {
			// This logic says "if any replicate fails, the entire sample fails"
			samplePassed &= ro.getCollectionStatus().equals("success");
			sampleDataLocation = ro.getSpectrumFile();
		}

		// Get the actual sample DataRecord to work with.
		sampleObject = getSampleByID(sro.getID());
		String sampleId = sampleObject.getStringVal(Constants.DF_MASTER_SAMPLE_ID, user);
		if (samplePassed) {
			int terminator = sampleDataLocation.lastIndexOf("\\");
			// Strip the filename from the path. Just want the path!
			sampleDataLocation = sampleDataLocation.substring(0, terminator);

			Map<String, Object> dataFields = new HashMap<String, Object>();
			dataFields.put(Constants.DF_BDX_LOCATION, sampleDataLocation);
			dataFields.put(Constants.DF_BDX_POSC_ALIGN_POINTS, poscAlignPoints);
			dataFields.put(Constants.DF_BDX_POSC_ALIGN_TOLERANCE, poscAlignTolerance);
			dataFields.put(Constants.DF_BDX_POSC_INTEGRATED_NOISE, poscIntegratedNoise);
			dataFields.put(Constants.DF_BDX_posc_Pre_Align_Peaks, poscPreAlignPeaks);
			dataFields.put(Constants.DF_BDX_POSC_VERISTRAT_RESULT, poscVeristratResult);
			dataFields.put(Constants.DF_BDX_NEGC_PEAKS, negcPreAlignPeaks);
			dataFields.put(Constants.DF_BDX_MSSERVER, msServerName);
			dataFields.put(Constants.DF_BDX_SAMPLE_PROCESSED_LOCATION, msServerLocation);
			dataFields.put(Constants.DF_BDX_SAMPLE_STATUS, Sample.SS_BATCH_PROCESSED);

			updateInfo(sampleObject, dataFields);
			Sample sample = new Sample(user);
			boolean isStatusSet = sample.getStatus(sampleObject) == Sample.SS_BATCH_PROCESSED;
			samplesPassedText += sampleId + (isStatusSet ? "" : " (Unable to update the Sample. Contact Production Support.)") + "\n";
			return true;
		} else {
			samplesFailedText += sro.getID() + "\n";
			try {
				// store these values first to avoid data lock issue.
				// if there are data lock issue the following method would captures, and sample can be reset without any
				// issues (yeah, still there is a very little chance to get data record locked if user locks the record
				// after calling following method)
				storeSamplesProcessInfo(sampleObject);
				sam.resetSample(this, sampleObject, activeTask, clientCallback);
				dataRecordManager.storeAndCommit("Sample " + sampleId + " has been reset.", user);
			} catch (Exception e) {
				String eMsg = "Unable to reset sample " + sampleId;
				log.logDebug(e, eMsg);
				clientCallback.displayError(eMsg);
			}
			return false;
		}

	}

	private void updateInfo(DataRecord sampleObject, Map<String, Object> dataFields) throws Exception {
		try {
			sampleObject.setFields(dataFields, user);
			dataRecordManager.storeAndCommit("Set POSC and NEGC QC Parameters on sample: " + sampleObject.getStringVal(Constants.DF_MASTER_SAMPLE_ID, user), user);
		} catch (Exception e) {
			String errorMsg = e.getMessage();
			if (errorMsg.contains("Data Record already locked")) {
				String userName = errorMsg.substring(errorMsg.lastIndexOf(':') + 1);
				String sampleId = sampleObject.getStringVal(Constants.DF_MASTER_SAMPLE_ID, user);
				log.logInfo("Showing lock dialog");
				String[] buttons = new String[] { "Retry", "Continue" };
				int iSelectedButton = clientCallback.showOptionDialog("", sampleId + " is locked by user " + userName + ".\nPlease contact the user to unlock the sample."
						+ "\n\nClick Retry when the sample is unlocked." + "\nClick Continue to finish batch processing and contact Production Support to resolve the issue with " + sampleId + ".",
						buttons, 0);
				log.logInfo("Selected button " + buttons[iSelectedButton]);

				log.logInfo(" sampleId = " + sampleId + "; values=" + dataFields);
				if (iSelectedButton == 0) {
					updateInfo(sampleObject, dataFields);
				}

			} else {
				throw e;
			}
		}
	}

	/**
	 * Creates and returns the available tag.
	 *
	 * @return an XML tag.
	 */
	protected String getAvailableMessage() {
		String request = XML_VERSION_PREFIX + "<Request name='" + REQUEST_AVAILABLE + "'/>";
		return request;
	}

	/**
	 * Creates a dock plate message.
	 *
	 * @return the dock plate tag
	 */
	protected String getDockPlateMessage() {
		String request = XML_VERSION_PREFIX + "<Request name='" + REQUEST_DOCK_PLATE + "'/>";
		return request;
	}

	/**
	 * Creates a 'process batch' XML message.
	 *
	 * @return the process batch XML tag set.
	 */
	protected String getProcessBatchMessage() {
		String request = XML_VERSION_PREFIX + "<Request name='" + REQUEST_PROCESS_BATCH + "'/>";
		return request;
	}

	/**
	 * Creates a batch info message.
	 *
	 * @param samples
	 *            to package
	 *
	 * @return an XML batch info tag set.
	 *
	 * @throws Exception
	 *             any exception
	 */
	protected String getXmlBatchInfoMessage(List<DataRecord> samples) throws Exception {

		DataRecord item;

		/*
		 * All samples in this batch have the same POSC and NEGC well locations. So we just pick off the first one in
		 * the samples list.
		 */
		item = samples.get(0);

		// Build the XML header and request element
		String header = XML_VERSION_PREFIX;
		String request = "<Request name='" + REQUEST_BATCH_INFO + "'>";

		// String together info pertaining to all samples in the batch.
		String batchId = "<BatchID>" + "" + item.getLongVal(Constants.DF_BATCH_ID, user) + "</BatchID>";
		String locPosc = "<LocationPositiveControl>" + item.getStringVal(Constants.DF_BDX_POSCONTROL_WELL, user) + "</LocationPositiveControl>";
		String locNegc = "<LocationNegativeControl>" + item.getStringVal(Constants.DF_BDX_NEGCONTROL_WELL, user) + "</LocationNegativeControl>";
		String collParamsFile = "<CollectionParametersFile>" + COLLECTION_PARAMETERS_FILE + "</CollectionParametersFile>";
		String asciiDir = "<SampleDataASCIIRootDir>" + SAMPLE_DATA_ASCII_ROOT_DIR + "</SampleDataASCIIRootDir>";
		String poscMethod = "<PositiveControlCollectionMethod>" + POSC_METHOD + "</PositiveControlCollectionMethod>";
		String negcMethod = "<NegativeControlCollectionMethod>" + NEGC_METHOD + "</NegativeControlCollectionMethod>";
		String expiration = "<ExpirationPositiveControl>" + POSC_EXPIRE + "</ExpirationPositiveControl>";
		String sampleMethod = "<SampleCollectionMethod>" + SAMPLE_METHOD + "</SampleCollectionMethod>";

		// Create Strings for reuse while constructing individual 'Sample' XML elements.
		StringBuilder xmlSamples = new StringBuilder();
		String samplesHeader = "<SampleLocations>";
		/*
		 * Create elements for each sample in the batch and concatenate them into a single "Samples" element
		 * (xmlSamples).
		 */

		for (DataRecord sample : samples) {
			xmlSamples.append("<Sample ID='" + sample.getStringVal(Constants.DF_MASTER_SAMPLE_ID, user) + "'>");
			for (int i = 1; i < 4; i++) {
				xmlSamples.append("<Location>" + getSpot(sample, i) + "</Location>");// add locations from 1 to 3
			}
			xmlSamples.append("</Sample>");
		}

		String endScript = "</SampleLocations></Request>";
		// Close the "Samples" and request elements. Concatenate the entire XML message and return it.
		message = header + request + batchId + locPosc + locNegc + collParamsFile + asciiDir + poscMethod + negcMethod + expiration + sampleMethod + samplesHeader + xmlSamples.toString() + endScript;
		return message;
	}
}
