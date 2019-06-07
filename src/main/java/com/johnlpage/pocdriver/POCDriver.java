package com.johnlpage.pocdriver;


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import org.apache.commons.cli.ParseException;
import org.bson.BsonBinaryWriter;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;

import java.util.logging.LogManager;

public class POCDriver {

    public static void main(String[] args) {

        POCTestOptions testOpts;
        LogManager.getLogManager().reset();
        System.out.println("MongoDB Proof Of Concept - Load Generator version 0.1.2");
        try {
            testOpts = new POCTestOptions(args);
            // Quit after displaying help message
            if (testOpts.helpOnly) {
                return;
            }

            if (testOpts.arrayupdates > 0 && (testOpts.arraytop < 1 || testOpts.arraynext < 1)) {
                System.out.println("You must specify an array size to update arrays");
                return;
            }
            if (testOpts.printOnly) {
                printTestDocument(testOpts);
                return;
            }

        } catch (ParseException e) {
            System.err.println(e.getMessage());
            return;
        }

        POCTestResults testResults = new POCTestResults(testOpts);
        LoadRunner runner = new LoadRunner(testOpts);
        runner.RunLoad(testOpts, testResults);
    }

    private static void printTestDocument(final POCTestOptions testOpts) {
        //Sets up sample data don't remove
        TestRecord tr;
        int[] arr = new int[2];
        arr[0] = testOpts.arraytop;
        arr[1] = testOpts.arraynext;
        tr = new TestRecord(testOpts.numFields, testOpts.depth, testOpts.textFieldLen,
                1, 12345678, testOpts.NUMBER_SIZE, arr, testOpts.blobSize);
        //System.out.println(tr);
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        JsonParser jp = new JsonParser();
        JsonElement je = jp.parse(tr.internalDoc.toJson());

        String json = gson.toJson(je);
        StringBuilder newJson = new StringBuilder();
        int arrays = 0;

        // Collapse inner newlines
        boolean inquotes = false;
        for (int c = 0; c < json.length(); c++) {
            char inChar = json.charAt(c);
            if (inChar == '[') {
                arrays++;
            }
            if (inChar == ']') {
                arrays--;
            }
            if (inChar == '"') {
                inquotes = !inquotes;
            }

            if (arrays > 1 && inChar == '\n') {
                continue;
            }
            if (arrays > 1 && !inquotes && inChar == ' ') {
                continue;
            }
            newJson.append(json.charAt(c));
        }

        System.out.println(newJson.toString());

        //Thanks to Ross Lawley for this bit of black magic
        BasicOutputBuffer buffer = new BasicOutputBuffer();
        BsonBinaryWriter binaryWriter = new BsonBinaryWriter(buffer);
        new DocumentCodec().encode(binaryWriter, tr.internalDoc, EncoderContext.builder().build());
        int length = binaryWriter.getBsonOutput().getSize();

        System.out.println(String.format("Documents are %.2f KB each as BSON", (float) length / 1024));
    }

}
