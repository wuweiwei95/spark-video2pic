/**
 * Created by wuweiwei on 05/05/2017.
 */
import java.awt.image.BufferedImage;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.spark.SparkConf;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.bytedeco.javacpp.opencv_core;
import org.bytedeco.javacpp.opencv_core.Mat;
import org.bytedeco.javacpp.opencv_core.RectVector;
import org.bytedeco.javacpp.opencv_core.Scalar;
import org.bytedeco.javacpp.opencv_imgcodecs.*;
import org.bytedeco.javacpp.opencv_imgproc.*;
import org.bytedeco.javacpp.opencv_objdetect.CascadeClassifier;
import org.bytedeco.javacv.*;

import javax.imageio.ImageIO;

import static java.lang.Thread.sleep;

public class sparkVideo2Pic {

    public static void copyFile(String from, String to) throws IOException {
        Configuration conf = new Configuration();
        Path fromPath = new Path(from);
        Path toPath = new Path(to);

        FSDataInputStream is = fromPath.getFileSystem(conf).open(fromPath);
        FSDataOutputStream os = toPath.getFileSystem(conf).create(toPath);

        IOUtils.copyBytes(is, os, conf);

        is.close();
        os.close();
    }

    public static void video2Pic(String inputFilePath, String outputPath) throws IOException {
        System.out.println("filename is " + inputFilePath);
        System.out.println("output file path is " + outputPath);

        String inputFileName = inputFilePath.substring(inputFilePath.lastIndexOf("/") + 1);
        String extension = inputFileName.substring(inputFileName.lastIndexOf("."));
        String inputName = inputFileName.substring(0, inputFileName.lastIndexOf("."));
        String tmpPicFile;

        File localIn = File.createTempFile(inputName + "-", extension, new File("/tmp/"));

        // copy video file from HDFS to worker tmp path
        copyFile(inputFilePath, "file://" + localIn.getPath());

        FFmpegFrameGrabber grabber = new FFmpegFrameGrabber(localIn.getPath());

        try {
            grabber.start();
        } catch (Exception e) {
            e.printStackTrace();
        }

        long frameLength = grabber.getLengthInFrames();

        // To grab frames per 0.5 seconds
        for (int i = 1; i < frameLength; i += grabber.getFrameRate() / 2) {
            Java2DFrameConverter converter = new Java2DFrameConverter();
            grabber.setFrameNumber(i);
            tmpPicFile = "/tmp/" + inputName + "-" + "frame" + i + ".jpg";
            BufferedImage bi = converter.getBufferedImage(grabber.grab());
            // TODO write to HDFS
            ImageIO.write(bi, "jpg", new File(tmpPicFile));

            copyFile("file://" + tmpPicFile, outputPath + inputName + "-" + "frame" + i + ".jpg");
        }

        // To know number for each video
        System.out.println("Video " + inputFilePath + " have " + frameLength + " frames.");

        try {
            grabber.stop();
        } catch (FrameGrabber.Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws InterruptedException {

        if (args.length < 2) {
            System.err.println("Usage: sparkVideo2Pic <input-videos-path> <output-pics-path>\n\n");
            System.exit(1);
        }
        String inputPath = args[0];
        String outputPath = args[1];

        SparkConf conf = new SparkConf().setAppName("Video-split");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        List<String> fileList = new ArrayList<String>();
        //String filenameRDDPath = "hdfs://master:9000/filenamelist";

        String filenameRDDPath = outputPath + "/filenamelist";
        try {
            FileSystem fs = FileSystem.get(new Configuration());
            FileStatus[] status = fs.listStatus(new Path(inputPath));
            for (FileStatus fileStat : status) {
                if (fileStat.isDirectory()) {

                } else {
                    fileList.add(fileStat.getPath().toString());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        // convert input path into RDD
        //JavaRDD<String> videoNamesRDD = jsc.textFile("hdfs://master:9000/videos/");
        JavaRDD<String> videoNamesRDD = jsc.parallelize(fileList,3);
        videoNamesRDD.saveAsTextFile(filenameRDDPath);
        JavaRDD<String> filenameRDD = jsc.textFile(filenameRDDPath);

        filenameRDD.cache();
        long startTime = System.currentTimeMillis();
        //System.out.println("####### Process files number is  " + videoNamesRDD.count());
        //System.out.println("####### partition is " + videoNamesRDD.getNumPartitions());
        // distribute video process
        filenameRDD.foreach(new VoidFunction<String>() {
            public void call (String videoFile) throws IOException {
                video2Pic(videoFile, outputPath);
            }
        });
        // get the process time
        long endTime   = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        System.out.println("####### Video decode take time " + totalTime/1000 + "s");

        // delete the filename list file
        try {
            FileSystem fs = FileSystem.get(new Configuration());
            fs.delete(new Path(filenameRDDPath),true);
        } catch (IOException e) {
            e.printStackTrace();
        }

        while(true){
        }
        // done, stop spark
        //jsc.stop();

    }

}
