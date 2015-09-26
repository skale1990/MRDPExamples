package com.som.hadoop.mapreduce2.compress;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.Progressable;

public class StreamProcessorOptionsParser {
  private static final Log LOG = LogFactory.getLog(StreamProcessorOptionsParser.class);
  private Configuration conf;
  private CommandLine commandLine;
  private CompressionCodecFactory codecFactory ;
  private Options options;


  /**
   * Create an options parser with the given options to parse the args.
   * @param opts the options
   * @param args the command line arguments
   * @throws IOException 
   */
  public StreamProcessorOptionsParser(Options opts ) 
      throws IOException {
    this(new Configuration(), opts);
  }

  /**
   * Create an options parser to parse the args.
   * @throws IOException 
   */
  public StreamProcessorOptionsParser() 
      throws IOException {
    this(new Configuration(), new Options());
  }
  
  /** 
   * Create a <code>StreamProcessorOptionsParser<code> to parse only the generic Hadoop  
   * arguments. 
   * 
   * The array of string arguments other than the generic arguments can be 
   * obtained by {@link #getRemainingArgs()}.
   * 
   * @param conf the <code>Configuration</code> to modify.
   * @throws IOException 
   */
  public StreamProcessorOptionsParser(Configuration conf) 
      throws IOException {
    this(conf, new Options()); 
  }

  /** 
   * Create a <code>StreamProcessorOptionsParser</code> to parse given options as well 
   * as generic Hadoop options. 
   * 
   * The resulting <code>CommandLine</code> object can be obtained by 
   * {@link #getCommandLine()}.
   * 
   * @param conf the configuration to modify  
   * @param options options built by the caller 
   * @throws IOException 
   */
  public StreamProcessorOptionsParser(Configuration conf,
      Options options) throws IOException {
    
    this.conf = conf;
    codecFactory = new CompressionCodecFactory(this.conf);
    this.options = options;
    }
  
  /**
   * Specify properties of each generic option
   */
  @SuppressWarnings("static-access")
  private  void buildGeneralOptions() {
    Option codec = OptionBuilder.withArgName("BZip2Codec| bzip| DefaultCodec| default | DeflateCodec | deflate| GzipCodec | gzip | Lz4Codec | lz4 | SnappyCodec |snappy")
    .hasArg()
    .withDescription("specify codec's canonical class name or by codec alias")
    .create("codec");
    
    
    Option hdfsInput = OptionBuilder.withArgName("HDFSFile|HDFSDir")
    .hasArg()
//    .isRequired(true)
    .withDescription("specify a HDFS/Local File or Dirctory")
    .create("hdfsInput");
    
    Option localInput = OptionBuilder.withArgName("LocalFile|LocalDir")
        .hasArg()
//        .isRequired(true)
        .withDescription("specify a Local File or Dirctory")
        .create("localInput");
    
   
    Option outputDir = OptionBuilder.withArgName("HDFSDir")
    .hasArg()
    .withDescription("specify a HDFS Dirctory")
    .create("outputDir");
    
    options.addOption(codec);
    options.addOption(hdfsInput);
    options.addOption(localInput);
    options.addOption(outputDir);
   
    
  }
  
  /**
   * Modify configuration according user-specified generic options
   * @param conf Configuration to be modified
   * @param line User-specified generic options
   */
  private void processGeneralOptions(Configuration conf,
      CommandLine line) throws IOException {
    CompressionCodec codec=null;
    InputStream in = null;
//    FileSystem fs = null;
    boolean local=false;
    CompressionOutputStream out = null;
    try {
      if (line.hasOption("codec")) {
        codec=codecFactory.getCodecByName(line.getOptionValue("codec"));
      }else {
        codec=codecFactory.getCodecByName("DefaultCodec");
      }

      if (line.hasOption("hdfsInput")) {
        String hdfsInput = line.getOptionValue("hdfsInput");
        URI pathURI;
        try {
          pathURI = new URI(hdfsInput);
        } catch (URISyntaxException e) {
          throw new IllegalArgumentException(e);
        }
        Path path = new Path(pathURI);
        FileSystem fs = path.getFileSystem(conf);
        if (!fs.exists(path)) {
          throw new FileNotFoundException("File " + hdfsInput + " does not exist.");
        }
       if (fs.isFile(path)) {
        in = fs.open(path);
      }
        
       }else if (line.hasOption("localInput")) {
         local = true;
         String localInput = line.getOptionValue("localInput");
         Path path = new Path(localInput);
         FileSystem localFs = FileSystem.getLocal(conf);
         
         
           if (!localFs.exists(path)) {
             throw new FileNotFoundException("File " + localInput + " does not exist.");
           }
//        Files.exists(new, options)
         in = Files.newInputStream(Paths.get(localInput));
      } else {
        printGenericCommandUsage(System.out);
      }
      
      
      if (line.hasOption("outputDir")) {
        Path outputDirPath= Path.mergePaths(new Path(line.getOptionValue("outputDir")), new Path(Path.SEPARATOR+"part_"+new SimpleDateFormat("yyyyMMddhhmmss").format(new Date(System.currentTimeMillis()))).suffix(codec.getDefaultExtension()));
        FileSystem fs = outputDirPath.getFileSystem(conf);
//        if (!fs.exists(outputDirPath) && !fs.mkdirs(outputDirPath)) {
//          throw new IOException("Unable to create " + outputDirPath + " directory.");
//        }
//        fs.delete(outputDirPath, true);
       out= codec.createOutputStream(fs.create(outputDirPath, new Progressable() {
          
          @Override
          public void progress() {
            // TODO Auto-generated method stub
            System.out.print("#");
          }
        }));
      }
      
      
      IOUtils.copyBytes(in, out, 4096, false);
    } finally{
      IOUtils.closeStream(out);
      IOUtils.closeStream(in);
    }
    
    
  }
  
  /**
   * Print the usage message for generic command-line options supported.
   * 
   * @param out stream to print the usage message to.
   */
  public static void printGenericCommandUsage(PrintStream out) {
    
    out.println("Options supported are");
    out.println("[-codec <BZip2Codec| bzip| DefaultCodec| default | DeflateCodec | deflate| GzipCodec | gzip ");
    out.println("| Lz4Codec | lz4 | SnappyCodec |snappy>                                       specify codec's canonical class name or by codec alias. If not specified DefaultCodec is used]");
    out.println("-hdfsInput <HDFSFile|HDFSDir>  | -localInput <LocalFile|LocalDir>             specify a HDFS/Local File or Dirctory");
    out.printf("-outputDir <HDFSDir>                                                          specify a HDFS Dirctory%n");
    out.println("The general command line syntax is");
    out.printf("bin/hadoop com.som.hadoop.mapreduce2.compress.StreamCompressorUsingCodecPool [genericOptions] [commandOptions]%n");
  }
  
  /**
   * Parse the user-specified options, get the generic options, and modify
   * configuration accordingly
   * @param opts Options to use for parsing args.
   * @param conf Configuration to be modified
   * @param args User-specified arguments
   */
  public void parseAndProcessOptions( 
      String[] args) throws IOException {
   buildGeneralOptions();
    CommandLineParser parser = new GnuParser();
    try {
      commandLine = parser.parse(options, args);
      processGeneralOptions(conf, commandLine);
    } catch(ParseException e) {
      LOG.warn("options parsing failed: "+e.getMessage());
      e.printStackTrace();

      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("general options are: ", options);
    }
  }
  
}
