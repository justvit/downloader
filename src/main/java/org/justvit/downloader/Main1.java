package org.justvit.downloader;

import org.apache.commons.cli.*;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.nio.conn.NHttpClientConnectionManager;
import org.apache.http.nio.reactor.ConnectingIOReactor;
import org.apache.http.nio.reactor.IOReactorException;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A class with the main()
 */
public class Main1 {
    private static final Pattern NUMBER_REGEX = Pattern.compile("\\d+");
    private static final Pattern NUMBER_KM_REGEX = Pattern.compile("(\\d+)([KkMm]?)");
    public static final String CMDLINE_SYNTAX = "java -jar downloader.jar [OPTIONS]";
    public static final Pattern SPACES_REGEX = Pattern.compile("\\s+");
    private static final int KILOS = 2 << 10;
    private static final int MEGAS = 2 << 20;

    private static final Path TEMP_DIR = Paths.get(System.getProperty("java.io.tmpdir"));
    private static final LongAccumulator downloadedBytes = new LongAccumulator((a,b) -> a + b, 0);
    private static final Map<String,Path> linkFiles = new LinkedHashMap<>();

    private static final Function<String,Boolean> NTHREADS_VALIDATOR = (s) -> NUMBER_REGEX.matcher(s).matches();
    private static final Function<String,Integer> NTHREADS_CONVERTER = (s) -> Integer.parseInt(s);

    private static final Function<String,Boolean> SPEEDLIMIT_VALIDATOR = (s) -> NUMBER_KM_REGEX.matcher(s).matches();
    private static final Function<String,Integer> SPEEDLIMIT_CONVERTER =
              (s) -> {
                        Matcher m = NUMBER_KM_REGEX.matcher(s);
                        m.find();
                        int l = Integer.parseInt(m.group(1));
                        String t = m.group(2);
                        int term = StringUtils.isEmpty(t) ? 1 : t.equals("k") ? KILOS : MEGAS;
                        return l * term;
                     };

    public static final int DEFAULT_BUFFER_SIZE = 10 * MEGAS;

    public static void main(String[] args) {
        final Options options = makeOptionsDeclaration();

        final HelpFormatter helpFormatter = new HelpFormatter();

        final CommandLineParser parser = new GnuParser();
        final CommandLine cmd;
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException ex){
            System.err.printf("*** FATAL! %s%n%n", ex.getMessage());
            helpFormatter.printHelp(CMDLINE_SYNTAX, options);
            return;
        }

        /*asList(cmd.getOptions()).stream()
                .map(o -> o.getOpt() + ":" + o.getValuesList())
                .forEach(System.out::println);*/

        if (cmd.hasOption("help")){
            helpFormatter.printHelp(CMDLINE_SYNTAX, options);
            return;
        }

        final String nThreadsString = cmd.getOptionValue("nthreads", "1");
        final int nThreads;
        if (NTHREADS_VALIDATOR.apply(nThreadsString)){
            nThreads = NTHREADS_CONVERTER.apply(nThreadsString);
        } else {
            System.err.printf("*** FATAL! --nthreads arg must be a positive integer, not '%s'%n%n", nThreadsString);
            return;
        }

        final String speedLimitString = cmd.getOptionValue("speedlimit", "0");
        final int speedLimit;
        if (SPEEDLIMIT_VALIDATOR.apply(speedLimitString)){
            speedLimit = SPEEDLIMIT_CONVERTER.apply(speedLimitString);
        } else {
            System.err.printf("*** FATAL! --speedlimit arg must be a positive integer (with optional k or m), not '%s'%n%n", speedLimitString);
            return;
        }

        final String fileString = cmd.getOptionValue("file");
        final Path inputFilePath;
        try {
            inputFilePath = Paths.get(fileString);
        } catch (InvalidPathException e) {
            System.err.printf("*** FATAL! --file arg must be a file path, not '%s'%n%n", fileString);
            return;
        }

        final String outputString = cmd.getOptionValue("output");
        final Path outputDirPath;
        try {
            outputDirPath = Paths.get(fileString);
        } catch (InvalidPathException e) {
            System.err.printf("*** FATAL! --output arg must be a directory path, not '%s'%n%n", outputString);
            return;
        }


        try {
            Files.createDirectories(outputDirPath);
        } catch (IOException ex) {
            System.err.printf("*** FATAL! could not create output dir '%s': %s%n", outputDirPath, ex.getMessage());
            return;
        }

        System.out.printf("DEBUG nThreads = %d, speedLimit = %,d, file = '%s', output = '%s'%n",
                nThreads, speedLimit, inputFilePath, outputDirPath);

        final long start = System.currentTimeMillis();

        final BufferedReader linkReader;
        try {
            linkReader = new BufferedReader(new FileReader(inputFilePath.toString()));
        } catch (FileNotFoundException e) {
            System.err.printf("*** FATAL! could not find input file '%s'!%n", inputFilePath);
            return;
        }

        final Map<String, Set<Path>> links = new HashMap<>();

        // parsing input file
        final int[] lineCount = {1};
        linkReader.lines()
                .forEach(line -> {
                    String[] parts = SPACES_REGEX.split(line);
                    if (parts.length < 2) {
                        System.err.printf("*** WARNING! input file '%s' - line #%d is not of the correct format: [%s]%n",
                                inputFilePath, lineCount[0], line);
                    } else {
                        final String link = parts[0];
                        final String destFileName = parts[1];
                        final Path outputFilePath = outputDirPath.resolve(destFileName);
                        links.computeIfAbsent(link, k -> new LinkedHashSet()).add(outputFilePath);
                    }
                    lineCount[0]++;
                });

        links.forEach((link, files) -> {
            System.out.printf("DEBUG [%s] -> (%s)\n", link, StringUtils.join(files, ", "));
        });

        FixedRateTokenBucket tokenBucket = new FixedRateTokenBucket(speedLimit, speedLimit, 1, TimeUnit.SECONDS);

        //-- creating http async client
        /*
        final CredentialsProvider credsProvider = new BasicCredentialsProvider();
        credsProvider.setCredentials(new AuthScope(PROXY_HOST, PROXY_PORT),
                new UsernamePasswordCredentials(PROXY_USERNAME, PROXY_PASSWORD));
        final HttpHost proxyHost = new HttpHost(PROXY_HOST, PROXY_PORT);
        */

        final RequestConfig requestConfig = RequestConfig.custom()
                .setSocketTimeout(3000)
                .setConnectTimeout(3000)
                //.setProxy(proxyHost)
                .build();

        final IOReactorConfig ioReactorCfg = IOReactorConfig.custom()
                .setIoThreadCount(nThreads)
                .setShutdownGracePeriod(1000)
                .build();

        final ConnectingIOReactor connIoReactor;
        try {
            connIoReactor = new DefaultConnectingIOReactor(ioReactorCfg);
        } catch (IOReactorException ex) {
            System.out.printf("*** FATAL! failed to create http client: %s", ex);
            return;
        }

        final NHttpClientConnectionManager connMgr = new PoolingNHttpClientConnectionManager(connIoReactor);

        final HttpContext context = new BasicHttpContext();

        final HttpAsyncClientBuilder httpClientBuilder = HttpAsyncClients.custom()
                .setDefaultRequestConfig(requestConfig)
                //.setDefaultCredentialsProvider(credsProvider)
                .setConnectionManager(connMgr);

        try (final CloseableHttpAsyncClient httpClient = httpClientBuilder.build()) {
            System.out.println("Started downloading...");
            httpClient.start();
            final CountDownLatch latch = new CountDownLatch(links.size());

            links.forEach((link, files) -> {
                System.out.printf("DEBUG %s -> (%s)\n", link, StringUtils.join(files, ", "));

                final HttpGet request = new HttpGet(link);
                final Path tempFilePath = TEMP_DIR.resolve(files.iterator().next().getFileName()
                                            + "-" + RandomStringUtils.randomAlphanumeric(5));
                System.out.printf("DEBUG starting to download [%s] to file [%s]\n",
                                                     link, tempFilePath.toString());

                //TODO retry downloading on error
                httpClient.execute(request, context, new FutureCallback<HttpResponse>() {
                    @Override
                    public void completed(HttpResponse response) {
                        final byte[] buf = new byte[speedLimit > 0 ? speedLimit : DEFAULT_BUFFER_SIZE];
                        System.out.printf("DEBUG [%s] started '%s' ...\n",
                                Thread.currentThread().getName(), link);

                        try (InputStream content = response.getEntity().getContent()) {
//                            System.out.printf(">>>> response.entity.class = %s%n", response.getEntity().getClass());
                            int nBytesRead;
                            final BufferedOutputStream output
                                    = new BufferedOutputStream(new FileOutputStream(tempFilePath.toString()));
                            do {
                                nBytesRead = content.read(buf);
                                if (nBytesRead > 0) {
                                    downloadedBytes.accumulate(nBytesRead);
                                    output.write(buf, 0, nBytesRead);
                                    System.out.printf("DEBUG [%s] read and write %d bytes%n",
                                            Thread.currentThread().getName(), nBytesRead);
                                    if (speedLimit > 0) {
                                        long slept = tokenBucket.consume(nBytesRead);
                                        System.out.printf("DEBUG [%s] slept for %,d ms%n",
                                              Thread.currentThread().getName(), slept);
                                    }
                                }
                            } while (nBytesRead > 0);
                            output.close();

                            final Path firstDestPath = files.iterator().next();
                            Files.move(tempFilePath, firstDestPath);

                            for (Path destFile : files) {
                                if (!Files.isSameFile(firstDestPath, destFile)) {
                                    Files.copy(firstDestPath, destFile);
                                }
                            }

                        } catch (IOException e) {
                            System.err.printf("ERROR [%s] io error: %s%n",
                                    Thread.currentThread().getName(), e.getMessage());
                        } finally {
                            latch.countDown();
                            System.out.printf("DEBUG [%s] finished '%s' to file '%s'%n",
                                    Thread.currentThread().getName(), link, tempFilePath.toString());
                        }
                    } //-- def completed(HttpResponse)

                    @Override
                    public void failed(Exception ex) {
                        latch.countDown();
                        System.err.printf("ERROR [%s] %s failed: %s%n",
                                Thread.currentThread().getName(), request.getRequestLine(), ex.getMessage());
                    }

                    @Override
                    public void cancelled() {
                        latch.countDown();
                        System.err.printf("ERROR [%s] %s cancelled%n",
                                Thread.currentThread().getName(), request.getRequestLine());
                    }
                });
            });

            latch.await();

        } catch (InterruptedException e) {
            System.err.printf("ERROR interrupted: %s%n", e.getMessage());
            System.exit(-1);
        } catch (IOException e) {
            System.err.printf("ERROR IO error: %s%n", e.getMessage());
            System.exit(-1);
        } finally {
            final long finish = System.currentTimeMillis();
            final long delta = finish - start;
            System.out.printf("TOTAL: app was running for %,d milliseconds, has downloaded %,d bytes%n",
                    delta, downloadedBytes.longValue());
        }
    } //-- def main(String[])

    private static Options makeOptionsDeclaration() {
        final Options options = new Options();

        final Option nThreadsOption = OptionBuilder.withLongOpt("nthreads")
                .hasArg()
                .withType(Integer.class)
                .withDescription("Number of downloading threads")
                .create('n');

        final Option speedLimitOption = OptionBuilder.withLongOpt("speedlimit")
                .hasArg()
                .withType(String.class)
                .withDescription("Upper limit of downloading speed (bytes per second)\nExamples: 1000, 100k, 5m")
                .create('l');

        final Option fileOption = OptionBuilder.withLongOpt("file")
                .hasArg()
                .isRequired()
                .withType(String.class)
                .withDescription("Path to a file with links to download")
                .create('f');

        final Option outputOption = OptionBuilder.withLongOpt("output")
                .hasArg()
                .withType(String.class)
                .withDescription("Path to a folder for downloaded files to save")
                .create('o');

        final Option helpOption = OptionBuilder.withLongOpt("help")
                .withDescription("This text")
                .create('h');

        return options.addOption(nThreadsOption)
               .addOption(speedLimitOption)
               .addOption(fileOption)
               .addOption(outputOption)
               .addOption(helpOption);
    }

}

