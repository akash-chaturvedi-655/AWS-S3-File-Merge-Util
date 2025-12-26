package com.example.s3merge;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class S3SequentialJsonArrayMerger {

    private static final String CHECKPOINT_FILE = "checkpoint.txt";
    private static final int CHECKPOINT_SAVE_INTERVAL_MS = 10000; // Save checkpoint every 10 seconds
    private static final SimpleDateFormat LOG_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static PrintWriter logWriter;
    
    // Phase 2: Concurrency configuration
    private static final int DOWNLOAD_THREADS = 25;
    private static final int QUEUE_CAPACITY = 500;
    private static ExecutorService downloadExecutor;
    private static ExecutorService uploadExecutor;
    private static BlockingQueue<LineBatch> lineQueue;
    
    // Phase 1: Thread-safe checkpoint lock
    private static final Object checkpointLock = new Object();
    
    // Auto-resume configuration for network failures
    private static final int INITIAL_RETRY_DELAY_MS = 1000; // Start with 1 second
    private static final int MAX_RETRY_DELAY_MS = 300000; // Max 5 minutes between retries
    private static final double BACKOFF_MULTIPLIER = 2.0; // Exponential backoff
    
    // Helper method for exponential backoff with unlimited retries
    private static void waitWithExponentialBackoff(int attemptNumber) {
        int delayMs = (int) Math.min(
            INITIAL_RETRY_DELAY_MS * Math.pow(BACKOFF_MULTIPLIER, attemptNumber - 1),
            MAX_RETRY_DELAY_MS
        );
        
        log("Network issue detected. Waiting " + (delayMs / 1000) + " seconds before retry (attempt " + attemptNumber + ")...");
        
        try {
            Thread.sleep(delayMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log("Retry wait interrupted");
        }
    }
    
    // Check if exception is network-related
    private static boolean isNetworkException(Exception e) {
        String message = e.getMessage();
        if (message == null) return false;
        
        return message.contains("UnknownHostException") ||
               message.contains("SocketTimeoutException") ||
               message.contains("ConnectException") ||
               message.contains("NoRouteToHostException") ||
               message.contains("Connection reset") ||
               message.contains("Connection refused") ||
               message.contains("Network is unreachable") ||
               e instanceof software.amazon.awssdk.core.exception.SdkClientException;
    }

    public static void main(String[] args) {
        try {
            // Initialize log file
            initializeLogger();
            
            log("=== S3 Log Merger Started ===");
            log("Loading configuration...");
            
            Properties props = new Properties();
            
            // Try loading from classpath (src/main/resources)
            InputStream input = S3SequentialJsonArrayMerger.class.getClassLoader()
                    .getResourceAsStream("config.properties");
            
            if (input == null) {
                // Fallback to current directory
                File configFile = new File("config.properties");
                if (!configFile.exists()) {
                    log("ERROR: config.properties not found in classpath or current directory");
                    log("Current directory: " + System.getProperty("user.dir"));
                    log("Please ensure config.properties exists in src/main/resources/");
                    System.exit(1);
                }
                input = new FileInputStream(configFile);
            }
            
            props.load(input);
            input.close();

            String accessKey = props.getProperty("aws.accessKey");
            String secretKey = props.getProperty("aws.secretKey");
            String regionStr = props.getProperty("aws.region");
            String sourceBucket = props.getProperty("source.bucket");
            String targetBucket = props.getProperty("target.bucket");
            int chunkSize = Integer.parseInt(props.getProperty("chunk.size", "25500"));

            log("Configuration loaded:");
            log("  Region: " + regionStr);
            log("  Source Bucket: " + sourceBucket);
            log("  Target Bucket: " + targetBucket);
            log("  Chunk Size: " + chunkSize);
            log("  Download Threads: " + DOWNLOAD_THREADS);

            AwsBasicCredentials awsCreds = AwsBasicCredentials.create(accessKey, secretKey);
            Region region = Region.of(regionStr);

            // Phase 1: Enhanced S3Client with connection pooling
            S3Client s3 = S3Client.builder()
                    .region(region)
                    .credentialsProvider(StaticCredentialsProvider.create(awsCreds))
                    .httpClientBuilder(ApacheHttpClient.builder()
                        .maxConnections(100)
                        .connectionTimeout(Duration.ofSeconds(30))
                        .socketTimeout(Duration.ofSeconds(60))
                        .tcpKeepAlive(true))
                    .build();

            log("S3 Client initialized with connection pooling (max 100 connections)");
            
            // Phase 2: Initialize thread pools
            downloadExecutor = Executors.newFixedThreadPool(DOWNLOAD_THREADS);
            uploadExecutor = Executors.newSingleThreadExecutor();
            lineQueue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);
            log("Thread pools initialized: " + DOWNLOAD_THREADS + " download threads, 1 upload thread");
            
            log("Starting bucket processing...");

            processBucket(s3, sourceBucket, targetBucket, chunkSize);
            
            // Shutdown thread pools gracefully
            downloadExecutor.shutdown();
            uploadExecutor.shutdown();
            downloadExecutor.awaitTermination(5, TimeUnit.MINUTES);
            uploadExecutor.awaitTermination(5, TimeUnit.MINUTES);
            
            s3.close();
            log("=== Processing Completed Successfully ===");
            
        } catch (Exception e) {
            log("FATAL ERROR: " + e.getMessage());
            e.printStackTrace();
            if (logWriter != null) {
                e.printStackTrace(logWriter);
            }
            
            // Ensure thread pools are shut down
            if (downloadExecutor != null) downloadExecutor.shutdownNow();
            if (uploadExecutor != null) uploadExecutor.shutdownNow();
            
            System.exit(1);
        } finally {
            if (logWriter != null) {
                logWriter.close();
            }
        }
    }

    private static void initializeLogger() throws IOException {
        String logFileName = "s3-merger-" + new SimpleDateFormat("yyyyMMdd-HHmmss").format(new Date()) + ".log";
        logWriter = new PrintWriter(new FileWriter(logFileName, true), true);
        log("Log file created: " + logFileName);
    }

    private static void log(String message) {
        String timestamp = LOG_DATE_FORMAT.format(new Date());
        String logMessage = "[" + timestamp + "] " + message;
        System.out.println(logMessage);
        if (logWriter != null) {
            logWriter.println(logMessage);
        }
    }

    private static void processBucket(S3Client s3, String sourceBucket, String targetBucket, int chunkSize) throws IOException, InterruptedException, ExecutionException {
        String continuationToken = null;
        Map<String, String> checkpoint = loadCheckpoint();

        // OPTIMIZATION: Build a HashSet of completed file keys for O(1) lookup
        Set<String> completedFiles = new HashSet<>();
        for (Map.Entry<String, String> entry : checkpoint.entrySet()) {
            String key = entry.getKey();
            // Only add actual file keys (not buffer entries)
            if (!key.startsWith("buffer") && !key.equals("bufferCount") && !key.equals("lastSourceKey")) {
                completedFiles.add(key);
            }
        }
        log("Loaded checkpoint with " + completedFiles.size() + " completed files");

        int bufferCount = checkpoint.containsKey("bufferCount") ? Integer.parseInt(checkpoint.get("bufferCount")) : 0;
        List<String> restoredBuffer = new ArrayList<>();
        String restoredLastSourceKey = checkpoint.get("lastSourceKey");
        
        // Restore buffered lines from checkpoint
        if (bufferCount > 0) {
            log("Restoring " + bufferCount + " buffered lines from checkpoint...");
            for (int i = 0; i < bufferCount; i++) {
                String bufferedLine = checkpoint.get("buffer_" + i);
                if (bufferedLine != null) {
                    restoredBuffer.add(bufferedLine);
                } else {
                    log("WARNING: Missing buffer line at index " + i);
                }
            }
            log("✓ Successfully restored " + restoredBuffer.size() + " lines from previous session");
        }

        long lastCheckpointTime = System.currentTimeMillis();
        int filesProcessed = 0;
        int filesSkipped = 0;
        int totalFilesListed = 0;
        long totalLinesProcessed = 0;
        int chunksUploaded = 0;

        log("Listing objects in source bucket: " + sourceBucket);
        
        // Phase 2: Start async upload worker WITH restored buffer and lastSourceKey
        AtomicInteger uploadedChunks = new AtomicInteger(0);
        Future<?> uploadWorker = uploadExecutor.submit(() -> 
            uploadWorkerTask(s3, targetBucket, lineQueue, chunkSize, uploadedChunks, checkpoint, restoredBuffer, restoredLastSourceKey)
        );

        // Phase 2: Track download futures for concurrency control
        List<Future<FileProcessResult>> downloadFutures = new ArrayList<>();

        do {
            int listAttemptNumber = 0;
            ListObjectsV2Response listRes = null;
            
            // Retry S3 listing with exponential backoff for network resilience
            while (listRes == null) {
                listAttemptNumber++;
                try {
                    ListObjectsV2Request listReq = ListObjectsV2Request.builder()
                            .bucket(sourceBucket)
                            .continuationToken(continuationToken)
                            .build();

                    listRes = s3.listObjectsV2(listReq);
                    
                } catch (Exception e) {
                    if (isNetworkException(e)) {
                        log("⚠ Network error listing bucket objects: " + e.getMessage());
                        
                        // Save checkpoint before long wait
                        synchronized (checkpointLock) {
                            saveCheckpoint(checkpoint);
                        }
                        
                        waitWithExponentialBackoff(listAttemptNumber);
                        log("↻ Retrying bucket listing...");
                    } else {
                        log("ERROR listing bucket: " + e.getMessage());
                        e.printStackTrace();
                        if (logWriter != null) {
                            e.printStackTrace(logWriter);
                        }
                        waitWithExponentialBackoff(listAttemptNumber);
                    }
                }
            }
            
            continuationToken = listRes.nextContinuationToken();

            for (S3Object obj : listRes.contents()) {
                String key = obj.key();
                totalFilesListed++;

                // OPTIMIZATION: Check if file is in completed set - O(1) operation, NO S3 download!
                if (completedFiles.contains(key)) {
                    filesSkipped++;
                    // Log progress every 1000 skipped files
                    if (filesSkipped % 1000 == 0) {
                        log("Fast-skipped " + filesSkipped + " completed files (checkpoint-only check, no downloads)...");
                    }
                    continue; // Skip to next file WITHOUT downloading from S3
                }

                // File is not in checkpoint OR partially processed - process it
                long fileSize = obj.size();
                long lastLineProcessed = checkpoint.containsKey(key) ? Long.parseLong(checkpoint.get(key)) : 0;

                log("Processing file [" + (filesProcessed + 1) + "/" + (totalFilesListed - filesSkipped) + " new]: " + key + 
                    " (Size: " + formatBytes(fileSize) + ")" +
                    (lastLineProcessed > 0 ? " [Resuming from line " + lastLineProcessed + "]" : ""));

                // Phase 2: Submit download task to thread pool
                Future<FileProcessResult> future = downloadExecutor.submit(() -> 
                    downloadAndProcessFile(s3, sourceBucket, key, fileSize, lastLineProcessed, lineQueue, checkpoint, completedFiles)
                );
                downloadFutures.add(future);
                
                // Limit in-flight downloads to prevent memory overflow
                if (downloadFutures.size() >= DOWNLOAD_THREADS * 3) {
                    FileProcessResult result = downloadFutures.get(0).get();
                    downloadFutures.remove(0);
                    
                    if (result != null && result.success) {
                        filesProcessed++;
                        totalLinesProcessed += result.linesRead;
                        
                        // Periodic checkpoint save
                        if (filesProcessed % 100 == 0 || 
                            System.currentTimeMillis() - lastCheckpointTime > CHECKPOINT_SAVE_INTERVAL_MS) {
                            saveCheckpoint(checkpoint);
                            lastCheckpointTime = System.currentTimeMillis();
                            log("Checkpoint saved. Processed: " + filesProcessed + 
                                ", Skipped: " + filesSkipped + ", Chunks uploaded: " + uploadedChunks.get());
                        }
                    }
                }
            }

        } while (continuationToken != null);

        // Wait for all downloads to complete
        log("Waiting for all downloads to complete...");
        for (Future<FileProcessResult> future : downloadFutures) {
            FileProcessResult result = future.get();
            if (result != null && result.success) {
                filesProcessed++;
                totalLinesProcessed += result.linesRead;
            }
        }
        
        // Signal upload worker to finish
        lineQueue.put(new LineBatch(Collections.emptyList(), null)); // Poison pill
        uploadWorker.get(); // Wait for upload worker
        
        saveCheckpoint(checkpoint);
        log("=== Summary ===");
        log("Total files listed: " + totalFilesListed);
        log("Files skipped (already complete): " + filesSkipped);
        log("Files processed: " + filesProcessed);
        log("Total lines processed: " + totalLinesProcessed);
        log("Total chunks uploaded: " + uploadedChunks.get());
        log("Average lines per file: " + (filesProcessed > 0 ? totalLinesProcessed / filesProcessed : 0));
        log("Final checkpoint size: " + formatBytes(new File(CHECKPOINT_FILE).length()));
    }

    // Phase 2: File processing result class
    private static class FileProcessResult {
        boolean success;
        String key;
        long linesRead;
        
        FileProcessResult(boolean success, String key, long linesRead) {
            this.success = success;
            this.key = key;
            this.linesRead = linesRead;
        }
    }

    // Wrapper class to pass lines with source file key
    private static class LineBatch {
        List<String> lines;
        String sourceKey;
        
        LineBatch(List<String> lines, String sourceKey) {
            this.lines = lines;
            this.sourceKey = sourceKey;
        }
    }

    // Phase 2: Download and process file in parallel with unlimited retries
    private static FileProcessResult downloadAndProcessFile(S3Client s3, String sourceBucket, String key, 
                                                            long fileSize, long lastLineProcessed,
                                                            BlockingQueue<LineBatch> queue,
                                                            Map<String, String> checkpoint,
                                                            Set<String> completedFiles) {
        log("Processing file: " + key + " (Size: " + formatBytes(fileSize) + ")" +
            (lastLineProcessed > 0 ? " [Resuming from line " + lastLineProcessed + "]" : ""));

        int attemptNumber = 0;
        
        // Unlimited retries with exponential backoff for network resilience
        while (true) {
            attemptNumber++;
            try (BufferedReader br = new BufferedReader(new InputStreamReader(
                    s3.getObject(GetObjectRequest.builder().bucket(sourceBucket).key(key).build()),
                    StandardCharsets.UTF_8))) {

                long lineNum = 0;
                String line;
                long linesInThisFile = 0;
                List<String> batch = new ArrayList<>();
                
                while ((line = br.readLine()) != null) {
                    lineNum++;
                    if (lineNum <= lastLineProcessed) continue;

                    batch.add(line);
                    linesInThisFile++;
                    
                    // Send batches to upload queue
                    if (batch.size() >= 1000) {
                        queue.put(new LineBatch(new ArrayList<>(batch), key));
                        batch.clear();
                    }
                }
                
                // Send remaining lines
                if (!batch.isEmpty()) {
                    queue.put(new LineBatch(new ArrayList<>(batch), key));
                }
                
                // Mark file as complete (thread-safe)
                synchronized (checkpointLock) {
                    checkpoint.put(key, String.valueOf(lineNum));
                    completedFiles.add(key);
                }
                
                log("  Completed: " + linesInThisFile + " new lines from " + key);
                return new FileProcessResult(true, key, linesInThisFile);
                
            } catch (Exception e) {
                if (isNetworkException(e)) {
                    log("⚠ Network error downloading " + key + ": " + e.getMessage());
                    
                    // Save checkpoint before long wait to prevent data loss on power cuts
                    synchronized (checkpointLock) {
                        saveCheckpoint(checkpoint);
                    }
                    
                    waitWithExponentialBackoff(attemptNumber);
                    log("↻ Retrying download for: " + key);
                    // Continue loop for retry
                } else if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                    log("Download interrupted for: " + key);
                    return new FileProcessResult(false, key, 0);
                } else {
                    log("ERROR processing file: " + key + " - " + e.getMessage());
                    e.printStackTrace();
                    if (logWriter != null) {
                        e.printStackTrace(logWriter);
                    }
                    
                    // Save checkpoint before retry wait
                    synchronized (checkpointLock) {
                        saveCheckpoint(checkpoint);
                    }
                    
                    // For non-network errors, wait and retry
                    waitWithExponentialBackoff(attemptNumber);
                }
            }
        }
    }

    // Phase 2: Async upload worker
    private static void uploadWorkerTask(S3Client s3, String targetBucket, 
                                         BlockingQueue<LineBatch> queue, int chunkSize,
                                         AtomicInteger uploadedChunks,
                                         Map<String, String> checkpoint,
                                         List<String> restoredBuffer,
                                         String restoredLastSourceKey) {
        List<String> buffer = new ArrayList<>(restoredBuffer);
        String lastSourceKey = restoredLastSourceKey; // Track the last source file key
        
        try {
            while (true) {
                LineBatch batch = queue.take();
                
                if (batch.lines.isEmpty()) break; // Poison pill
                
                buffer.addAll(batch.lines);
                if (batch.sourceKey != null) {
                    lastSourceKey = batch.sourceKey; // Update the source key
                }
                
                if (buffer.size() >= chunkSize) {
                    // Extract directory path from source key
                    String targetKey = buildTargetKey(lastSourceKey, "merged-" + System.currentTimeMillis());
                    uploadChunk(s3, new ArrayList<>(buffer.subList(0, chunkSize)), targetBucket, targetKey);
                    uploadedChunks.incrementAndGet();
                    
                    buffer = new ArrayList<>(buffer.subList(chunkSize, buffer.size()));
                    
                    // Save checkpoint after successful upload (thread-safe)
                    synchronized (checkpointLock) {
                        clearBufferFromCheckpoint(checkpoint);
                        updateBufferInCheckpoint(checkpoint, buffer, buffer.size());
                        updateLastSourceKeyInCheckpoint(checkpoint, lastSourceKey); // Save lastSourceKey
                        saveCheckpoint(checkpoint);
                    }
                    
                    log("  Progress: " + uploadedChunks.get() + " chunks uploaded");
                }
            }
            
            // Upload remaining buffer
            if (!buffer.isEmpty()) {
                String targetKey = buildTargetKey(lastSourceKey, "final-merged-" + System.currentTimeMillis());
                uploadChunk(s3, buffer, targetBucket, targetKey);
                uploadedChunks.incrementAndGet();
                
                synchronized (checkpointLock) {
                    clearBufferFromCheckpoint(checkpoint);
                    checkpoint.put("bufferCount", "0");
                    saveCheckpoint(checkpoint);
                }
            }
            
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log("Upload worker interrupted");
        } catch (Exception e) {
            log("ERROR in upload worker: " + e.getMessage());
            e.printStackTrace();
            if (logWriter != null) {
                e.printStackTrace(logWriter);
            }
        }
    }

    // Helper method to build target key with source folder structure
    private static String buildTargetKey(String sourceKey, String mergedFileName) {
        if (sourceKey == null || sourceKey.isEmpty()) {
            return mergedFileName; // Fallback to root if no source key
        }
        
        // Extract directory path from source key
        int lastSlashIndex = sourceKey.lastIndexOf('/');
        if (lastSlashIndex > 0) {
            String sourceDirectory = sourceKey.substring(0, lastSlashIndex + 1);
            return sourceDirectory + mergedFileName;
        }
        
        return mergedFileName; // No directory structure, use root
    }

    // Phase 1: Enhanced upload with multi-part support and unlimited retries
    private static void uploadChunk(S3Client s3, List<String> buffer, String targetBucket, String key) {
        String content = String.join("\n", buffer);
        byte[] data = content.getBytes(StandardCharsets.UTF_8);
        
        int attemptNumber = 0;
        
        // Unlimited retries with exponential backoff for network resilience
        while (true) {
            attemptNumber++;
            try {
                long startTime = System.currentTimeMillis();
                
                // Use multi-part upload for large files (>5MB)
                if (data.length > 5 * 1024 * 1024) {
                    uploadMultipartWithRetry(s3, buffer, targetBucket, key, data);
                } else {
                    uploadSinglePartWithRetry(s3, buffer, targetBucket, key, content);
                }
                
                long uploadTime = System.currentTimeMillis() - startTime;
                log("✓ Uploaded: " + key + 
                    " (" + buffer.size() + " records, " + 
                    formatBytes(data.length) + ", " + 
                    uploadTime + "ms)");
                
                return; // Success, exit retry loop
                
            } catch (Exception e) {
                if (isNetworkException(e)) {
                    log("⚠ Network error uploading " + key + ": " + e.getMessage());
                    waitWithExponentialBackoff(attemptNumber);
                    log("↻ Retrying upload for: " + key);
                    // Continue loop for retry
                } else {
                    log("✗ ERROR uploading: " + key + " - " + e.getMessage());
                    e.printStackTrace();
                    if (logWriter != null) {
                        e.printStackTrace(logWriter);
                    }
                    // For non-network errors, also retry after backoff
                    waitWithExponentialBackoff(attemptNumber);
                }
            }
        }
    }

    // Single-part upload with exception propagation for retry logic
    private static void uploadSinglePartWithRetry(S3Client s3, List<String> buffer, String targetBucket, String key, String content) throws Exception {
        PutObjectRequest putReq = PutObjectRequest.builder()
                .bucket(targetBucket)
                .key(key)
                .contentType("binary/octet-stream")
                .build();

        s3.putObject(putReq, RequestBody.fromString(content, StandardCharsets.UTF_8));
    }

    // Multi-part upload with exception propagation for retry logic
    private static void uploadMultipartWithRetry(S3Client s3, List<String> buffer, String targetBucket, String key, byte[] data) throws Exception {
        CreateMultipartUploadResponse initResponse = s3.createMultipartUpload(
            CreateMultipartUploadRequest.builder()
                .bucket(targetBucket)
                .key(key)
                .contentType("binary/octet-stream")
                .build());
        
        String uploadId = initResponse.uploadId();
        List<CompletedPart> completedParts = new ArrayList<>();
        
        int partSize = 5 * 1024 * 1024; // 5MB parts
        int partNumber = 1;
        
        try {
            for (int i = 0; i < data.length; i += partSize) {
                int end = Math.min(i + partSize, data.length);
                byte[] partData = Arrays.copyOfRange(data, i, end);
                
                UploadPartResponse uploadPartResponse = s3.uploadPart(
                    UploadPartRequest.builder()
                        .bucket(targetBucket)
                        .key(key)
                        .uploadId(uploadId)
                        .partNumber(partNumber)
                        .build(),
                    RequestBody.fromBytes(partData));
                
                completedParts.add(CompletedPart.builder()
                    .partNumber(partNumber)
                    .eTag(uploadPartResponse.eTag())
                    .build());
                
                partNumber++;
            }
            
            s3.completeMultipartUpload(CompleteMultipartUploadRequest.builder()
                .bucket(targetBucket)
                .key(key)
                .uploadId(uploadId)
                .multipartUpload(CompletedMultipartUpload.builder()
                    .parts(completedParts)
                    .build())
                .build());
                
        } catch (Exception e) {
            // Abort multipart upload on failure
            try {
                s3.abortMultipartUpload(AbortMultipartUploadRequest.builder()
                    .bucket(targetBucket)
                    .key(key)
                    .uploadId(uploadId)
                    .build());
            } catch (Exception abortEx) {
                log("Warning: Failed to abort multipart upload: " + abortEx.getMessage());
            }
            throw e;
        }
    }

    private static String formatBytes(long bytes) {
        if (bytes < 1024) return bytes + " B";
        if (bytes < 1024 * 1024) return String.format("%.2f KB", bytes / 1024.0);
        if (bytes < 1024 * 1024 * 1024) return String.format("%.2f MB", bytes / (1024.0 * 1024));
        return String.format("%.2f GB", bytes / (1024.0 * 1024 * 1024));
    }

    private static void updateBufferInCheckpoint(Map<String, String> checkpoint, List<String> buffer, int bufferCount) {
        // Clear old buffer entries first
        clearBufferFromCheckpoint(checkpoint);
        
        // Save current buffer
        checkpoint.put("bufferCount", String.valueOf(bufferCount));
        for (int i = 0; i < buffer.size(); i++) {
            checkpoint.put("buffer_" + i, buffer.get(i));
        }
    }
    
    // Save lastSourceKey to checkpoint for proper restoration
    private static void updateLastSourceKeyInCheckpoint(Map<String, String> checkpoint, String lastSourceKey) {
        if (lastSourceKey != null) {
            checkpoint.put("lastSourceKey", lastSourceKey);
        }
    }

    private static void clearBufferFromCheckpoint(Map<String, String> checkpoint) {
        // Remove all buffer-related keys
        checkpoint.entrySet().removeIf(entry -> 
            entry.getKey().startsWith("buffer_") || entry.getKey().equals("bufferCount")
        );
    }

    // Phase 1: Thread-safe checkpoint loading
    private static Map<String, String> loadCheckpoint() {
        synchronized (checkpointLock) {
            Map<String, String> map = new HashMap<>();
            File f = new File(CHECKPOINT_FILE);
            if (!f.exists()) {
                log("No checkpoint found. Starting fresh.");
                return map;
            }

            try (BufferedReader br = new BufferedReader(new FileReader(f))) {
                String line;
                while ((line = br.readLine()) != null) {
                    String[] parts = line.split("=", 2);
                    if (parts.length == 2) {
                        map.put(parts[0], parts[1]);
                    }
                }
                log("Checkpoint loaded. Tracked files/entries: " + map.size());
            } catch (IOException e) {
                log("ERROR loading checkpoint: " + e.getMessage());
                e.printStackTrace();
            }
            return map;
        }
    }

    // Phase 1: Thread-safe atomic checkpoint saving
    private static void saveCheckpoint(Map<String, String> checkpoint) {
        synchronized (checkpointLock) {
            File tempFile = new File(CHECKPOINT_FILE + ".tmp");
            File finalFile = new File(CHECKPOINT_FILE);
            
            try (BufferedWriter bw = new BufferedWriter(new FileWriter(tempFile))) {
                for (Map.Entry<String, String> entry : checkpoint.entrySet()) {
                    bw.write(entry.getKey() + "=" + entry.getValue());
                    bw.newLine();
                }
                bw.flush();
            } catch (IOException e) {
                log("ERROR saving checkpoint: " + e.getMessage());
                e.printStackTrace();
                return;
            }
            
            // Atomic rename
            if (!tempFile.renameTo(finalFile)) {
                log("ERROR: Failed to atomically update checkpoint");
            }
        }
    }
}
