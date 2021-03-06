/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2016, 2017  
 */
package com.ibm.streamsx.topology.internal.context.remote;

import static com.ibm.streamsx.topology.internal.context.remote.DeployKeys.deploy;
import static com.ibm.streamsx.topology.internal.context.remote.DeployKeys.keepArtifacts;
import static com.ibm.streamsx.topology.internal.graph.GraphKeys.CONFIG;
import static com.ibm.streamsx.topology.internal.graph.GraphKeys.graph;
import static com.ibm.streamsx.topology.internal.graph.GraphKeys.splAppName;
import static com.ibm.streamsx.topology.internal.graph.GraphKeys.splAppNamespace;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.object;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.objectArray;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URISyntaxException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream;

import com.google.gson.JsonObject;
import com.ibm.streamsx.topology.context.ContextProperties;
import com.ibm.streamsx.topology.context.remote.RemoteContext;
import com.ibm.streamsx.topology.internal.gson.GsonUtilities;
import com.ibm.streamsx.topology.internal.process.CompletedFuture;

public class ZippedToolkitRemoteContext extends ToolkitRemoteContext {
    


    private final boolean keepBuildArchive;

	public ZippedToolkitRemoteContext() {
        this.keepBuildArchive = false;
    }
	
    public ZippedToolkitRemoteContext(boolean keepBuildArchive) {
        this.keepBuildArchive = keepBuildArchive;
    }
	
	@Override
    public Type getType() {
        return Type.BUILD_ARCHIVE;
    }
    
    @Override
    public Future<File> _submit(JsonObject submission) throws Exception {
        
        JsonObject deploy = deploy(submission);
        
        // Ensure the code archive is assembled in
        // a clean directory to avoid multiple runs
        // overwriting each other.
        if (!deploy.has(ContextProperties.TOOLKIT_DIR)) {
            
            Path toolkitDir = Files
                    .createTempDirectory(Paths.get(""), "tk");
            
            deploy.addProperty(ContextProperties.TOOLKIT_DIR, toolkitDir.toString());
        }
              
        final File toolkitRoot = super._submit(submission).get();
        return createCodeArchive(toolkitRoot, submission);
    }
    
    public Future<File> createCodeArchive(File toolkitRoot, JsonObject submission) throws IOException, URISyntaxException {
        
        String tkName = toolkitRoot.getName();
        
        Path zipOutPath = pack(toolkitRoot.toPath(), graph(submission), tkName);
        
        if (keepBuildArchive || keepArtifacts(submission)) {
        	final JsonObject submissionResult = GsonUtilities.objectCreate(submission, RemoteContext.SUBMISSION_RESULTS);
        	submissionResult.addProperty(SubmissionResultsKeys.ARCHIVE_PATH, zipOutPath.toString());
        }
        
        JsonObject deployInfo = object(submission, SUBMISSION_DEPLOY);
        deleteToolkit(toolkitRoot, deployInfo);
        
        return new CompletedFuture<File>(zipOutPath.toFile());
    }
        
    private static Path pack(final Path folder, JsonObject graph, String tkName) throws IOException, URISyntaxException {
        String namespace = splAppNamespace(graph);
        String name = splAppName(graph);

        Path zipFilePath = Paths.get(folder.toAbsolutePath().toString() + ".zip");
        
        final Path topologyToolkit = TkInfo.getTopologyToolkitRoot().getAbsoluteFile().toPath(); 
        final String topologyToolkitName = topologyToolkit.getFileName().toString();
        
        // Paths to completely copy into the code archive
        Map<Path, String> paths = new HashMap<>();
        
        // Paths to completely copy into the code archive
        Map<Path,String> toolkits = new HashMap<>();
        toolkits.put(folder, tkName);
        toolkits.put(topologyToolkit, topologyToolkitName);
        
        // Avoid multiple concurrent executions overwriting files.
        Path manifestTmp = Files.createTempFile("manifest_tk", "txt");
        Path mainCompTmp = Files.createTempFile("main_composite", "txt");
        
        // tkManifest is the list of toolkits contained in the archive
        try (PrintWriter tkManifest = new PrintWriter(manifestTmp.toFile(), "UTF-8")) {
            tkManifest.println(tkName);
            tkManifest.println(topologyToolkitName);
            
            JsonObject configSpl = object(graph, CONFIG, "spl");
            if (configSpl != null) {
                objectArray(configSpl, "toolkits",
                        tk -> {
                            File tkRoot = new File(jstring(tk, "root"));
                            String tkRootName = tkRoot.getName();
                            tkManifest.println(tkRootName);
                            toolkits.put(tkRoot.toPath(), tkRootName);
                            }
                        );
            }
        }
        
        // mainComposite is a string of the namespace and the main composite.
        // This is used by the Makefile
        try (PrintWriter mainComposite = new PrintWriter(mainCompTmp.toFile(), "UTF-8")) {
            mainComposite.print(namespace + "::" + name);
        }
               
        Path makefile = topologyToolkit.resolve(Paths.get("opt", "client", "remote", "Makefile.template"));
                      
        paths.put(manifestTmp, "manifest_tk.txt");
        paths.put(mainCompTmp, "main_composite.txt");
        paths.put(makefile, "Makefile");
        
        try {
            addAllToZippedArchive(toolkits, paths, zipFilePath);
        } finally {
            manifestTmp.toFile().delete();
            mainCompTmp.toFile().delete();
        }
        
        return zipFilePath;
    }
    
    
    private static void addAllToZippedArchive(Map<Path, String> toolkits, Map<Path, String> starts, Path zipFilePath) throws IOException {
        try (ZipArchiveOutputStream zos = new ZipArchiveOutputStream(zipFilePath.toFile())) {
            for (Path tk : toolkits.keySet()) {
                final String rootEntryName = toolkits.get(tk);
                Files.walkFileTree(tk, new ToolkitCopy(zos, rootEntryName, tk));
            }
            for (Path start : starts.keySet()) {
                final String rootEntryName = starts.get(start);
                Files.walkFileTree(start, new FullCopy(zos, rootEntryName, start));
            }
        }
    }
    
    /**
     * Copy a complete folder/file into the code archive.
     */
    private static class FullCopy extends SimpleFileVisitor<Path> {
        private final ZipArchiveOutputStream zos;
        private final String rootEntryName;
        final Path start;

        FullCopy(ZipArchiveOutputStream zos, String rootEntryName, Path start) {
            this.zos = zos;
            this.rootEntryName = rootEntryName;
            this.start = start;
        }

        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
            // Skip pyc files.
            if (file.getFileName().toString().endsWith(".pyc"))
                return FileVisitResult.CONTINUE;
            
            String entryName = rootEntryName;
            String relativePath = start.relativize(file).toString();
            // If empty, file is the start file.
            if(!relativePath.isEmpty()){                          
                entryName = entryName + "/" + relativePath;
            }
            // Zip uses forward slashes
            entryName = entryName.replace(File.separatorChar, '/');
            
            ZipArchiveEntry entry = new ZipArchiveEntry(file.toFile(), entryName);
            if (Files.isExecutable(file))
                entry.setUnixMode(0100770);
            else
                entry.setUnixMode(0100660);

            zos.putArchiveEntry(entry);
            Files.copy(file, zos);
            zos.closeArchiveEntry();
            return FileVisitResult.CONTINUE;
        }

        public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
            final String dirName = dir.getFileName().toString();
            // Don't include pyc files or .toolkit 
            if (dirName.equals("__pycache__"))
                return FileVisitResult.SKIP_SUBTREE;
            
            // Zip format does not require directory entries
            return FileVisitResult.CONTINUE;
        }
    }
    
    /**
     * Top-level toolkit directories not to include in a build archive.
     */
    private static final Set<String> TK_EXCLUDES = new HashSet<>(
            Arrays.asList("doc", "output", "samples"));
    
    /**
     * Copy a toolkit into a code archive, skipping any directories
     * not expected to be part of a build archive.
     *
     */
    private static class ToolkitCopy extends FullCopy {

        ToolkitCopy(ZipArchiveOutputStream zos, String rootEntryName, Path start) {
            super(zos, rootEntryName, start);
        }
        
        @Override
        public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
            FileVisitResult r = super.preVisitDirectory(dir, attrs);
            if (r == FileVisitResult.SKIP_SUBTREE)
                return r;
            final String dirName = dir.getFileName().toString();
            if (TK_EXCLUDES.contains(dirName)) {
                if (dir.getParent().equals(start))
                    return FileVisitResult.SKIP_SUBTREE;
            }
            
            // Skip opt/client
            if ("client".equals(dirName)) {
                Path possibleOpt = dir.getParent();
                if ("opt".equals(possibleOpt.getFileName().toString())) {
                    if (possibleOpt.getParent().equals(start))
                        return FileVisitResult.SKIP_SUBTREE;
                }
            }
            return r;
        }
    }

}
