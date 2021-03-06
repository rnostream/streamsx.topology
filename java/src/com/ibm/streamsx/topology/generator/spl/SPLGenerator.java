/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2015  
 */
package com.ibm.streamsx.topology.generator.spl;

import static com.ibm.streamsx.topology.builder.JParamTypes.TYPE_COMPOSITE_PARAMETER;
import static com.ibm.streamsx.topology.builder.JParamTypes.TYPE_SUBMISSION_PARAMETER;
import static com.ibm.streamsx.topology.generator.operator.OpProperties.KIND;
import static com.ibm.streamsx.topology.generator.spl.GraphUtilities.getDownstream;
import static com.ibm.streamsx.topology.generator.spl.GraphUtilities.getUpstream;
import static com.ibm.streamsx.topology.generator.spl.GraphUtilities.kind;
import static com.ibm.streamsx.topology.internal.context.remote.DeployKeys.DEPLOYMENT_CONFIG;
import static com.ibm.streamsx.topology.internal.graph.GraphKeys.CFG_HAS_ISOLATE;
import static com.ibm.streamsx.topology.internal.graph.GraphKeys.CFG_HAS_LOW_LATENCY;
import static com.ibm.streamsx.topology.internal.graph.GraphKeys.CFG_STREAMS_COMPILE_VERSION;
import static com.ibm.streamsx.topology.internal.graph.GraphKeys.CFG_STREAMS_VERSION;
import static com.ibm.streamsx.topology.internal.graph.GraphKeys.splAppNamespace;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.array;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jboolean;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jobject;
import static com.ibm.streamsx.topology.internal.gson.GsonUtilities.jstring;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.ibm.streamsx.topology.builder.BVirtualMarker;
import com.ibm.streamsx.topology.builder.JParamTypes;
import com.ibm.streamsx.topology.internal.gson.GsonUtilities;

public class SPLGenerator {
    // Needed for composite name generation
    private int numParallelComposites = 0;

    // The final list of composites (Main composite and parallel regions), which
    // compose the graph.
    List<JsonObject> composites = new ArrayList<>();
    
    private SubmissionTimeValue stvHelper;
    
    private int targetVersion;
    private int targetRelease;
    @SuppressWarnings("unused")
    private int targetMod;

    public String generateSPL(JsonObject graph) throws IOException {
        
        JsonObject graphConfig = getGraphConfig(graph);
        breakoutVersion(graphConfig);
                
        stvHelper = new SubmissionTimeValue(graph);
        new Preprocessor(this, graph).preprocess();
        
        // Generate parallel composites
        JsonObject mainCompsiteDef = new JsonObject();
        mainCompsiteDef.addProperty(KIND, graph.get("name").getAsString());
        mainCompsiteDef.addProperty("public", true);
        mainCompsiteDef.add("parameters", graph.get("parameters"));
        mainCompsiteDef.addProperty("__spl_mainComposite", true);

        Set<JsonObject> starts = GraphUtilities.findStarts(graph);
        separateIntoComposites(starts, mainCompsiteDef, graph);
        StringBuilder sb = new StringBuilder();
        generateGraph(graph, sb);
        
        setDeployment(graph);
        
        return sb.toString();
    }
    
    /**
     * Set any Job Config Overlay deployment options
     * based upon the graph.
     * Currently always sets fusion scheme legacy
     * to ensure that isolation works.
     */
    private void setDeployment(JsonObject graph) {
        
        JsonObject config = jobject(graph, "config");
                      
        // DeploymentConfig
        JsonObject deploymentConfig = new JsonObject();
        config.add(DEPLOYMENT_CONFIG, deploymentConfig);
        
        boolean hasIsolate = jboolean(config, CFG_HAS_ISOLATE);
        boolean hasLowLatency = jboolean(config, CFG_HAS_LOW_LATENCY);
        
        if (hasIsolate)     
            deploymentConfig.addProperty("fusionScheme", "legacy");
        else {
            
            // Default to isolating parallel channels.
            JsonObject parallelRegionConfig = new JsonObject();
            deploymentConfig.add("parallelRegionConfig", parallelRegionConfig);
            
            parallelRegionConfig.addProperty("fusionType", "channelIsolation");
        }
    }
    
    void generateGraph(JsonObject graph, StringBuilder sb) throws IOException {
        JsonObject graphConfig = getGraphConfig(graph);
        graphConfig.addProperty("supportsJobConfigOverlays", versionAtLeast(4,2));

        String namespace = splAppNamespace(graph);
        if (namespace != null && !namespace.isEmpty()) {
            sb.append("namespace ");
            sb.append(namespace);
            sb.append(";\n");
        }

        

        for (int i = 0; i < composites.size(); i++) {
            StringBuilder compBuilder = new StringBuilder();
            generateComposite(graphConfig, composites.get(i), compBuilder);
            sb.append(compBuilder.toString());
        }
    }
    
    private void breakoutVersion(JsonObject graphConfig) {
        String version = jstring(graphConfig, CFG_STREAMS_COMPILE_VERSION);
        if (version == null) {
            version = jstring(graphConfig, CFG_STREAMS_VERSION);
            if (version == null)
                version = "4.0.1";
        }
        String[] vrmf = version.split("\\.");
        targetVersion = Integer.valueOf(vrmf[0]);
        targetRelease = Integer.valueOf(vrmf[1]);
        // allow version to be only V.R (e.g. 4.2)
        if (vrmf.length > 2)
            targetMod = Integer.valueOf(vrmf[2]);
    }
    
    boolean versionAtLeast(int version, int release) {
        if (targetVersion > version)
            return true;
        if (targetVersion == version)
            return targetRelease >= release;
        return false;
    }

    void generateComposite(JsonObject graphConfig, JsonObject graph,
            StringBuilder compBuilder) throws IOException {
        boolean isPublic = jboolean(graph, "public");
        String kind = jstring(graph, KIND);
        kind = getSPLCompatibleName(kind);
        if (isPublic)
            compBuilder.append("public ");

        compBuilder.append("composite ");

        compBuilder.append(kind);
        if (kind.startsWith("__parallel_")) {
            String iput = jstring(graph, "inputName");
            String oput = jstring(graph, "outputName");

            iput = getSPLCompatibleName(iput);
            compBuilder.append("(input " + iput);
            
            if(oput != null && !oput.isEmpty()){
                oput = getSPLCompatibleName(oput);
                compBuilder.append("; output " + oput);
            }
              compBuilder.append(")");
        }
        compBuilder.append("\n{\n");
        
        generateCompParams(graph, compBuilder);

        compBuilder.append("graph\n");
        operators(graphConfig, graph, compBuilder);
        
        generateCompConfig(graph, graphConfig, compBuilder);

        compBuilder.append("}\n");
    }
    
    private void generateCompParams(JsonObject graph, StringBuilder sb) {
        JsonObject jparams = GsonUtilities.jobject(graph, "parameters");
        if (jparams != null && jparams.entrySet().size() > 0) {
            sb.append("param\n");
            for (Entry<String, JsonElement> on : jparams.entrySet()) {
                String name = on.getKey();
                JsonObject param = on.getValue().getAsJsonObject();
                String type = jstring(param, "type");
                
                if (TYPE_COMPOSITE_PARAMETER.equals(type)) {
                    JsonObject value = param.get("value").getAsJsonObject();
                    
                    sb.append("  ");
                    String metaType = jstring(value, "metaType");
                    String splType = Types.metaTypeToSPL(metaType);
                    
                    sb.append(String.format("expression<%s> $%s", splType, name));
                    if (value.has("defaultValue")) {
                        sb.append(" : ");
                        sb.append(value.get("defaultValue").getAsString());
                    }
                        
                    sb.append(";\n");
                }
                else if (TYPE_SUBMISSION_PARAMETER.equals(type))
                    ; // ignore - as it was converted to a TYPE_COMPOSITE_PARAMETER
                else
                    throw new IllegalArgumentException("Unhandled param name=" + name + " jo=" + param);
            }
        }
    }
    
    private void generateCompConfig(JsonObject graph, JsonObject graphConfig, StringBuilder sb) {
        boolean isMainComposite = jboolean(graph, "__spl_mainComposite");
        if (isMainComposite) {
            generateMainCompConfig(graphConfig, sb);
        }
    }
    
    private void generateMainCompConfig(JsonObject graphConfig, StringBuilder sb) {
        JsonArray hostPools = array(graphConfig, "__spl_hostPools");
        boolean hasHostPools =  hostPools != null && hostPools.size() != 0;
        
        JsonObject checkpoint = GsonUtilities.jobject(graphConfig, "checkpoint");
        
        boolean hasCheckpoint = checkpoint != null;
                
        if (hasHostPools || hasCheckpoint)
            sb.append("  config\n");
        
        
        if (hasHostPools) {
            boolean seenOne = false;
            for (JsonElement hpo : hostPools) {
                if (!seenOne) {
                    sb.append("    hostPool:\n");
                    seenOne = true;
                } else {
                    sb.append(",");
                }
                JsonObject hp = hpo.getAsJsonObject();
                String name = jstring(hp, "name");
                JsonArray resourceTags = array(hp, "resourceTags");
                
                sb.append("    ");
                sb.append(name);
                sb.append("=createPool({tags=[");
                for (int i = 0; i < resourceTags.size(); i++) {
                    if (i != 0)
                        sb.append(",");
                    stringLiteral(sb, resourceTags.get(i).getAsString());
                }
                sb.append("]}, Sys.Shared)");
            }
            sb.append(";\n");
        }
        
        if (hasCheckpoint) {
            TimeUnit unit = TimeUnit.valueOf(jstring(checkpoint, "unit"));
            long period = checkpoint.get("period").getAsLong();
            
            // SPL works in seconds, including fractions.
            long periodMs = unit.toMillis(period);
            double periodSec = ((double) periodMs) / 1000.0;
            sb.append("    checkpoint: periodic(");
            sb.append(periodSec);
            sb.append(");\n");
        }
    }

    void operators(JsonObject graphConfig, JsonObject graph, StringBuilder sb)
            throws IOException {
      
        OperatorGenerator opGenerator = new OperatorGenerator(this);
        JsonArray ops = array(graph, "operators");
        for (JsonElement ope : ops) {
            String splOp = opGenerator.generate(graphConfig, ope.getAsJsonObject());
            sb.append(splOp);
            sb.append("\n");
        }
    }
    
    SubmissionTimeValue stvHelper() {
        return stvHelper;
    }

    /**
     * Recursively breaks the graph into different composites, separating the
     * Main composite from the parallel ones. Should work with nested
     * parallelism, but hasn't yet been tested.
     * 
     * @param starts
     *            A list of operators that indicate the start of the region.
     *            These are either source operators, or operators in a composite
     *            that read from the composite's input port.
     * @param comp
     *            A JSON object representing a composite with a name field, but
     *            not an operator field.
     * @param graph
     *            The top-level JSON graph that contains all the operators.
     *            Necessary to pass it to the GraphUtilities.getChildren
     *            function.
     */
    private JsonObject separateIntoComposites(Set<JsonObject> starts,
            JsonObject comp, JsonObject graph) {
        // Contains all ops which have been reached by graph traversal,
        // regardless of whether they are 'special' operators, such as the ones
        // whose kind begins with '$', or whether they're included in the final
        // physical graph.
        Set<JsonObject> allTraversedOps = new HashSet<>();

        // Only contains operators that are in the final physical graph
        // for the composite being generated (comp).
        JsonArray compositeOperators = new JsonArray();

        // Operators which might not have been visited yet.
        List<JsonObject> unvisited = new ArrayList<>();
        JsonObject unparallelOp = null;

        unvisited.addAll(starts);

        // While there are still nodes to visit
        while (!unvisited.isEmpty()) {
            // Get the first unvisited node
            JsonObject visitOp = unvisited.get(0);
            // Check whether we've seen it before. Remember, allTraversedOps
            // contains *every* operator we've traversed in the JSON graph,
            // while visited is a list of only the physical operators that will
            // be included in the graph.
            if (allTraversedOps.contains(visitOp)) {
                unvisited.remove(0);
                continue;
            }
            // We've now traversed this operator.
            allTraversedOps.add(visitOp);

            // If the operator is not a special operator, add it to the
            // visited list.
            if (!isParallelStart(visitOp) && !isParallelEnd(visitOp)) {
                Set<JsonObject> children = GraphUtilities.getDownstream(
                        visitOp, graph);
                unvisited.addAll(children);
                compositeOperators.add(visitOp);
            }

            // If the operator is the start of a parallel region, make a new
            // JSON
            // operator to insert into the main graph, make a new JSON graph to
            // represent the parallel composite, find the parallel region's
            // start
            // operators, and recursively call this function to populate the new
            // composite.
            else if (isParallelStart(visitOp)) {
                JsonObject compOperatorInvocation = createCompositeDefinition(graph, unvisited, visitOp);

                // Add comp operator to the list of physical operators
                compositeOperators.add(compOperatorInvocation);
            }

            // Is end of parallel region
            else {
                unparallelOp = visitOp;
            }

            // remove the operator we've traversed from the list of unvisited
            // operators.
            unvisited.remove(0);
        }

        comp.add("operators", compositeOperators);
        stvHelper.addJsonParamDefs(comp);
        composites.add(comp);

        // If one of the operators in the composite was the $unparallel operator
        // then return that $unparallel operator, otherwise return null.
        return unparallelOp;
    }

    /**
     * Create a composite that contains a sub-section of the graph.
     *  
     * @param graph Complete graph representation.
     * @param unvisited Operator invocations that have not yet been placed into a composite.
     * @param startOp Marker operator
     * @return The operator invocation of the created composite.
     */
    private JsonObject createCompositeDefinition(JsonObject graph, List<JsonObject> unvisited, JsonObject startOp) {
        
        String compositeKind = "__parallel__" + numParallelComposites;
                
        // The new composite definition, represented in JSON
        JsonObject compositeDefinition = new JsonObject();
        compositeDefinition.addProperty(KIND, compositeKind);
        compositeDefinition.addProperty("public", false);

        // The operator to include in the graph that refers to the
        // parallel composite.
        JsonObject compositeInvocation = new JsonObject();

        compositeInvocation.addProperty(KIND, compositeKind);
        String parallelCompositeName = jstring(startOp, "name");
        if (parallelCompositeName == null)
            parallelCompositeName = "parallel_" + numParallelComposites;
        
        compositeInvocation.addProperty("name", parallelCompositeName);
        compositeInvocation.add("inputs", startOp.get("inputs"));
        
        numParallelComposites++;
        
        JsonObject output = startOp.get("outputs").getAsJsonArray().get(0).getAsJsonObject();
        
        boolean partitioned = jboolean(output, "partitioned");
        if (partitioned) {
            JsonArray inputs = startOp.get("inputs").getAsJsonArray();
            assert inputs.size() == 1;
            
            String parallelInputPortName = jstring(inputs.get(0).getAsJsonObject(), "name");

            compositeInvocation.addProperty("partitioned", true);
            compositeInvocation.addProperty("parallelInputPortName",
                    parallelInputPortName);
            compositeInvocation.add("partitionedKeys", output.get("partitionedKeys"));
        }

        // Necessary to later indicate whether the composite the
        // operator
        // refers to is parallelized.
        compositeInvocation.addProperty("parallelOperator", true);

        compositeInvocation.add("width", output.get("width"));

        // Get the start operators in the composite -- the ones
        // immediately downstream from the virtual marker
        // that started the composite.
        Set<JsonObject> compositeStarts = getDownstream(startOp, graph);

        // Once you have the start operators, recursively call the
        // function to populate the composite.
        JsonObject compositeEnd = separateIntoComposites(compositeStarts,
                compositeDefinition, graph);
        stvHelper.addJsonInstanceParams(compositeInvocation, compositeDefinition);

        // Set all relevant input port connections to the input port
        // name of the composite
        String compositeStartOutputPortName = jstring(output, "name");
        String inputName = "__In";
        compositeDefinition.addProperty("inputName", inputName);
        for(JsonObject start : compositeStarts){
            JsonArray inputs = array(start, "inputs");
            for(JsonElement inputObj : inputs){
                JsonObject input = inputObj.getAsJsonObject();
                JsonArray connections = array(input, "connections");
                for(int i = 0; i < connections.size(); i++){
                    if(connections.get(i).getAsString().equals(compositeStartOutputPortName)){
                        connections.set(i, new JsonPrimitive(inputName));
                    }
                }
            }
        }

        if (compositeEnd != null) {
            // Add the children of the end to the unvisited to allow them
            // to be put into the parent composite.
            unvisited.addAll(getDownstream(compositeEnd, graph));
            
            String outputName = "__Out";
            compositeInvocation.add("outputs", compositeEnd.get("outputs"));
            compositeDefinition.addProperty("outputName", outputName);

            // Set all relevant output port names to the output port of
            // the composite.
            JsonObject compositeEndIn = array(compositeEnd, "inputs").get(0).getAsJsonObject();
            String compositeEndInputPortName = jstring(compositeEndIn, "name");
            Set<JsonObject> compositeOutParents = getUpstream(compositeEnd, graph);
            for (JsonObject end : compositeOutParents) {
                if (jstring(end, "kind").equals("com.ibm.streamsx.topology.functional.java::HashAdder")) {
                    
                    String endType = jstring(array(end, "outputs").get(0).getAsJsonObject(), "type");
                    array(compositeInvocation, "outputs").get(0).getAsJsonObject().addProperty("type", endType);
                }
                JsonArray endOutputs = array(end, "outputs");
                for (JsonElement outputObj : endOutputs) {
                    JsonObject endOutput = outputObj.getAsJsonObject();
                    JsonArray connections = array(endOutput, "connections");
                    for (int i = 0; i < connections.size(); i++) {
                        if (connections.get(i).getAsString().equals(compositeEndInputPortName)) {
                            endOutput.addProperty("name", outputName);
                        }
                    }
                }
            }

        }
        return compositeInvocation;
    }




    private boolean isParallelEnd(JsonObject visitOp) {
        return BVirtualMarker.END_PARALLEL.isThis(kind(visitOp));
    }

    private boolean isParallelStart(JsonObject visitOp) {
        return BVirtualMarker.PARALLEL.isThis(kind(visitOp));
    }

    /**
     * Takes a name String that might have characters which are incompatible in
     * an SPL stream name (which just supports ASCII) and returns a valid SPL
     * name.
     * 
     * In addition since an operator name maps to a file name (with .cpp etc. suffixes)
     * we limit the name to a reasonable length. Any name that cannot be represented
     * as ASCII in under 80 characters is mapped to a MD5 representation of
     * the name.
     * 
     * This is a one way mapping, we only need to provide a name that is a
     * unique and consistent mapping of the input.
     * 
     * Use of MD5 means hashing and thus a really small chance of collisions
     * for different names.
     * 
     * Since the true (user) name can be set in a SPL note annotation
     * and displayed by the console, having a "meaningless" name is
     * not so much of an issue.
     * 
     * @param name
     * @return A string which can be a valid SPL stream name. If name is valid
     * as an SPL identifier and less than 80 chars then it is returned (same reference).
     */
    private static final int NAME_LEN = 80;
    public static String getSPLCompatibleName(String name) {

        if (name.length() <= NAME_LEN && name.matches("^[a-zA-Z_][a-zA-Z0-9_]+$"))
            return name;
        
        final byte[] original = name.getBytes(StandardCharsets.UTF_8);
        return "__spl_" + md5Name(original);
    }
    public static String md5Name(byte[] original) {
        try {
            
            MessageDigest md = MessageDigest.getInstance("MD5");
            StringBuilder sb = new StringBuilder(32);
            for (byte b : md.digest(original))
                sb.append(String.format("%02x", b));
                      
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            // Java is required to have MD5
            throw new RuntimeException(e);
        }
    }
    
    /**
     * Add an arbitrary SPL value.
     * JsonObject has a type and a value. 
     */
    static void value(StringBuilder sb, JsonObject tv) {
        
        JsonElement value = tv.get("value");
        
        String type = JParamTypes.TYPE_SPL_EXPRESSION;
        if (tv.has("type")) {
            type = tv.get("type").getAsString();          
        } else {
            if (value.isJsonPrimitive()) {
                JsonPrimitive pv = value.getAsJsonPrimitive();               
                if (pv.isString())
                    type = "RSTRING";
            }
            else if (value.isJsonArray()) {
                type = "RSTRING";
            }
        }
               
        if (value.isJsonArray()) {
            JsonArray array = value.getAsJsonArray();
            
           for (int i = 0; i < array.size(); i++) {
                if (i != 0)
                    sb.append(", ");
                value(sb, type, array.get(i));
            }
        }
        else
        {
            value(sb, type, value);
        }
    }
    
    /**
     * Add a single value of a known type.
     */
    static void value(StringBuilder sb, String type, JsonElement value) {
        switch (type) {
        case "UINT8":
        case "UINT16":
        case "UINT32":
        case "UINT64":
        case "INT8":
        case "INT16":
        case "INT32":
        case "INT64":
        case "FLOAT32":
        case "FLOAT64":
            numberLiteral(sb, value.getAsJsonPrimitive(), type);
            break;
        case "RSTRING":
            stringLiteral(sb, value.getAsString());
            break;
        case "USTRING":
            stringLiteral(sb, value.getAsString());
            sb.append("u");
            break;
            
        case "BOOLEAN":
            sb.append(value.getAsBoolean());
            break;
            
        default:
        case JParamTypes.TYPE_ENUM:
        case JParamTypes.TYPE_SPLTYPE:
        case JParamTypes.TYPE_ATTRIBUTE:
        case JParamTypes.TYPE_SPL_EXPRESSION:
            sb.append(value.getAsString());
            break;
        }
    }

    
    static String stringLiteral(String value) {
        StringBuilder sb = new StringBuilder();
        stringLiteral(sb, value);
        return sb.toString();
    }

    /**
     * Use single quotes for strings to allow clearer
     * representation of JSON objects.
     */
    static void stringLiteral(StringBuilder sb, String value) {
        sb.append("'");

        // Replace any backslash with an escaped version
        // to stop SPL treating the value as an escape leadin
        value = value.replace("\\", "\\\\");

        // Replace new-lines with its SPL escaped version, \n
        // which is \\n as a Java string literal
        value = value.replace("\n", "\\n");

        value = value.replace("'", "\\'");

        sb.append(value);
        sb.append("'");
    }

    /**
     * Append the value with the correct SPL suffix. Integer & Double do not
     * require a suffix
     */
    static void numberLiteral(StringBuilder sb, JsonPrimitive value, String type) {
        String suffix = "";
        
        switch (type) {
        case "INT8": suffix = "b"; break;
        case "INT16": suffix = "h"; break;
        case "INT32": break;
        case "INT64": suffix = "l"; break;
        
        case "UINT8": suffix = "ub"; break;
        case "UINT16": suffix = "uh"; break;
        case "UINT32": suffix = "uw"; break;
        case "UINT64": suffix = "ul"; break;
        
        case "FLOAT32": suffix = "w"; break; // word, meaning 32 bits
        case "FLOAT64": break;
        }

        String literal;

        if (value.isNumber() && isUnsignedInt(type)) {
            Number nv = value.getAsNumber();

            if ("UINT64".equals(type))
                literal = Long.toUnsignedString(nv.longValue());
            else if ("UINT32".equals(type))
                literal = Integer.toUnsignedString(nv.intValue());
            else if ("UINT16".equals(type))
                literal = Integer.toUnsignedString(Short.toUnsignedInt(nv.shortValue()));
            else
                literal = Integer.toUnsignedString(Byte.toUnsignedInt(nv.byteValue()));
        } else {
            literal = value.getAsNumber().toString();
        }
        
        sb.append(literal);
        sb.append(suffix);
    }
    
    private static boolean isUnsignedInt(String type) {
        return "UINT8".equals(type)
                || "UINT16".equals(type)
                || "UINT32".equals(type)
                || "UINT64".equals(type);
    }
    
    /**
     * Get the string value of an "unsigned" Byte, Short, Integer or Long.
     */
    public static String unsignedString(Object integerValue) {
// java8 impl
//        if (integerValue instanceof Long)
//            return Long.toUnsignedString((Long) integerValue);
//        
//        Integer i;
//        if (integerValue instanceof Byte)
//            i = Byte.toUnsignedInt((Byte) integerValue);
//        else if (integerValue instanceof Short)
//            i = Short.toUnsignedInt((Short) integerValue);
//        else if (integerValue instanceof Integer)
//            i = (Integer) integerValue;
//        else
//            throw new IllegalArgumentException("Illegal type for unsigned " + integerValue.getClass());
//        return Integer.toUnsignedString(i);

        if (integerValue instanceof Long) {
            String hex = Long.toHexString((Long)integerValue);
            hex = "00" + hex;  // don't sign extend
            BigInteger bi = new BigInteger(hex, 16);
            return bi.toString();
        }

        long l;
        if (integerValue instanceof Byte)
            l = ((Byte) integerValue) & 0x00ff;
        else if (integerValue instanceof Short)
            l = ((Short) integerValue) & 0x00ffff;
        else if (integerValue instanceof Integer)
            l = ((Integer) integerValue) & 0x00ffffffffL;
        else
            throw new IllegalArgumentException("Illegal type for unsigned " + integerValue.getClass());
        return Long.toString(l);
    }

    static JsonObject getGraphConfig(JsonObject graph) {
        return GsonUtilities.objectCreate(graph, "config");
    }
}
