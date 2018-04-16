package datawave.query.postprocessing.tf;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import datawave.query.Constants;
import datawave.query.jexl.JexlASTHelper;

import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParseException;

class ContentFunctionArguments {
    private List<String> terms;
    private int distance;
    private float minScore;
    private List<String> zone = null;
    
    public List<String> terms() {
        return terms;
    }
    
    public int distance() {
        return distance;
    }
    
    public List<String> zone() {
        return zone;
    }
    
    public ContentFunctionArguments(Function f) throws ParseException {
        String functionName = f.name();
        List<JexlNode> args = f.args();
        
        int currentArg = 0;
        
        // Pass back the necessary elements based on the function used
        if (functionName.equals(Constants.CONTENT_WITHIN_FUNCTION_NAME)) {
            // if the first argument is an int, then we have no zone provided
            try {
                // integer argument is stored at the root level
                distance = Integer.parseInt(JexlASTHelper.dereference(args.get(currentArg)).image);
                
                // Don't want to do the inline ++ in case currentArg still gets
                // incremented on exception
                currentArg++;
            } catch (NumberFormatException e) {
                // If the first arg isn't an int, then it's the zone
                zone = constructZone(args.get(currentArg++));
                
                try {
                    // integer argument is stored at the root level
                    distance = Integer.parseInt(JexlASTHelper.dereference(args.get(currentArg++)).image);
                } catch (NumberFormatException e1) {
                    throw new ParseException("Could not parse an integer distance value");
                }
            }
            
            // Ensure the next term is the termOffsetMap variable
            if (args.get(currentArg++) == null) {
                throw new ParseException("Did not find the term offset map name where expected in the function arguments: " + f.name() + " '"
                                + args.get(currentArg - 1) + "'");
            }
            
            terms = new ArrayList<>(args.size() - currentArg);
            
            // Get the actual terms
            for (int i = currentArg; i < args.size(); i++) {
                String term = JexlASTHelper.dereference(args.get(i)).image.trim();
                
                if (term.length() > 1 && term.charAt(0) == '\'' && term.charAt(term.length() - 1) == '\'') {
                    term = term.substring(1, term.length() - 1);
                }
                
                terms.add(term);
            }
        } else if (functionName.startsWith(Constants.CONTENT_ADJACENT_FUNCTION_NAME)) {
            // Pull off the zone if it's the zone adjacent function
            if (!Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME.equalsIgnoreCase(JexlASTHelper.dereference(args.get(currentArg)).image)) {
                zone = constructZone(args.get(currentArg++));
            }
            
            // Ensure the next term is the termOffsetMap variable
            if (!Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME.equalsIgnoreCase(JexlASTHelper.dereference(args.get(currentArg++)).image.trim())) {
                throw new ParseException("Did not find the term offset map name where expected in the function arguments");
            }
            
            // Get the actual terms
            terms = new ArrayList<>(args.size() - currentArg);
            
            // Get the actual terms
            for (int i = currentArg; i < args.size(); i++) {
                String term = JexlASTHelper.dereference(args.get(i)).image.trim();
                
                if (term.length() > 1 && term.charAt(0) == '\'' && term.charAt(term.length() - 1) == '\'') {
                    term = term.substring(1, term.length() - 1);
                }
                
                terms.add(term);
            }
        } else if (functionName.startsWith(Constants.CONTENT_PHRASE_FUNCTION_NAME)) {
            // Pull off the zone if it's the zone phrase function
            if (!Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME.equalsIgnoreCase(JexlASTHelper.dereference(args.get(currentArg)).image)) {
                zone = constructZone(args.get(currentArg++));
                if (zone.isEmpty()) {
                    currentArg--;
                }
            }
            
            Float minScoreArg = readMinScore(JexlASTHelper.dereference(args.get(currentArg++)).image.trim());
            if (null != minScoreArg) {
                minScore = minScoreArg.floatValue();
            } else { // Rollback the current arg if it is not the min score
                currentArg--;
            }
            
            // Ensure the next term is the termOffsetMap variable
            if (!Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME.equalsIgnoreCase(JexlASTHelper.dereference(args.get(currentArg++)).image.trim())) {
                throw new ParseException("Did not find the term offset map name where expected in the function arguments");
            }
            
            // Get the actual terms
            terms = new ArrayList<>(args.size() - currentArg);
            
            // Get the actual terms
            for (int i = currentArg; i < args.size(); i++) {
                String term = JexlASTHelper.dereference(args.get(i)).image.trim();
                
                if (term.length() > 1 && term.charAt(0) == '\'' && term.charAt(term.length() - 1) == '\'') {
                    term = term.substring(1, term.length() - 1);
                }
                
                terms.add(term.toLowerCase(Locale.ENGLISH));
            }
        } else {
            throw new ParseException("Unrecognized content function: " + f.name());
        }
    }
    
    private List<String> constructZone(JexlNode node) {
        List<String> zones = new ArrayList<>();
        
        for (String zone : JexlASTHelper.getIdentifierNames(node)) {
            if (zone.length() > 1 && zone.charAt(0) == '\'' && zone.charAt(zone.length() - 1) == '\'') {
                zones.add(JexlASTHelper.deconstructIdentifier(zone.substring(1, zone.length() - 1)));
            } else {
                zones.add(JexlASTHelper.deconstructIdentifier(zone));
            }
        }
        return zones;
    }
    
    private Float readMinScore(String arg) {
        try {
            return Float.parseFloat(arg);
        } catch (NumberFormatException nfe) {
            return null;
        }
    }
}
