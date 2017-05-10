package datawave.query.rewrite.postprocessing.tf;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import datawave.query.rewrite.Constants;
import org.apache.commons.jexl2.parser.ParseException;

class ContentFunctionArguments {
    private List<String> terms;
    private int distance;
    private String zone = null;
    
    public List<String> terms() {
        return terms;
    }
    
    public int distance() {
        return distance;
    }
    
    public String zone() {
        return zone;
    }
    
    public ContentFunctionArguments(Function f) throws ParseException {
        String functionName = f.name();
        List<String> args = f.args();
        
        int currentArg = 0;
        
        // Pass back the necessary elements based on the function used
        if (functionName.equals(Constants.CONTENT_WITHIN_FUNCTION_NAME)) {
            // if the first argument is an int, then we have no zone provided
            try {
                distance = Integer.parseInt(args.get(currentArg));
                
                // Don't want to do the inline ++ in case currentArg still gets
                // incremented on exception
                currentArg++;
            } catch (NumberFormatException e) {
                // If the first arg isn't an int, then it's the zone
                zone = args.get(currentArg++).trim();
                
                if (zone.length() > 1 && zone.charAt(0) == '\'' && zone.charAt(zone.length() - 1) == '\'') {
                    zone = zone.substring(1, zone.length() - 1);
                }
                
                try {
                    distance = Integer.parseInt(args.get(currentArg++));
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
                String term = args.get(i).trim();
                
                if (term.length() > 1 && term.charAt(0) == '\'' && term.charAt(term.length() - 1) == '\'') {
                    term = term.substring(1, term.length() - 1);
                }
                
                terms.add(term);
            }
        } else if (functionName.startsWith(Constants.CONTENT_ADJACENT_FUNCTION_NAME)) {
            // Pull off the zone if it's the zone adjacent function
            if (!args.get(currentArg).equalsIgnoreCase(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME)) {
                zone = args.get(currentArg++).trim();
                
                if (zone.length() > 1 && zone.charAt(0) == '\'' && zone.charAt(zone.length() - 1) == '\'') {
                    zone = zone.substring(1, zone.length() - 1);
                }
            }
            
            // Ensure the next term is the termOffsetMap variable
            if (!args.get(currentArg++).trim().equalsIgnoreCase(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME)) {
                throw new ParseException("Did not find the term offset map name where expected in the function arguments");
            }
            
            // Get the actual terms
            terms = new ArrayList<>(args.size() - currentArg);
            
            // Get the actual terms
            for (int i = currentArg; i < args.size(); i++) {
                String term = args.get(i).trim();
                
                if (term.length() > 1 && term.charAt(0) == '\'' && term.charAt(term.length() - 1) == '\'') {
                    term = term.substring(1, term.length() - 1);
                }
                
                terms.add(term);
            }
        } else if (functionName.startsWith(Constants.CONTENT_PHRASE_FUNCTION_NAME)) {
            // Pull off the zone if it's the zone phrase function
            if (!args.get(currentArg).equalsIgnoreCase(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME)) {
                zone = args.get(currentArg++).trim();
                
                if (zone.length() > 1 && zone.charAt(0) == '\'' && zone.charAt(zone.length() - 1) == '\'') {
                    zone = zone.substring(1, zone.length() - 1);
                }
            }
            
            // Ensure the next term is the termOffsetMap variable
            if (!args.get(currentArg++).trim().equalsIgnoreCase(Constants.TERM_OFFSET_MAP_JEXL_VARIABLE_NAME)) {
                throw new ParseException("Did not find the term offset map name where expected in the function arguments");
            }
            
            // Get the actual terms
            terms = new ArrayList<>(args.size() - currentArg);
            
            // Get the actual terms
            for (int i = currentArg; i < args.size(); i++) {
                String term = args.get(i).trim();
                
                if (term.length() > 1 && term.charAt(0) == '\'' && term.charAt(term.length() - 1) == '\'') {
                    term = term.substring(1, term.length() - 1);
                }
                
                terms.add(term.toLowerCase(Locale.ENGLISH));
            }
        } else {
            throw new ParseException("Unrecognized content function: " + f.name());
        }
    }
}
