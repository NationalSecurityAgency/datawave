package datawave.query.jexl.functions;

import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.ExecutionException;

import datawave.query.config.ShardQueryConfiguration;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.jexl.ArithmeticJexlEngines;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.jexl.JexlNodeFactory.ContainerType;
import datawave.query.jexl.functions.arguments.JexlArgumentDescriptor;
import datawave.query.util.DateIndexHelper;
import datawave.query.util.MetadataHelper;
import datawave.webservice.query.exception.BadRequestQueryException;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.NotFoundQueryException;
import datawave.webservice.query.exception.PreConditionFailedQueryException;
import datawave.webservice.query.exception.QueryException;

import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.commons.jexl2.parser.*;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Sets;
import org.apache.commons.lang.mutable.MutableBoolean;

public class ContentFunctionsDescriptor implements JexlFunctionArgumentDescriptorFactory {
    
    /**
     * This is the JexlNode descriptor which can be used to normalize and optimize function node queries
     */
    public static class ContentJexlArgumentDescriptor implements JexlArgumentDescriptor {
        
        private final ASTFunctionNode node;
        private final String namespace, name;
        private final List<JexlNode> args;
        
        public ContentJexlArgumentDescriptor(ASTFunctionNode node, String namespace, String name, List<JexlNode> args) {
            this.node = node;
            this.namespace = namespace;
            this.name = name;
            this.args = args;
        }
        
        @Override
        public JexlNode getIndexQuery(ShardQueryConfiguration config, MetadataHelper helper, DateIndexHelper dateIndexHelper, Set<String> datatypeFilter) {
            try {
                return getIndexQuery(helper.getTermFrequencyFields(datatypeFilter), helper.getIndexedFields(datatypeFilter),
                                helper.getContentFields(datatypeFilter));
            } catch (TableNotFoundException e) {
                QueryException qe = new QueryException(DatawaveErrorCode.METADATA_TABLE_FETCH_ERROR, e);
                throw new DatawaveFatalQueryException(qe);
            } catch (ExecutionException e) {
                QueryException qe = new QueryException(DatawaveErrorCode.METADATA_TABLE_RECORD_FETCH_ERROR, e);
                throw new DatawaveFatalQueryException(qe);
            }
            
        }
        
        public JexlNode getIndexQuery(Set<String> termFrequencyFields, Set<String> indexedFields, Set<String> contentFields) {
            
            LinkedList<JexlNode> nodes = Lists.newLinkedList();
            
            // get the cartesian product of all the fields and terms
            MutableBoolean oredFields = new MutableBoolean();
            Set<String>[] fieldsAndTerms = fieldsAndTerms(termFrequencyFields, indexedFields, contentFields, oredFields, true);
            if (!fieldsAndTerms[0].isEmpty()) {
                final JexlNode eq = new ASTEQNode(ParserTreeConstants.JJTEQNODE);
                
                for (String field : fieldsAndTerms[0]) {
                    nodes.add(JexlNodeFactory.createNodeTreeFromFieldValues(ContainerType.AND_NODE, eq, null, field, fieldsAndTerms[1]));
                }
            }
            
            // A single field needs no wrapper node.
            if (1 == fieldsAndTerms[0].size()) {
                return nodes.iterator().next();
            } else if (oredFields.booleanValue()) {
                return JexlNodeFactory.createOrNode(nodes);
            } else {
                return JexlNodeFactory.createAndNode(nodes);
            }
        }
        
        @Override
        public Set<String> fieldsForNormalization(MetadataHelper helper, Set<String> datatypeFilter, int arg) {
            // normalize everything past the termOffsetMap
            
            Set<String> fields = Collections.emptySet();
            int firstTermIndex = 1;
            
            final String funcName = name;
            
            PeekingIterator<JexlNode> args = Iterators.peekingIterator(this.args.iterator());
            
            if (args.hasNext()) {
                if (ContentFunctions.CONTENT_ADJACENT_FUNCTION_NAME.equals(funcName) || ContentFunctions.CONTENT_PHRASE_FUNCTION_NAME.equals(funcName)) {
                    JexlNode firstArg = args.next();
                    
                    // we override the zones if the first argument is a string
                    if (firstArg instanceof ASTStringLiteral) {
                        firstTermIndex = 2;
                    } else {
                        JexlNode nextArg = args.peek();
                        
                        // The zones may (more likely) be specified as an idenfifier
                        if (!JexlASTHelper.getIdentifiers(firstArg).isEmpty() && !JexlASTHelper.getIdentifiers(nextArg).isEmpty()) {
                            firstTermIndex = 2;
                        }
                    }
                } else if (ContentFunctions.CONTENT_WITHIN_FUNCTION_NAME.equals(funcName)) {
                    firstTermIndex = 2;
                    JexlNode nextArg = args.next();
                    
                    // we override the zones if the first argument is a string or identifier
                    if (nextArg instanceof ASTStringLiteral || !JexlASTHelper.getIdentifiers(nextArg).isEmpty()) {
                        firstTermIndex = 3;
                        nextArg = args.next();
                    }
                    
                    // we can trash the distance
                    if (!(nextArg instanceof ASTNumberLiteral)) {
                        BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.NUMERIC_DISTANCE_ARGUMENT_MISSING);
                        throw new IllegalArgumentException(qe);
                    }
                } else {
                    BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.FUNCTION_ARGUMENTS_MISSING);
                    throw new IllegalArgumentException(qe);
                }
                
            } else {
                NotFoundQueryException qe = new NotFoundQueryException(DatawaveErrorCode.JEXL_NODES_MISSING, MessageFormat.format(
                                "Class: {0}, Namespace: {1}, Function: {2}", this.getClass().getSimpleName(), namespace, funcName));
                throw new IllegalArgumentException(qe);
            }
            
            if (arg >= firstTermIndex) {
                fields = fields(helper, datatypeFilter);
            }
            
            return fields;
        }
        
        @Override
        public Set<String> fields(MetadataHelper helper, Set<String> datatypeFilter) {
            try {
                return fieldsAndTerms(helper.getTermFrequencyFields(datatypeFilter), helper.getIndexedFields(datatypeFilter),
                                helper.getContentFields(datatypeFilter), null)[0];
            } catch (TableNotFoundException e) {
                QueryException qe = new QueryException(DatawaveErrorCode.METADATA_TABLE_FETCH_ERROR, e);
                throw new DatawaveFatalQueryException(qe);
            } catch (ExecutionException e) {
                QueryException qe = new QueryException(DatawaveErrorCode.METADATA_TABLE_RECORD_FETCH_ERROR, e);
                throw new DatawaveFatalQueryException(qe);
            }
            
        }
        
        @Override
        public Set<Set<String>> fieldSets(MetadataHelper helper, Set<String> datatypeFilter) {
            try {
                MutableBoolean oredFields = new MutableBoolean();
                Set<String>[] fieldsAndTerms = fieldsAndTerms(helper.getTermFrequencyFields(datatypeFilter), helper.getIndexedFields(datatypeFilter),
                                helper.getContentFields(datatypeFilter), oredFields);
                Set<Set<String>> fieldSets = new HashSet<>();
                if (oredFields.booleanValue()) {
                    for (String field : fieldsAndTerms[0]) {
                        fieldSets.add(Collections.singleton(field));
                    }
                } else {
                    fieldSets.add(fieldsAndTerms[0]);
                }
                return fieldSets;
            } catch (TableNotFoundException e) {
                QueryException qe = new QueryException(DatawaveErrorCode.METADATA_TABLE_FETCH_ERROR, e);
                throw new DatawaveFatalQueryException(qe);
            } catch (ExecutionException e) {
                QueryException qe = new QueryException(DatawaveErrorCode.METADATA_TABLE_RECORD_FETCH_ERROR, e);
                throw new DatawaveFatalQueryException(qe);
            }
            
        }
        
        public Set<String>[] fieldsAndTerms(Set<String> termFrequencyFields, Set<String> indexedFields, Set<String> contentFields, MutableBoolean oredFields) {
            return fieldsAndTerms(termFrequencyFields, indexedFields, contentFields, oredFields, false);
        }
        
        @SuppressWarnings("unchecked")
        public Set<String>[] fieldsAndTerms(Set<String> termFrequencyFields, Set<String> indexedFields, Set<String> contentFields, MutableBoolean oredFields,
                        boolean validateFields) {
            
            final String funcName = name;
            
            PeekingIterator<JexlNode> args = Iterators.peekingIterator(this.args.iterator());
            
            Set<String> termFreqFields = Sets.newHashSet(termFrequencyFields);
            Set<String> fields = Sets.newHashSetWithExpectedSize(termFreqFields.size());
            Set<String> terms = Sets.newHashSetWithExpectedSize(this.args.size() - 1);
            Iterator<String> itr = termFreqFields.iterator();
            // Can any one of the fields satisfy the query? Always true unless the zone is specified in an AND clause.
            if (oredFields != null) {
                oredFields.setValue(true);
            }
            while (itr.hasNext()) {
                String field = itr.next();
                if (indexedFields.contains(field) && (contentFields.isEmpty() || contentFields.contains(field))) {
                    fields.add(field);
                }
            }
            
            if (args.hasNext()) {
                JexlNode termOffsetMap = null;
                
                if (ContentFunctions.CONTENT_ADJACENT_FUNCTION_NAME.equals(funcName)) {
                    JexlNode firstArg = args.next();
                    
                    // we override the zones if the first argument is a string
                    if (firstArg instanceof ASTStringLiteral) {
                        fields = Collections.singleton(firstArg.image);
                        termOffsetMap = args.next();
                    } else {
                        JexlNode nextArg = args.peek();
                        
                        // The zones may (more likely) be specified as an idenfifier
                        if (!JexlASTHelper.getIdentifiers(firstArg).isEmpty() && !JexlASTHelper.getIdentifiers(nextArg).isEmpty()) {
                            if (oredFields != null && firstArg instanceof ASTAndNode) {
                                oredFields.setValue(false);
                            }
                            
                            fields = JexlASTHelper.getIdentifierNames(firstArg);
                            termOffsetMap = args.next();
                        } else {
                            termOffsetMap = firstArg;
                        }
                    }
                } else if (ContentFunctions.CONTENT_PHRASE_FUNCTION_NAME.equals(funcName)) {
                    JexlNode firstArg = args.next();
                    
                    if (firstArg instanceof ASTNumberLiteral || firstArg instanceof ASTUnaryMinusNode) {
                        // firstArg is max score value, skip
                        firstArg = args.next();
                    }
                    
                    // we override the zones if the first argument is a string
                    if (firstArg instanceof ASTStringLiteral) {
                        fields = Collections.singleton(firstArg.image);
                        
                        if (args.peek() instanceof ASTNumberLiteral || args.peek() instanceof ASTUnaryMinusNode) {
                            args.next(); // max score not needed for fields and terms
                        }
                        
                        termOffsetMap = args.next();
                    } else {
                        JexlNode nextArg = args.peek();
                        
                        // The zones may (more likely) be specified as an identifier
                        if (!JexlASTHelper.getIdentifiers(firstArg).isEmpty() && !JexlASTHelper.getIdentifiers(nextArg).isEmpty()) {
                            if (oredFields != null && firstArg instanceof ASTAndNode) {
                                oredFields.setValue(false);
                            }
                            
                            fields = JexlASTHelper.getIdentifierNames(firstArg);
                            termOffsetMap = args.next();
                        } else {
                            termOffsetMap = firstArg;
                        }
                    }
                } else if (ContentFunctions.CONTENT_WITHIN_FUNCTION_NAME.equals(funcName)) {
                    JexlNode arg = args.next();
                    
                    // we override the zones if the first argument is a string or identifier
                    if (arg instanceof ASTStringLiteral) {
                        fields = Collections.singleton(arg.image);
                        arg = args.next();
                    } else if (!JexlASTHelper.getIdentifiers(arg).isEmpty()) {
                        if (oredFields != null && arg instanceof ASTAndNode) {
                            oredFields.setValue(false);
                        }
                        
                        fields = JexlASTHelper.getIdentifierNames(arg);
                        arg = args.next();
                    }
                    
                    // we can trash the distance
                    if (!(arg instanceof ASTNumberLiteral)) {
                        BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.NUMERIC_DISTANCE_ARGUMENT_MISSING);
                        throw new IllegalArgumentException(qe);
                    }
                    
                    termOffsetMap = args.next();
                } else {
                    BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.FUNCTION_ARGUMENTS_MISSING);
                    throw new IllegalArgumentException(qe);
                }
                
                if (null == termOffsetMap || !(termOffsetMap instanceof ASTIdentifier)) {
                    BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.TERMOFFSETMAP_AND_TERMS_MISSING);
                    throw new IllegalArgumentException(qe);
                }
                
                if (!args.hasNext()) {
                    BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.TERMS_MISSING);
                    throw new IllegalArgumentException(qe);
                }
                
                // moving this validation later in the call stack, since it requires other processing (i.e. apply query model)
                if (validateFields) {
                    for (String field : fields) {
                        // deconstruct & upcase the fieldname for testing in case we have not normalized the field names yet. Return the unnormalized fieldname.
                        if (!termFreqFields.contains(JexlASTHelper.deconstructIdentifier(field.toUpperCase()))) {
                            PreConditionFailedQueryException qe = new PreConditionFailedQueryException(DatawaveErrorCode.FIELD_PHRASE_QUERY_NOT_INDEXED,
                                            MessageFormat.format("Field: {0}", field));
                            throw new IllegalArgumentException(qe);
                        }
                    }
                }
                
                // now take the remaining string literals as terms
                Iterator<String> termsItr = Iterators.transform(Iterators.filter(args, new StringLiteralsOnly()), new GetImage());
                while (termsItr.hasNext()) {
                    terms.add(termsItr.next());
                }
                
            } else {
                NotFoundQueryException qe = new NotFoundQueryException(DatawaveErrorCode.JEXL_NODES_MISSING, MessageFormat.format(
                                "Class: {0}, Namespace: {1}, Function: {2}", this.getClass().getSimpleName(), namespace, funcName));
                throw new IllegalArgumentException(qe);
            }
            
            return new Set[] {fields, terms};
        }
        
        @Override
        public boolean useOrForExpansion() {
            return true;
        }
        
        @Override
        public boolean regexArguments() {
            return false;
        }
    }
    
    @Override
    public ContentJexlArgumentDescriptor getArgumentDescriptor(ASTFunctionNode node) {
        FunctionJexlNodeVisitor fvis = new FunctionJexlNodeVisitor();
        fvis.visit(node, null);
        
        Class<?> functionClass = (Class<?>) ArithmeticJexlEngines.functions().get(fvis.namespace());
        
        if (!ContentFunctions.CONTENT_FUNCTION_NAMESPACE.equals(node.jjtGetChild(0).image)) {
            BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.JEXLNODEDESCRIPTOR_NAMESPACE_UNEXPECTED, MessageFormat.format(
                            "Class: {0}, Namespace: {1}", this.getClass().getSimpleName(), node.jjtGetChild(0).image));
            throw new IllegalArgumentException(qe);
        }
        if (!functionClass.equals(ContentFunctions.class)) {
            BadRequestQueryException qe = new BadRequestQueryException(DatawaveErrorCode.JEXLNODEDESCRIPTOR_NODE_FOR_FUNCTION, MessageFormat.format(
                            "Class: {0}, Function: {1}", this.getClass().getSimpleName(), functionClass));
            throw new IllegalArgumentException(qe);
        }
        
        return new ContentJexlArgumentDescriptor(node, fvis.namespace(), fvis.name(), fvis.args());
    }
    
}
