package datawave.query.jexl;

import org.apache.commons.jexl2.parser.ASTAdditiveNode;
import org.apache.commons.jexl2.parser.ASTAdditiveOperator;
import org.apache.commons.jexl2.parser.ASTAmbiguous;
import org.apache.commons.jexl2.parser.ASTAndNode;
import org.apache.commons.jexl2.parser.ASTArrayAccess;
import org.apache.commons.jexl2.parser.ASTAssignment;
import org.apache.commons.jexl2.parser.ASTBitwiseAndNode;
import org.apache.commons.jexl2.parser.ASTBitwiseComplNode;
import org.apache.commons.jexl2.parser.ASTBitwiseOrNode;
import org.apache.commons.jexl2.parser.ASTBitwiseXorNode;
import org.apache.commons.jexl2.parser.ASTBlock;
import org.apache.commons.jexl2.parser.ASTConstructorNode;
import org.apache.commons.jexl2.parser.ASTDivNode;
import org.apache.commons.jexl2.parser.ASTEQNode;
import org.apache.commons.jexl2.parser.ASTERNode;
import org.apache.commons.jexl2.parser.ASTEmptyFunction;
import org.apache.commons.jexl2.parser.ASTFalseNode;
import org.apache.commons.jexl2.parser.ASTForeachStatement;
import org.apache.commons.jexl2.parser.ASTFunctionNode;
import org.apache.commons.jexl2.parser.ASTGENode;
import org.apache.commons.jexl2.parser.ASTGTNode;
import org.apache.commons.jexl2.parser.ASTIdentifier;
import org.apache.commons.jexl2.parser.ASTIfStatement;
import org.apache.commons.jexl2.parser.ASTJexlScript;
import org.apache.commons.jexl2.parser.ASTLENode;
import org.apache.commons.jexl2.parser.ASTLTNode;
import org.apache.commons.jexl2.parser.ASTMapEntry;
import org.apache.commons.jexl2.parser.ASTMethodNode;
import org.apache.commons.jexl2.parser.ASTModNode;
import org.apache.commons.jexl2.parser.ASTMulNode;
import org.apache.commons.jexl2.parser.ASTNENode;
import org.apache.commons.jexl2.parser.ASTNRNode;
import org.apache.commons.jexl2.parser.ASTNotNode;
import org.apache.commons.jexl2.parser.ASTNullLiteral;
import org.apache.commons.jexl2.parser.ASTNumberLiteral;
import org.apache.commons.jexl2.parser.ASTOrNode;
import org.apache.commons.jexl2.parser.ASTReference;
import org.apache.commons.jexl2.parser.ASTReferenceExpression;
import org.apache.commons.jexl2.parser.ASTReturnStatement;
import org.apache.commons.jexl2.parser.ASTSizeFunction;
import org.apache.commons.jexl2.parser.ASTSizeMethod;
import org.apache.commons.jexl2.parser.ASTStringLiteral;
import org.apache.commons.jexl2.parser.ASTTernaryNode;
import org.apache.commons.jexl2.parser.ASTTrueNode;
import org.apache.commons.jexl2.parser.ASTUnaryMinusNode;
import org.apache.commons.jexl2.parser.ASTVar;
import org.apache.commons.jexl2.parser.ASTWhileStatement;
import org.apache.commons.jexl2.parser.JexlNode;
import org.apache.commons.jexl2.parser.ParserTreeConstants;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * This class provides convenient ways to create new instances of {@link JexlNode} without needing to explicitly specify the class type.
 * 
 * @param <T>
 *            the {@link JexlNode} type that wil
 */
public class JexlNodeInstance<T extends JexlNode> {
    
    private static final Map<Class<? extends JexlNode>,JexlNodeInstance<?>> classToInstances = new HashMap<>();
    
    private static <T extends JexlNode> JexlNodeInstance<T> createInstance(Supplier<T> constructor) {
        JexlNodeInstance<T> instance = new JexlNodeInstance<>(constructor);
        Class<? extends JexlNode> clazz = constructor.get().getClass();
        classToInstances.put(clazz, instance);
        return instance;
    }
    
    public static final JexlNodeInstance<org.apache.commons.jexl2.parser.ASTJexlScript> ASTJexlScript = createInstance(() -> new ASTJexlScript(
                    ParserTreeConstants.JJTJEXLSCRIPT));
    public static final JexlNodeInstance<org.apache.commons.jexl2.parser.ASTBlock> ASTBlock = createInstance(() -> new ASTBlock(ParserTreeConstants.JJTBLOCK));
    public static final JexlNodeInstance<org.apache.commons.jexl2.parser.ASTAmbiguous> ASTAmbiguous = createInstance(() -> new ASTAmbiguous(
                    ParserTreeConstants.JJTAMBIGUOUS));
    public static final JexlNodeInstance<org.apache.commons.jexl2.parser.ASTIfStatement> ASTIfStatement = createInstance(() -> new ASTIfStatement(
                    ParserTreeConstants.JJTIFSTATEMENT));
    public static final JexlNodeInstance<org.apache.commons.jexl2.parser.ASTWhileStatement> ASTWhileStatement = createInstance(() -> new ASTWhileStatement(
                    ParserTreeConstants.JJTWHILESTATEMENT));
    public static final JexlNodeInstance<org.apache.commons.jexl2.parser.ASTForeachStatement> ASTForeachStatement = createInstance(() -> new ASTForeachStatement(
                    ParserTreeConstants.JJTFOREACHSTATEMENT));
    public static final JexlNodeInstance<org.apache.commons.jexl2.parser.ASTReturnStatement> ASTReturnStatement = createInstance(() -> new ASTReturnStatement(
                    ParserTreeConstants.JJTRETURNSTATEMENT));
    public static final JexlNodeInstance<org.apache.commons.jexl2.parser.ASTAssignment> ASTAssignment = createInstance(() -> new ASTAssignment(
                    ParserTreeConstants.JJTASSIGNMENT));
    public static final JexlNodeInstance<org.apache.commons.jexl2.parser.ASTVar> ASTVar = createInstance(() -> new ASTVar(ParserTreeConstants.JJTVAR));
    public static final JexlNodeInstance<org.apache.commons.jexl2.parser.ASTReference> ASTReference = createInstance(() -> new ASTReference(
                    ParserTreeConstants.JJTREFERENCE));
    public static final JexlNodeInstance<org.apache.commons.jexl2.parser.ASTTernaryNode> ASTTernaryNode = createInstance(() -> new ASTTernaryNode(
                    ParserTreeConstants.JJTTERNARYNODE));
    public static final JexlNodeInstance<org.apache.commons.jexl2.parser.ASTOrNode> ASTOrNode = createInstance(() -> new ASTOrNode(
                    ParserTreeConstants.JJTORNODE));
    public static final JexlNodeInstance<org.apache.commons.jexl2.parser.ASTAndNode> ASTAndNode = createInstance(() -> new ASTAndNode(
                    ParserTreeConstants.JJTANDNODE));
    public static final JexlNodeInstance<org.apache.commons.jexl2.parser.ASTBitwiseOrNode> ASTBitwiseOrNode = createInstance(() -> new ASTBitwiseOrNode(
                    ParserTreeConstants.JJTBITWISEORNODE));
    public static final JexlNodeInstance<org.apache.commons.jexl2.parser.ASTBitwiseXorNode> ASTBitwiseXorNode = createInstance(() -> new ASTBitwiseXorNode(
                    ParserTreeConstants.JJTBITWISEXORNODE));
    public static final JexlNodeInstance<org.apache.commons.jexl2.parser.ASTBitwiseAndNode> ASTBitwiseAndNode = createInstance(() -> new ASTBitwiseAndNode(
                    ParserTreeConstants.JJTBITWISEANDNODE));
    public static final JexlNodeInstance<org.apache.commons.jexl2.parser.ASTEQNode> ASTEQNode = createInstance(() -> new ASTEQNode(
                    ParserTreeConstants.JJTEQNODE));
    public static final JexlNodeInstance<org.apache.commons.jexl2.parser.ASTNENode> ASTNENode = createInstance(() -> new ASTNENode(
                    ParserTreeConstants.JJTNENODE));
    public static final JexlNodeInstance<org.apache.commons.jexl2.parser.ASTLTNode> ASTLTNode = createInstance(() -> new ASTLTNode(
                    ParserTreeConstants.JJTLTNODE));
    public static final JexlNodeInstance<org.apache.commons.jexl2.parser.ASTGTNode> ASTGTNode = createInstance(() -> new ASTGTNode(
                    ParserTreeConstants.JJTGTNODE));
    public static final JexlNodeInstance<org.apache.commons.jexl2.parser.ASTLENode> ASTLENode = createInstance(() -> new ASTLENode(
                    ParserTreeConstants.JJTLENODE));
    public static final JexlNodeInstance<org.apache.commons.jexl2.parser.ASTGENode> ASTGENode = createInstance(() -> new ASTGENode(
                    ParserTreeConstants.JJTGENODE));
    public static final JexlNodeInstance<org.apache.commons.jexl2.parser.ASTERNode> ASTERNode = createInstance(() -> new ASTERNode(
                    ParserTreeConstants.JJTERNODE));
    public static final JexlNodeInstance<org.apache.commons.jexl2.parser.ASTNRNode> ASTNRNode = createInstance(() -> new ASTNRNode(
                    ParserTreeConstants.JJTNRNODE));
    public static final JexlNodeInstance<org.apache.commons.jexl2.parser.ASTAdditiveNode> ASTAdditiveNode = createInstance(() -> new ASTAdditiveNode(
                    ParserTreeConstants.JJTADDITIVENODE));
    public static final JexlNodeInstance<org.apache.commons.jexl2.parser.ASTAdditiveOperator> ASTAdditiveOperator = createInstance(() -> new ASTAdditiveOperator(
                    ParserTreeConstants.JJTADDITIVEOPERATOR));
    public static final JexlNodeInstance<org.apache.commons.jexl2.parser.ASTMulNode> ASTMulNode = createInstance(() -> new ASTMulNode(
                    ParserTreeConstants.JJTMULNODE));
    public static final JexlNodeInstance<org.apache.commons.jexl2.parser.ASTDivNode> ASTDivNode = createInstance(() -> new ASTDivNode(
                    ParserTreeConstants.JJTDIVNODE));
    public static final JexlNodeInstance<org.apache.commons.jexl2.parser.ASTModNode> ASTModNode = createInstance(() -> new ASTModNode(
                    ParserTreeConstants.JJTMODNODE));
    public static final JexlNodeInstance<org.apache.commons.jexl2.parser.ASTUnaryMinusNode> ASTUnaryMinusNode = createInstance(() -> new ASTUnaryMinusNode(
                    ParserTreeConstants.JJTUNARYMINUSNODE));
    public static final JexlNodeInstance<org.apache.commons.jexl2.parser.ASTBitwiseComplNode> ASTBitwiseComplNode = createInstance(() -> new ASTBitwiseComplNode(
                    ParserTreeConstants.JJTBITWISECOMPLNODE));
    public static final JexlNodeInstance<org.apache.commons.jexl2.parser.ASTNotNode> ASTNotNode = createInstance(() -> new ASTNotNode(
                    ParserTreeConstants.JJTNOTNODE));
    public static final JexlNodeInstance<org.apache.commons.jexl2.parser.ASTIdentifier> ASTIdentifier = createInstance(() -> new ASTIdentifier(
                    ParserTreeConstants.JJTIDENTIFIER));
    public static final JexlNodeInstance<org.apache.commons.jexl2.parser.ASTNullLiteral> ASTNullLiteral = createInstance(() -> new ASTNullLiteral(
                    ParserTreeConstants.JJTNULLLITERAL));
    public static final JexlNodeInstance<org.apache.commons.jexl2.parser.ASTTrueNode> ASTTrueNode = createInstance(() -> new ASTTrueNode(
                    ParserTreeConstants.JJTTRUENODE));
    public static final JexlNodeInstance<org.apache.commons.jexl2.parser.ASTFalseNode> ASTFalseNode = createInstance(() -> new ASTFalseNode(
                    ParserTreeConstants.JJTFALSENODE));
    public static final JexlNodeInstance<org.apache.commons.jexl2.parser.ASTNumberLiteral> ASTNumberLiteral = createInstance(() -> new ASTNumberLiteral(
                    ParserTreeConstants.JJTNUMBERLITERAL));
    public static final JexlNodeInstance<org.apache.commons.jexl2.parser.ASTStringLiteral> ASTStringLiteral = createInstance(() -> new ASTStringLiteral(
                    ParserTreeConstants.JJTSTRINGLITERAL));
    public static final JexlNodeInstance<org.apache.commons.jexl2.parser.ASTMapEntry> ASTMapEntry = createInstance(() -> new ASTMapEntry(
                    ParserTreeConstants.JJTMAPENTRY));
    public static final JexlNodeInstance<org.apache.commons.jexl2.parser.ASTEmptyFunction> ASTEmptyFunction = createInstance(() -> new ASTEmptyFunction(
                    ParserTreeConstants.JJTEMPTYFUNCTION));
    public static final JexlNodeInstance<org.apache.commons.jexl2.parser.ASTSizeFunction> ASTSizeFunction = createInstance(() -> new ASTSizeFunction(
                    ParserTreeConstants.JJTSIZEFUNCTION));
    public static final JexlNodeInstance<org.apache.commons.jexl2.parser.ASTFunctionNode> ASTFunctionNode = createInstance(() -> new ASTFunctionNode(
                    ParserTreeConstants.JJTFUNCTIONNODE));
    public static final JexlNodeInstance<org.apache.commons.jexl2.parser.ASTMethodNode> ASTMethodNode = createInstance(() -> new ASTMethodNode(
                    ParserTreeConstants.JJTFUNCTIONNODE));
    public static final JexlNodeInstance<org.apache.commons.jexl2.parser.ASTSizeMethod> ASTSizeMethod = createInstance(() -> new ASTSizeMethod(
                    ParserTreeConstants.JJTSIZEMETHOD));
    public static final JexlNodeInstance<org.apache.commons.jexl2.parser.ASTConstructorNode> ASTConstructorNode = createInstance(() -> new ASTConstructorNode(
                    ParserTreeConstants.JJTCONSTRUCTORNODE));
    public static final JexlNodeInstance<org.apache.commons.jexl2.parser.ASTArrayAccess> ASTArrayAccess = createInstance(() -> new ASTArrayAccess(
                    ParserTreeConstants.JJTARRAYACCESS));
    public static final JexlNodeInstance<org.apache.commons.jexl2.parser.ASTReferenceExpression> ASTReferenceExpression = createInstance(() -> new ASTReferenceExpression(
                    ParserTreeConstants.JJTREFERENCEEXPRESSION));
    
    /**
     * Return a {@link JexlNodeInstance} that will create a {@link JexlNode} of the same type as the provided class.
     * 
     * @param type
     *            the type
     * @return the {@link JexlNodeInstance}
     */
    public static JexlNodeInstance<?> forType(Class<? extends JexlNode> type) {
        JexlNodeInstance<?> instance = classToInstances.get(type);
        if (instance == null) {
            throw new IllegalArgumentException("No " + JexlNodeInstance.class.getName() + " registered for type " + type.getName());
        }
        return instance;
    }
    
    /**
     * Return a {@link JexlNodeInstance} that will create a {@link JexlNode} of the same type as the provided node.
     * 
     * @param node
     *            the node
     * @return the {@link JexlNodeInstance}
     */
    public static JexlNodeInstance<?> forType(JexlNode node) {
        return forType(node.getClass());
    }
    
    private final Supplier<T> constructor;
    
    private final Class<T> type;
    
    @SuppressWarnings("unchecked")
    private JexlNodeInstance(Supplier<T> constructor) {
        this.constructor = constructor;
        this.type = (Class<T>) constructor.get().getClass();
    }
    
    /**
     * Return a {@link Supplier} that provides a callable constructor to create a new {@link JexlNode} of type {@code T}.
     * 
     * @return the constructor
     */
    public Supplier<T> getConstructor() {
        return constructor;
    }
    
    /**
     * Return the {@link Class} for the type {@code T}.
     * 
     * @return the type {@code T} class
     */
    public Class<T> getType() {
        return type;
    }
    
    /**
     * Return a new {@link JexlNode} of type {@code T}.
     * 
     * @return the new node
     */
    public T create() {
        return constructor.get();
    }
}
