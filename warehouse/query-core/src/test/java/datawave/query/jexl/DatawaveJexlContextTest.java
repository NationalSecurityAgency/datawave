package datawave.query.jexl;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertAll;

import org.apache.commons.jexl2.JexlEngine;
import org.junit.Test;

public class DatawaveJexlContextTest {

    private final JexlEngine engine = new JexlEngine();
    private final DatawaveJexlContext context = new DatawaveJexlContext();

    @Test
    public void testEscapingSingleQuote() {
        // @formatter:off
        assertAll(
                        // an escaped single quote within single quotes will match a single quote in the context
                        () -> {
                            context.set("F1", "Str\\ingWithquo'te");
                            assertTrue((Boolean) engine.createScript("F1 == 'Str\\\\ingWithquo\\'te'").execute(context));
                        },
                        // an escaped single quote within single quotes will not match an escaped single quotes in the context
                        () -> {
                            context.set("F1", "Str\\ingWithquo\\'te");
                            assertFalse((Boolean) engine.createScript("F1 == 'Str\\\\ingWithquo\\'te'").execute(context));
                        },
                        // an escaped double quote within single quotes will not match a double quote in the context
                        () -> {
                            context.set("F1", "Str\\ingWithquo\"te");
                            assertFalse((Boolean) engine.createScript("F1 == 'Str\\\\ingWithquo\\\"te'").execute(context));
                        },
                        // a backslash double quote within single quotes will match a backslash double quote in the context
                        () -> {
                            context.set("F1", "Str\\ingWithquo\\\"te");
                            assertTrue((Boolean) engine.createScript("F1 == 'Str\\\\ingWithquo\\\"te'").execute(context));
                        },
                        // an escaped backslash within single quotes will match a backslash in the context
                        () -> {
                            context.set("F1", "Str\\ing");
                            assertTrue((Boolean) engine.createScript("F1 == 'Str\\\\ing'").execute(context));
                        },
                        // a backslash within single quotes will match a backslash in the context
                        () -> {
                            context.set("F1", "Str\\ing");
                            assertTrue((Boolean) engine.createScript("F1 == 'Str\\ing'").execute(context));
                        },
                        // a backslash within single quotes will match a backslash in the context
                        () -> {
                            context.set("F1", "Str\\\\ing");
                            assertTrue((Boolean) engine.createScript("F1 == 'Str\\\\\\\\ing'").execute(context));
                        },
                        // to match a backslash followed by a single quote in the context, you must use an escaped backslash followed by an escaped single quote
                        () -> {
                            context.set("F1", "Str\\'ing");
                            assertTrue((Boolean) engine.createScript("F1 == 'Str\\\\\\'ing'").execute(context));
                        });
        // @formatter:on
    }

    @Test
    public void testEscapingDoubleQuote() {
        // @formatter:off
        assertAll(
                        // an escaped double quote within double quotes will match a double quote in the context
                        () -> {
                            context.set("F1", "Str\\ingWithquo\"te");
                            assertTrue((Boolean) engine.createScript("F1 == \"Str\\\\ingWithquo\\\"te\"").execute(context));
                        },
                        // an escaped double quote within double quotes will not match an escaped double quote in the context
                        () -> {
                            context.set("F1", "Str\\ingWithquo\\\"te");
                            assertFalse((Boolean) engine.createScript("F1 == \"Str\\\\ingWithquo\\\"te\"").execute(context));
                        },
                        // an escaped single quote within double quotes will not match a single quote in the context
                        () -> {
                            context.set("F1", "Str\\ingWithquo'te");
                            assertFalse((Boolean) engine.createScript("F1 == \"Str\\\\ingWithquo\\'te\"").execute(context));
                        },
                        // a backlash single quote within double quotes will match a backslash single quote in the context
                        () -> {
                            context.set("F1", "Str\\ingWithquo\\'te");
                            assertTrue((Boolean) engine.createScript("F1 == \"Str\\\\ingWithquo\\'te\"").execute(context));
                        },
                        // an escaped backslash within double quotes will match a backslash in the context
                        () -> {
                            context.set("F1", "Str\\ing");
                            assertTrue((Boolean) engine.createScript("F1 == \"Str\\\\ing\"").execute(context));
                        },
                        // a backslash within double quotes will match a backslash in the context
                        () -> {
                            context.set("F1", "Str\\ing");
                            assertTrue((Boolean) engine.createScript("F1 == \"Str\\ing\"").execute(context));
                        },
                        // a backslash within double quotes will match a backslash in the context
                        () -> {
                            context.set("F1", "Str\\\\ing");
                            assertTrue((Boolean) engine.createScript("F1 == \"Str\\\\\\\\ing\"").execute(context));
                        },
                        // to match a backslash followed by a double quote in the context, you must us e an escaped backslash followed be an escaped double
                        // quote
                        () -> {
                            context.set("F1", "Str\\\"ing");
                            assertTrue((Boolean) engine.createScript("F1 == \"Str\\\\\\\"ing\"").execute(context));
                        });
        // @formatter:on
    }

    @Test
    public void testEscapingUnicode() {
        // @formatter:off
        assertAll(() -> {
            context.set("F1", "Str∂");
            assertTrue((Boolean) engine.createScript("F1 == 'Str∂'").execute(context));
        }, () -> {
            context.set("F1", "Str∂");
            assertTrue((Boolean) engine.createScript("F1 == 'Str\u2202'").execute(context));
        }, () -> {
            context.set("F1", "Str\u2202");
            assertTrue((Boolean) engine.createScript("F1 == 'Str\u2202'").execute(context));
        }, () -> {
            context.set("F1", "Str\u2202");
            assertTrue((Boolean) engine.createScript("F1 == 'Str∂'").execute(context));
        });
        // @formatter:on
    }

}
