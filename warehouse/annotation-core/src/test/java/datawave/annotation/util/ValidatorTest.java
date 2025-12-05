package datawave.annotation.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ValidatorTest {
    static class Person {
        String name;
        int size;
        LocalDate start;
        LocalDate end;
    }

    static class Container {
        String type;
        List<Person> children;
        Person favoriteChild;
    }

    Person alice, bob, charlie, drew;
    Container container;

    @BeforeEach
    public void setUpData() {
        drew = new Person();
        drew.name = "Drew";
        drew.size = 52;
        drew.start = LocalDate.parse("2020-10-10", dateFormat);
        drew.end = LocalDate.parse("2025-05-05", dateFormat);

        alice = new Person();
        alice.name = "Alice";
        alice.size = 10;

        bob = new Person();
        bob.name = "Bob";
        bob.size = 10;

        charlie = new Person();
        charlie.name = "Charlie";
        charlie.size = 10;
        charlie.start = LocalDate.parse("2020-10-10", dateFormat);
        charlie.end = LocalDate.parse("2025-05-05", dateFormat);

        container = new Container();
        container.type = "People";
        container.children = List.of(alice, bob);
        container.favoriteChild = charlie;
    }

    DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    Validator<Person> validator = Validator.<Person> create().addCheck(o -> o.name != null && !o.name.isBlank(), "Name is required")
                    .addCheck(o -> o.size > 0, "Size must be larger than zero").addCheck(o -> o.start != null, "Start date must not be null")
                    .addCheck(o -> o.end != null, "End date must not be null")
                    .addCheck(o -> o.end != null && o.start != null && o.end.isAfter(o.start), "Start date must be prior to end date");

    @Test
    public void testSimpleValidation() {
        assertTrue(validator.check(drew).isValid());
    }

    @Test
    public void testSimpleValidationInvalidFailFast() {
        Validator.ValidationState<Person> validationState = validator.check(alice);
        assertFalse(validationState.isValid());
        List<String> errors = validationState.getErrors();
        assertEquals(1, errors.size());
        String error = errors.get(0);
        assertTrue(error.contains("not be null"), error);
    }

    @Test
    public void testSimpleValidationInvalidFailSlow() {
        Validator.ValidationState<Person> validationState = validator.check(alice, false);
        assertFalse(validationState.isValid());
        List<String> errors = validationState.getErrors();
        assertEquals(3, errors.size());
        String error = errors.get(0);
        assertTrue(error.contains("not be null"), error);
    }

    @Test
    public void testMemberValidation() {
        Validator<Person> childValidator = Validator.<Person> create().addCheck(o -> o.name != null && !o.name.isBlank(), "Name is required")
                        .addCheck(o -> o.size > 0, "Size must be larger than zero").addCheck(o -> o.start == null, "Start date must be null")
                        .addCheck(o -> o.end == null, "End date must be null");

        Validator<Container> validator = Validator.<Container> create().addCheck(o -> o.type != null && !o.type.isBlank(), "Type is required")
                        .addCheck(o -> o.children != null, "Children must not be null").addCheck(o -> !o.children.isEmpty(), "Children must not be empty")
                        .addCheck(o -> o.favoriteChild != null, "Favorite child must not be null").addMemberValidator(o -> o.children, childValidator)
                        .addMemberValidator(o -> Collections.singletonList(o.favoriteChild), u -> this.validator);

        assertTrue(validator.check(container).isValid());
    }

    @Test
    public void testMemberValidationFail() {

        container.favoriteChild = alice;

        Validator<Person> childValidator = Validator.<Person> create().addCheck(o -> o.name != null && !o.name.isBlank(), "Name is required")
                        .addCheck(o -> o.size > 0, "Size must be larger than zero").addCheck(o -> o.start == null, "Start date must be null")
                        .addCheck(o -> o.end == null, "End date must be null");

        Validator<Container> validator = Validator.<Container> create().addCheck(o -> o.type != null && !o.type.isBlank(), "Type is required")
                        .addCheck(o -> o.children != null, "Children must not be null").addCheck(o -> !o.children.isEmpty(), "Children must not be empty")
                        .addCheck(o -> o.favoriteChild != null, "Favorite child must not be null").addMemberValidator(o -> o.children, childValidator)
                        .addMemberValidator(o -> Collections.singletonList(o.favoriteChild), u -> this.validator);

        Validator.ValidationState<Container> validationState = validator.check(container, true);
        assertFalse(validationState.isValid());
        List<String> errors = validationState.getErrors();
        assertEquals(1, errors.size());
        String error = errors.get(0);
        assertTrue(error.contains("not be null"), error);

    }
}
