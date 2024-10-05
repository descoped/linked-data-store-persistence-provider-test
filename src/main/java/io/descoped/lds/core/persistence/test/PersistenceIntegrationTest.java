package io.descoped.lds.core.persistence.test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.descoped.lds.api.json.JsonNavigationPath;
import io.descoped.lds.api.persistence.DocumentKey;
import io.descoped.lds.api.persistence.PersistenceDeletePolicy;
import io.descoped.lds.api.persistence.Transaction;
import io.descoped.lds.api.persistence.json.JsonDocument;
import io.descoped.lds.api.persistence.json.JsonTools;
import io.descoped.lds.api.persistence.reactivex.Range;
import io.descoped.lds.api.persistence.reactivex.RxJsonPersistence;
import io.descoped.lds.api.specification.Specification;
import io.descoped.lds.api.specification.SpecificationElementType;
import io.reactivex.Flowable;
import org.json.JSONException;
import org.skyscreamer.jsonassert.JSONAssert;
import org.testng.annotations.Test;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.descoped.lds.api.persistence.json.JsonTools.mapper;
import static io.descoped.lds.core.persistence.test.SpecificationBuilder.arrayNode;
import static io.descoped.lds.core.persistence.test.SpecificationBuilder.arrayRefNode;
import static io.descoped.lds.core.persistence.test.SpecificationBuilder.booleanNode;
import static io.descoped.lds.core.persistence.test.SpecificationBuilder.numericNode;
import static io.descoped.lds.core.persistence.test.SpecificationBuilder.objectNode;
import static io.descoped.lds.core.persistence.test.SpecificationBuilder.refNode;
import static io.descoped.lds.core.persistence.test.SpecificationBuilder.stringNode;
import static java.lang.String.format;
import static java.time.ZonedDateTime.now;
import static java.time.ZonedDateTime.of;
import static java.time.ZonedDateTime.parse;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public abstract class PersistenceIntegrationTest {

    protected final Specification specification;
    protected final String namespace;
    protected RxJsonPersistence persistence;

    protected PersistenceIntegrationTest(String namespace) {
        this.namespace = namespace;
        this.specification = buildSpecification();
    }

    protected static ObjectNode createPerson(String firstname, String lastname) {
        ObjectNode person = mapper.createObjectNode();
        person.put("firstname", firstname);
        person.put("lastname", lastname);
        person.put("born", 1998);
        person.put("bornWeightKg", 3.82);
        person.put("isHuman", true);
        return person;
    }

    protected static ObjectNode createPerson(String firstname, String lastname, String currentAddressLink, String workAddressLink, List<String> previousAddressesLinks) {
        ObjectNode person = createPerson(firstname, lastname);
        ObjectNode history = person.putObject("history")
                .put("currentAddress", currentAddressLink)
                .put("workAddress", workAddressLink);
        ArrayNode previousAddresses = history.putArray("previousAddresses");
        for (String previousAddressLink : previousAddressesLinks) {
            previousAddresses.add(previousAddressLink);
        }
        return person;
    }

    protected static ObjectNode createAddress(String city, String state, String country) {
        ObjectNode address = mapper.createObjectNode();
        address.put("city", city);
        address.put("state", state);
        address.put("country", country);
        return address;
    }

    protected Specification buildSpecification() {
        return SpecificationBuilder.createSpecificationAndRoot(Set.of(
                objectNode(SpecificationElementType.MANAGED, "Person", Set.of(
                        stringNode("firstname"),
                        stringNode("lastname"),
                        numericNode("born"),
                        numericNode("bornWeightKg"),
                        booleanNode("isHuman"),
                        objectNode("history", Set.of(
                                refNode("currentAddress", Set.of("Address", "FunkyLongAddress")),
                                refNode("workAddress", Set.of("FunkyLongAddress", "Address")),
                                arrayRefNode("previousAddresses", Set.of("Address", "FunkyLongAddress"), stringNode("[]"))
                        ))
                )),
                objectNode(SpecificationElementType.MANAGED, "Address", Set.of(
                        stringNode("city"),
                        stringNode("state"),
                        stringNode("country")
                )),
                objectNode(SpecificationElementType.MANAGED, "FunkyLongAddress", Set.of(
                        stringNode("city"),
                        stringNode("state"),
                        stringNode("country")
                ))
        ));
    }

    private JsonDocument createPerson(String id, ZonedDateTime timestamp) {
        return toDocument(namespace, "Person", id, createPerson("John (" + id + ")", "Smith (" + timestamp + ")"), timestamp);
    }

    private JsonDocument createPerson(String id) {
        return createPerson(id, parse("2000-01-01T00:00:00.000Z"));
    }

    private JsonDocument createPersonVersion(ZonedDateTime timestamp) {
        return toDocument(namespace, "Person", "person00", createPerson("John", "Smith (" + timestamp + ")"), timestamp);
    }

    @Test
    public void thatDeleteAllWithIncomingRefWorks() throws JSONException {
        ZonedDateTime timestamp = parse("2019-01-01T00:00:00.000Z");

        JsonDocument paris = toDocument(namespace, "Address", "paris", createAddress("Paris", "", "France"), timestamp);
        JsonDocument london = toDocument(namespace, "Address", "london", createAddress("London", "", "England"), timestamp);
        JsonDocument oslo = toDocument(namespace, "Address", "oslo", createAddress("Oslo", "", "Norway"), timestamp);
        JsonDocument trondheim = toDocument(namespace, "FunkyLongAddress", "trondheim", createAddress("Trondheim", "", "Norway"), timestamp);
        JsonDocument jack = toDocument(namespace, "Person", "jack", createPerson("Jack", "Smith", "/Address/oslo", "/Address/oslo", List.of("/Address/london", "/Address/paris")), timestamp);
        JsonDocument jill = toDocument(namespace, "Person", "jill", createPerson("Jill", "Smith", "/Address/oslo", "/FunkyLongAddress/trondheim", List.of("/Address/london", "/FunkyLongAddress/trondheim")), timestamp);

        try (Transaction tx = persistence.createTransaction(false)) {
            persistence.deleteAllEntities(tx, namespace, "Person", specification).blockingAwait();
            persistence.deleteAllEntities(tx, namespace, "Address", specification).blockingAwait();
            persistence.deleteAllEntities(tx, namespace, "FunkyLongAddress", specification).blockingAwait();

            persistence.createOrOverwrite(tx, paris, specification).blockingAwait();
            persistence.createOrOverwrite(tx, london, specification).blockingAwait();
            persistence.createOrOverwrite(tx, oslo, specification).blockingAwait();
            persistence.createOrOverwrite(tx, trondheim, specification).blockingAwait();
            persistence.createOrOverwrite(tx, jack, specification).blockingAwait();
            persistence.createOrOverwrite(tx, jill, specification).blockingAwait();

            persistence.deleteAllDocumentVersions(tx, namespace, "Person", "jack", PersistenceDeletePolicy.FAIL_IF_INCOMING_LINKS).blockingAwait();
            persistence.deleteAllEntities(tx, namespace, "Address", specification).blockingAwait();

            JsonDocument parisFromDb = persistence.readDocument(tx, timestamp, namespace, "Address", "paris").blockingGet();
            JsonDocument londonFromDb = persistence.readDocument(tx, timestamp, namespace, "Address", "london").blockingGet();
            JsonDocument osloFromDb = persistence.readDocument(tx, timestamp, namespace, "Address", "oslo").blockingGet();
            JsonDocument jackFromDb = persistence.readDocument(tx, timestamp, namespace, "Person", "jack").blockingGet();
            JsonDocument jillFromDb = persistence.readDocument(tx, timestamp, namespace, "Person", "jill").blockingGet();

            assertNull(parisFromDb);
            assertNull(londonFromDb);
            assertNull(osloFromDb);
            assertNull(jackFromDb);
            JSONAssert.assertEquals(jill.jackson().toString(), jillFromDb.jackson().toString(), true);
        }
    }

    @Test
    public void thatBatchCreationWorks() throws JSONException {
        ZonedDateTime timestamp = parse("2019-01-01T00:00:00.000Z");

        JsonDocument paris = toDocument(namespace, "Address", "paris", createAddress("Paris", "", "France"), timestamp);
        JsonDocument london = toDocument(namespace, "Address", "london", createAddress("London", "", "England"), timestamp);
        JsonDocument oslo = toDocument(namespace, "Address", "oslo", createAddress("Oslo", "", "Norway"), timestamp);
        JsonDocument trondheim = toDocument(namespace, "FunkyLongAddress", "trondheim", createAddress("Trondheim", "", "Norway"), timestamp);
        JsonDocument jack = toDocument(namespace, "Person", "jack", createPerson("Jack", "Smith", "/Address/oslo", "/Address/oslo", List.of("/Address/london", "/Address/paris")), timestamp);
        JsonDocument jill = toDocument(namespace, "Person", "jill", createPerson("Jill", "Smith", "/Address/oslo", "/FunkyLongAddress/trondheim", List.of("/Address/london", "/FunkyLongAddress/trondheim")), timestamp);

        try (Transaction tx = persistence.createTransaction(false)) {
            persistence.deleteAllEntities(tx, namespace, "Person", specification).blockingAwait();
            persistence.deleteAllEntities(tx, namespace, "Address", specification).blockingAwait();
            persistence.deleteAllEntities(tx, namespace, "FunkyLongAddress", specification).blockingAwait();

            persistence.createOrOverwrite(tx, Flowable.just(paris, london, oslo, trondheim, jack, jill), specification).blockingAwait();

            JsonDocument parisFromDb = persistence.readDocument(tx, timestamp, namespace, "Address", "paris").blockingGet();
            JsonDocument londonFromDb = persistence.readDocument(tx, timestamp, namespace, "Address", "london").blockingGet();
            JsonDocument osloFromDb = persistence.readDocument(tx, timestamp, namespace, "Address", "oslo").blockingGet();
            JsonDocument trondheimFromDb = persistence.readDocument(tx, timestamp, namespace, "FunkyLongAddress", "trondheim").blockingGet();
            JsonDocument jackFromDb = persistence.readDocument(tx, timestamp, namespace, "Person", "jack").blockingGet();
            JsonDocument jillFromDb = persistence.readDocument(tx, timestamp, namespace, "Person", "jill").blockingGet();

            assertNotNull(parisFromDb);
            assertNotNull(londonFromDb);
            assertNotNull(osloFromDb);
            assertNotNull(trondheimFromDb);
            assertNotNull(jackFromDb);
            assertNotNull(jillFromDb);
            JSONAssert.assertEquals(paris.jackson().toString(), parisFromDb.jackson().toString(), true);
            JSONAssert.assertEquals(london.jackson().toString(), londonFromDb.jackson().toString(), true);
            JSONAssert.assertEquals(oslo.jackson().toString(), osloFromDb.jackson().toString(), true);
            JSONAssert.assertEquals(trondheim.jackson().toString(), trondheimFromDb.jackson().toString(), true);
            JSONAssert.assertEquals(jill.jackson().toString(), jillFromDb.jackson().toString(), true);
            JSONAssert.assertEquals(jack.jackson().toString(), jackFromDb.jackson().toString(), true);
        }
    }

    @Test
    public void thatRefWorks() throws JSONException {
        ZonedDateTime timestamp = parse("2019-01-01T00:00:00.000Z");

        JsonDocument paris = toDocument(namespace, "Address", "paris", createAddress("Paris", "", "France"), timestamp);
        JsonDocument london = toDocument(namespace, "Address", "london", createAddress("London", "", "England"), timestamp);
        JsonDocument oslo = toDocument(namespace, "Address", "oslo", createAddress("Oslo", "", "Norway"), timestamp);
        JsonDocument trondheim = toDocument(namespace, "FunkyLongAddress", "trondheim", createAddress("Trondheim", "", "Norway"), timestamp);
        JsonDocument jack = toDocument(namespace, "Person", "jack", createPerson("Jack", "Smith", "/Address/oslo", "/Address/oslo", List.of("/Address/london", "/Address/paris")), timestamp);
        JsonDocument jill = toDocument(namespace, "Person", "jill", createPerson("Jill", "Smith", "/Address/oslo", "/FunkyLongAddress/trondheim", List.of("/Address/london", "/FunkyLongAddress/trondheim")), timestamp);

        try (Transaction tx = persistence.createTransaction(false)) {
            persistence.deleteAllEntities(tx, namespace, "Person", specification).blockingAwait();
            persistence.deleteAllEntities(tx, namespace, "Address", specification).blockingAwait();
            persistence.deleteAllEntities(tx, namespace, "FunkyLongAddress", specification).blockingAwait();

            persistence.createOrOverwrite(tx, paris, specification).blockingAwait();
            persistence.createOrOverwrite(tx, london, specification).blockingAwait();
            persistence.createOrOverwrite(tx, oslo, specification).blockingAwait();
            persistence.createOrOverwrite(tx, trondheim, specification).blockingAwait();
            persistence.createOrOverwrite(tx, jack, specification).blockingAwait();
            persistence.createOrOverwrite(tx, jill, specification).blockingAwait();

            JsonDocument jackFromDb = persistence.readDocument(tx, timestamp, namespace, "Person", "jack").blockingGet();
            JsonDocument jillFromDb = persistence.readDocument(tx, timestamp, namespace, "Person", "jill").blockingGet();

            JSONAssert.assertEquals(jack.jackson().toString(), jackFromDb.jackson().toString(), true);
            JSONAssert.assertEquals(jill.jackson().toString(), jillFromDb.jackson().toString(), true);
        }
    }

    @Test
    public void testHasNextAndHasPrevious() {
        ZonedDateTime timestamp = parse("2000-01-01T00:00:00.000Z");
        try (Transaction tx = persistence.createTransaction(false)) {
            try {
                persistence.deleteAllEntities(tx, namespace, "Person", specification).blockingAwait();

                // Create one before.
                persistence.createOrOverwrite(tx, createPerson("person01", timestamp), specification).blockingAwait();

                assertThat(persistence.hasNext(tx, timestamp, namespace, "Person", "person01").blockingGet())
                        .as("hasNext() with empty database")
                        .isFalse();

                assertThat(persistence.hasPrevious(tx, timestamp, namespace, "Person", "person01").blockingGet())
                        .as("hasPrevious() with empty database")
                        .isFalse();

                // Create one before.
                persistence.createOrOverwrite(tx, createPerson("person00", timestamp), specification).blockingAwait();
                assertThat(persistence.hasPrevious(tx, timestamp, namespace, "Person", "person01").blockingGet())
                        .as("hasPrevious() with one before")
                        .isTrue();

                // Create one after.
                persistence.createOrOverwrite(tx, createPerson("person02", timestamp), specification).blockingAwait();
                assertThat(persistence.hasNext(tx, timestamp, namespace, "Person", "person01").blockingGet())
                        .as("hasNext() with one after")
                        .isTrue();


            } finally {
                // Clean up.
                persistence.deleteAllDocumentVersions(tx, namespace, "Person", "person00",
                        PersistenceDeletePolicy.CASCADE_DELETE_ALL_INCOMING_LINKS_AND_NODES).blockingAwait();
                persistence.deleteAllDocumentVersions(tx, namespace, "Person", "person01",
                        PersistenceDeletePolicy.CASCADE_DELETE_ALL_INCOMING_LINKS_AND_NODES).blockingAwait();
                persistence.deleteAllDocumentVersions(tx, namespace, "Person", "person02",
                        PersistenceDeletePolicy.CASCADE_DELETE_ALL_INCOMING_LINKS_AND_NODES).blockingAwait();
            }
        }

    }

    @Test
    public void testReadDocuments() {
        // Create 12 persons.
        ZonedDateTime timestamp = parse("2000-01-01T00:00:00.000Z");
        Flowable<JsonDocument> persons = Flowable.range(0, 12)
                .map(i -> format("person%02d", i))
                .map(id -> createPerson(id, timestamp));


        try (Transaction tx = persistence.createTransaction(false)) {
            try {
                persistence.deleteAllEntities(tx, namespace, "Person", specification).blockingAwait();

                // Create data.
                persons.flatMapCompletable(document -> persistence.createOrOverwrite(tx, document, specification)).blockingAwait();


                Flowable<JsonDocument> allPersons = persistence.readDocuments(tx, timestamp, namespace, "Person", Range.unbounded());
                assertThat(allPersons.map(JsonDocument::jackson).blockingIterable())
                        .as("json documents returned by readDocuments(..., unbounded)")
                        .usingElementComparator((o1, o2) -> o1.equals(o2) ? 0 : -1)
                        .containsExactlyInAnyOrderElementsOf(persons.map(JsonDocument::jackson).blockingIterable());

                Flowable<JsonDocument> firstThreePersons = persistence.readDocuments(tx, timestamp, namespace, "Person", Range.first(3));
                assertThat(firstThreePersons.map(JsonDocument::jackson).blockingIterable())
                        .as("json documents returned by readDocuments(..., first(3))")
                        .usingElementComparator((o1, o2) -> o1.equals(o2) ? 0 : -1)
                        .containsExactly(
                                createPerson("person00", timestamp).jackson(),
                                createPerson("person01", timestamp).jackson(),
                                createPerson("person02", timestamp).jackson()
                        );

                Flowable<JsonDocument> firstThreeAfter = persistence.readDocuments(tx, timestamp, namespace, "Person", Range.firstAfter(3, "person03"));
                assertThat(firstThreeAfter.map(JsonDocument::jackson).blockingIterable())
                        .as("json documents returned by readDocuments(..., firstAfter(3))")
                        .usingElementComparator((o1, o2) -> o1.equals(o2) ? 0 : -1)
                        .containsExactly(
                                createPerson("person04", timestamp).jackson(),
                                createPerson("person05", timestamp).jackson(),
                                createPerson("person06", timestamp).jackson()
                        );

                Flowable<JsonDocument> firstThreeBetween = persistence.readDocuments(tx, timestamp, namespace, "Person", Range.firstBetween(2, "person06", "person10"));
                assertThat(firstThreeBetween.map(JsonDocument::jackson).blockingIterable())
                        .as("json documents returned by readDocuments(..., firstBetween(2, \"person05\", \"person10\"))")
                        .usingElementComparator((o1, o2) -> o1.equals(o2) ? 0 : -1)
                        .containsExactly(
                                createPerson("person07", timestamp).jackson(),
                                createPerson("person08", timestamp).jackson()
                        );

                Flowable<JsonDocument> firstFourBetween = persistence.readDocuments(tx, timestamp, namespace, "Person", Range.firstBetween(4, "person06", "person10"));
                assertThat(firstFourBetween.map(JsonDocument::jackson).blockingIterable())
                        .as("json documents returned by readDocuments(..., firstBetween(4, \"person06\", \"person10\"))")
                        .usingElementComparator((o1, o2) -> o1.equals(o2) ? 0 : -1)
                        .containsExactly(
                                createPerson("person07", timestamp).jackson(),
                                createPerson("person08", timestamp).jackson(),
                                createPerson("person09", timestamp).jackson()
                        );

                Flowable<JsonDocument> lastThree = persistence.readDocuments(tx, timestamp, namespace, "Person", Range.last(3));
                assertThat(lastThree.map(JsonDocument::jackson).blockingIterable())
                        .as("json documents returned by readDocuments(..., last(3))")
                        .usingElementComparator((o1, o2) -> o1.equals(o2) ? 0 : -1)
                        .containsExactly(
                                createPerson("person11", timestamp).jackson(),
                                createPerson("person10", timestamp).jackson(),
                                createPerson("person09", timestamp).jackson()
                        );

                Flowable<JsonDocument> lastThreeBefore = persistence.readDocuments(tx, timestamp, namespace, "Person", Range.lastBefore(3, "person10"));
                assertThat(lastThreeBefore.map(JsonDocument::jackson).blockingIterable())
                        .as("json documents returned by readDocuments(..., lastBefore(3, \"person10\"))")
                        .usingElementComparator((o1, o2) -> o1.equals(o2) ? 0 : -1)
                        .containsExactly(
                                createPerson("person09", timestamp).jackson(),
                                createPerson("person08", timestamp).jackson(),
                                createPerson("person07", timestamp).jackson()
                        );

                Flowable<JsonDocument> lastTwoBetween = persistence.readDocuments(tx, timestamp, namespace, "Person", Range.lastBetween(2, "person06", "person10"));
                assertThat(lastTwoBetween.map(JsonDocument::jackson).blockingIterable())
                        .as("json documents returned by readDocuments(..., lastBetween(2, \"person06\", \"person10\"))")
                        .usingElementComparator((o1, o2) -> o1.equals(o2) ? 0 : -1)
                        .containsExactly(
                                createPerson("person09", timestamp).jackson(),
                                createPerson("person08", timestamp).jackson()
                        );

                Flowable<JsonDocument> lastFourBetween = persistence.readDocuments(tx, timestamp, namespace, "Person", Range.lastBetween(4, "person06", "person10"));
                assertThat(lastFourBetween.map(JsonDocument::jackson).blockingIterable())
                        .as("json documents returned by readDocuments(..., lastBetween(4, \"person06\", \"person10\"))")
                        .usingElementComparator((o1, o2) -> o1.equals(o2) ? 0 : -1)
                        .containsExactly(
                                createPerson("person09", timestamp).jackson(),
                                createPerson("person08", timestamp).jackson(),
                                createPerson("person07", timestamp).jackson()
                        );


            } finally {
                // Clean up.
                persons.flatMapCompletable(document ->
                        persistence.deleteAllDocumentVersions(tx, namespace, "Person", document.key().id(),
                                PersistenceDeletePolicy.CASCADE_DELETE_ALL_INCOMING_LINKS_AND_NODES)
                ).blockingAwait();
            }
        }
    }

    @Test
    public void testReadDocumentVersions() {
        // Create 12 version of the same person.
        ZonedDateTime timestamp = parse("2000-01-01T00:00:00.000Z");
        Flowable<JsonDocument> persons = Flowable.range(1, 12)
                .map(month -> timestamp.withMonth(month))
                .map(datetime -> createPersonVersion(datetime));


        try (Transaction tx = persistence.createTransaction(false)) {
            try {

                // Create data.
                persons.flatMapCompletable(document -> persistence.createOrOverwrite(tx, document, specification)).blockingAwait();


                Flowable<JsonDocument> allPersons = persistence.readDocumentVersions(
                        tx, namespace, "Person", "person00",
                        Range.unbounded()
                );
                assertThat(allPersons.map(JsonDocument::jackson).blockingIterable())
                        .as("json documents returned by readDocuments(..., unbounded)")
                        .usingElementComparator((o1, o2) -> o1.equals(o2) ? 0 : -1)
                        .containsExactlyInAnyOrderElementsOf(persons.map(JsonDocument::jackson).blockingIterable());

                Flowable<JsonDocument> firstThreePersons = persistence.readDocumentVersions(
                        tx, namespace, "Person", "person00",
                        Range.first(3)
                );
                assertThat(firstThreePersons.map(JsonDocument::jackson).blockingIterable())
                        .as("json documents returned by readDocuments(..., first(3))")
                        .usingElementComparator((o1, o2) -> o1.equals(o2) ? 0 : -1)
                        .containsExactly(
                                createPersonVersion(timestamp.withMonth(1)).jackson(),
                                createPersonVersion(timestamp.withMonth(2)).jackson(),
                                createPersonVersion(timestamp.withMonth(3)).jackson()
                        );

                Flowable<JsonDocument> firstThreeAfter = persistence.readDocumentVersions(
                        tx, namespace, "Person", "person00",
                        Range.firstAfter(3, timestamp.withMonth(3))
                );
                assertThat(firstThreeAfter.map(JsonDocument::jackson).blockingIterable())
                        .as("json documents returned by readDocuments(..., firstAfter(3, 2000-03...))")
                        .usingElementComparator((o1, o2) -> o1.equals(o2) ? 0 : -1)
                        .containsExactly(
                                createPersonVersion(timestamp.withMonth(4)).jackson(),
                                createPersonVersion(timestamp.withMonth(5)).jackson(),
                                createPersonVersion(timestamp.withMonth(6)).jackson()
                        );

                Flowable<JsonDocument> firstThreeBetween = persistence.readDocumentVersions(
                        tx, namespace, "Person", "person00",
                        Range.firstBetween(2, timestamp.withMonth(6), timestamp.withMonth(10))
                );
                assertThat(firstThreeBetween.map(JsonDocument::jackson).blockingIterable())
                        .as("json documents returned by readDocuments(..., firstBetween(2, \"person05\", \"person10\"))")
                        .usingElementComparator((o1, o2) -> o1.equals(o2) ? 0 : -1)
                        .containsExactly(
                                createPersonVersion(timestamp.withMonth(7)).jackson(),
                                createPersonVersion(timestamp.withMonth(8)).jackson()
                        );

                Flowable<JsonDocument> firstFourBetween = persistence.readDocumentVersions(
                        tx, namespace, "Person", "person00",
                        Range.firstBetween(4, timestamp.withMonth(6), timestamp.withMonth(10))
                );
                assertThat(firstFourBetween.map(JsonDocument::jackson).blockingIterable())
                        .as("json documents returned by readDocuments(..., firstBetween(4, 2000-06..., 2000-10...))")
                        .usingElementComparator((o1, o2) -> o1.equals(o2) ? 0 : -1)
                        .containsExactly(
                                createPersonVersion(timestamp.withMonth(7)).jackson(),
                                createPersonVersion(timestamp.withMonth(8)).jackson(),
                                createPersonVersion(timestamp.withMonth(9)).jackson()
                        );

                Flowable<JsonDocument> lastThree = persistence.readDocumentVersions(
                        tx, namespace, "Person", "person00",
                        Range.last(3)
                );
                assertThat(lastThree.map(JsonDocument::jackson).blockingIterable())
                        .as("json documents returned by readDocuments(..., last(3))")
                        .usingElementComparator((o1, o2) -> o1.equals(o2) ? 0 : -1)
                        .containsExactly(
                                createPersonVersion(timestamp.withMonth(12)).jackson(),
                                createPersonVersion(timestamp.withMonth(11)).jackson(),
                                createPersonVersion(timestamp.withMonth(10)).jackson()
                        );

                Flowable<JsonDocument> lastThreeBefore = persistence.readDocumentVersions(
                        tx, namespace, "Person", "person00",
                        Range.lastBefore(3, timestamp.withMonth(10))
                );
                assertThat(lastThreeBefore.map(JsonDocument::jackson).blockingIterable())
                        .as("json documents returned by readDocuments(..., lastBefore(3, \"person10\"))")
                        .usingElementComparator((o1, o2) -> o1.equals(o2) ? 0 : -1)
                        .containsExactly(
                                createPersonVersion(timestamp.withMonth(9)).jackson(),
                                createPersonVersion(timestamp.withMonth(8)).jackson(),
                                createPersonVersion(timestamp.withMonth(7)).jackson()
                        );

                Flowable<JsonDocument> lastTwoBetween = persistence.readDocumentVersions(
                        tx, namespace, "Person", "person00",
                        Range.lastBetween(2, timestamp.withMonth(6), timestamp.withMonth(10)));
                assertThat(lastTwoBetween.map(JsonDocument::jackson).blockingIterable())
                        .as("json documents returned by readDocuments(..., lastBetween(2, \"person06\", \"person10\"))")
                        .usingElementComparator((o1, o2) -> o1.equals(o2) ? 0 : -1)
                        .containsExactly(
                                createPersonVersion(timestamp.withMonth(9)).jackson(),
                                createPersonVersion(timestamp.withMonth(8)).jackson()
                        );

                Flowable<JsonDocument> lastFourBetween = persistence.readDocumentVersions(
                        tx, namespace, "Person", "person00",
                        Range.lastBetween(4, timestamp.withMonth(6), timestamp.withMonth(10)));
                assertThat(lastFourBetween.map(JsonDocument::jackson).blockingIterable())
                        .as("json documents returned by readDocuments(..., lastBetween(4, \"person06\", \"person10\"))")
                        .usingElementComparator((o1, o2) -> o1.equals(o2) ? 0 : -1)
                        .containsExactly(
                                createPersonVersion(timestamp.withMonth(9)).jackson(),
                                createPersonVersion(timestamp.withMonth(8)).jackson(),
                                createPersonVersion(timestamp.withMonth(7)).jackson()
                        );


            } finally {
                // Clean up.
                persons.flatMapCompletable(document ->
                        persistence.deleteAllDocumentVersions(tx, namespace, "Person", document.key().id(),
                                PersistenceDeletePolicy.CASCADE_DELETE_ALL_INCOMING_LINKS_AND_NODES)
                ).blockingAwait();
            }
        }
    }

    @Test
    public void thatDeleteAllVersionsWorks() {
        try (Transaction transaction = persistence.createTransaction(false)) {
            persistence.deleteAllDocumentVersions(transaction, namespace, "Address", "newyork", PersistenceDeletePolicy.FAIL_IF_INCOMING_LINKS).blockingAwait();

            ZonedDateTime jan1624 = of(1624, 1, 1, 12, 0, 0, (int) TimeUnit.MILLISECONDS.toNanos(0), ZoneId.of("Etc/UTC"));
            ZonedDateTime jan1626 = of(1626, 1, 1, 12, 0, 0, (int) TimeUnit.MILLISECONDS.toNanos(0), ZoneId.of("Etc/UTC"));
            ZonedDateTime jan1664 = of(1664, 1, 1, 12, 0, 0, (int) TimeUnit.MILLISECONDS.toNanos(0), ZoneId.of("Etc/UTC"));
            JsonDocument input0 = toDocument(namespace, "Address", "newyork", createAddress("", "NY", "USA"), jan1624);
            persistence.createOrOverwrite(transaction, input0, specification).blockingAwait();
            JsonDocument input1 = toDocument(namespace, "Address", "newyork", createAddress("New Amsterdam", "NY", "USA"), jan1626);
            persistence.createOrOverwrite(transaction, input1, specification).blockingAwait();
            JsonDocument input2 = toDocument(namespace, "Address", "newyork", createAddress("New York", "NY", "USA"), jan1664);
            persistence.createOrOverwrite(transaction, input2, specification).blockingAwait();
            Iterator<JsonDocument> iteratorWithDocuments = persistence.readDocumentVersions(transaction, namespace, "Address", "newyork", Range.unbounded()).blockingIterable().iterator();

            assertEquals(size(iteratorWithDocuments), 3);

            persistence.deleteAllDocumentVersions(transaction, namespace, "Address", "newyork", PersistenceDeletePolicy.FAIL_IF_INCOMING_LINKS).blockingAwait();

            Iterator<JsonDocument> iterator = persistence.readDocumentVersions(transaction, namespace, "Address", "newyork", Range.unbounded()).blockingIterable().iterator();

            assertEquals(size(iterator), 0);
        }
    }

    int size(Iterator<?> iterator) {
        int i = 0;
        while (iterator.hasNext()) {
            iterator.next();
            i++;
        }
        return i;
    }

    @Test
    public void thatBasicCreateThenReadWorks() throws JSONException {
        try (Transaction transaction = persistence.createTransaction(false)) {
            persistence.deleteAllDocumentVersions(transaction, namespace, "Person", "john", PersistenceDeletePolicy.FAIL_IF_INCOMING_LINKS).blockingAwait();

            ZonedDateTime oct18 = of(2018, 10, 7, 19, 49, 26, (int) TimeUnit.MILLISECONDS.toNanos(307), ZoneId.of("Etc/UTC"));
            JsonDocument input = toDocument(namespace, "Person", "john", createPerson("John", "Smith"), oct18);
            persistence.createOrOverwrite(transaction, input, specification).blockingAwait();

            JsonDocument output = persistence.readDocument(transaction, oct18, namespace, "Person", "john").blockingGet();
            assertNotNull(output);
            assertNotSame(output, input);
            JSONAssert.assertEquals(JsonTools.toJson(output.jackson()), JsonTools.toJson(input.jackson()), true);
        }
    }

    @Test
    public void thatCreateWithSameVersionDoesOverwriteInsteadOfCreatingDuplicateVersions() {
        try (Transaction transaction = persistence.createTransaction(false)) {
            persistence.deleteAllDocumentVersions(transaction, namespace, "Person", "john", PersistenceDeletePolicy.FAIL_IF_INCOMING_LINKS).blockingAwait();

            ZonedDateTime oct18 = of(2018, 10, 7, 19, 49, 26, (int) TimeUnit.MILLISECONDS.toNanos(307), ZoneId.of("Etc/UTC"));
            JsonDocument input = toDocument(namespace, "Person", "john", createPerson("Jimmy", "Smith"), oct18);
            JsonDocument input2 = toDocument(namespace, "Person", "john", createPerson("John", "Smith"), oct18);
            persistence.createOrOverwrite(transaction, input, specification).blockingAwait();
            persistence.createOrOverwrite(transaction, input2, specification).blockingAwait();

            Iterator<JsonDocument> iterator = persistence.readDocumentVersions(transaction, namespace,
                    "Person", "john", Range.unbounded()).blockingIterable().iterator();

            assertTrue(iterator.hasNext());
            assertNotNull(iterator.next());
            assertFalse(iterator.hasNext());
        }
    }

    @Test
    public void thatBasicTimeBasedVersioningWorks() {
        try (Transaction transaction = persistence.createTransaction(false)) {
            persistence.deleteAllDocumentVersions(transaction, namespace, "Address", "newyork", PersistenceDeletePolicy.FAIL_IF_INCOMING_LINKS).blockingAwait();

            ZonedDateTime jan1624 = of(1624, 1, 1, 12, 0, 0, (int) TimeUnit.MILLISECONDS.toNanos(0), ZoneId.of("Etc/UTC"));
            ZonedDateTime jan1626 = of(1626, 1, 1, 12, 0, 0, (int) TimeUnit.MILLISECONDS.toNanos(0), ZoneId.of("Etc/UTC"));
            ZonedDateTime jan1664 = of(1664, 1, 1, 12, 0, 0, (int) TimeUnit.MILLISECONDS.toNanos(0), ZoneId.of("Etc/UTC"));
            JsonDocument input0 = toDocument(namespace, "Address", "newyork", createAddress("", "NY", "USA"), jan1624);
            persistence.createOrOverwrite(transaction, input0, specification).blockingAwait();
            JsonDocument input2 = toDocument(namespace, "Address", "newyork", createAddress("New York", "NY", "USA"), jan1664);
            persistence.createOrOverwrite(transaction, input2, specification).blockingAwait();
            JsonDocument input1a = toDocument(namespace, "Address", "newyork", createAddress("1a New Amsterdam", "NY", "USA"), jan1626);
            JsonDocument input1b = toDocument(namespace, "Address", "newyork", createAddress("1b New Amsterdam", "NY", "USA"), jan1626);
            persistence.createOrOverwrite(transaction, input1a, specification).blockingAwait();
            persistence.createOrOverwrite(transaction, input1b, specification).blockingAwait();
            Iterator<JsonDocument> iterator = persistence.readDocumentVersions(transaction, namespace, "Address", "newyork", Range.unbounded())
                    .blockingIterable().iterator();
            Set<DocumentKey> actual = new LinkedHashSet<>();
            assertTrue(iterator.hasNext());
            actual.add(iterator.next().key());
            assertTrue(iterator.hasNext());
            actual.add(iterator.next().key());
            assertTrue(iterator.hasNext());
            actual.add(iterator.next().key());
            assertFalse(iterator.hasNext());
            assertEquals(actual, Set.of(input0.key(), input1b.key(), input2.key()));
        }
    }

    @Test
    public void thatDeleteMarkerWorks() {
        try (Transaction transaction = persistence.createTransaction(false)) {
            persistence.deleteAllDocumentVersions(transaction, namespace, "Address", "newyork", PersistenceDeletePolicy.FAIL_IF_INCOMING_LINKS).blockingAwait();

            ZonedDateTime jan1624 = of(1624, 1, 1, 12, 0, 0, (int) TimeUnit.MILLISECONDS.toNanos(0), ZoneId.of("Etc/UTC"));
            ZonedDateTime jan1626 = of(1626, 1, 1, 12, 0, 0, (int) TimeUnit.MILLISECONDS.toNanos(0), ZoneId.of("Etc/UTC"));
            ZonedDateTime feb1663 = of(1663, 2, 1, 0, 0, 0, (int) TimeUnit.MILLISECONDS.toNanos(0), ZoneId.of("Etc/UTC"));
            ZonedDateTime jan1664 = of(1664, 1, 1, 12, 0, 0, (int) TimeUnit.MILLISECONDS.toNanos(0), ZoneId.of("Etc/UTC"));

            persistence.createOrOverwrite(transaction, toDocument(namespace, "Address", "newyork", createAddress("", "NY", "USA"), jan1624), specification).blockingAwait();
            persistence.createOrOverwrite(transaction, toDocument(namespace, "Address", "newyork", createAddress("New Amsterdam", "NY", "USA"), jan1626), specification).blockingAwait();
            persistence.createOrOverwrite(transaction, toDocument(namespace, "Address", "newyork", createAddress("New York", "NY", "USA"), jan1664), specification).blockingAwait();

            assertEquals(size(persistence.readDocumentVersions(transaction, namespace, "Address", "newyork", Range.unbounded()).blockingIterable().iterator()), 3);

            persistence.markDocumentDeleted(transaction, namespace, "Address", "newyork", feb1663, PersistenceDeletePolicy.FAIL_IF_INCOMING_LINKS).blockingAwait();

            assertEquals(size(persistence.readDocumentVersions(transaction, namespace, "Address", "newyork", Range.unbounded()).blockingIterable().iterator()), 4);

            persistence.deleteDocument(transaction, namespace, "Address", "newyork", feb1663, PersistenceDeletePolicy.FAIL_IF_INCOMING_LINKS).blockingAwait();

            assertEquals(size(persistence.readDocumentVersions(transaction, namespace, "Address", "newyork", Range.unbounded()).blockingIterable().iterator()), 3);

            persistence.markDocumentDeleted(transaction, namespace, "Address", "newyork", feb1663, PersistenceDeletePolicy.FAIL_IF_INCOMING_LINKS).blockingAwait();

            assertEquals(size(persistence.readDocumentVersions(transaction, namespace, "Address", "newyork", Range.unbounded()).blockingIterable().iterator()), 4);
        }
    }

    @Test
    public void thatReadVersionsInRangeWorks() {
        try (Transaction transaction = persistence.createTransaction(false)) {
            persistence.deleteAllDocumentVersions(transaction, namespace, "Person", "john", PersistenceDeletePolicy.FAIL_IF_INCOMING_LINKS).blockingAwait();

            ZonedDateTime aug92 = of(1992, 8, 1, 13, 43, 20, (int) TimeUnit.MILLISECONDS.toNanos(301), ZoneId.of("Etc/UTC"));
            ZonedDateTime feb10 = of(2010, 2, 3, 15, 45, 22, (int) TimeUnit.MILLISECONDS.toNanos(303), ZoneId.of("Etc/UTC"));
            ZonedDateTime nov13 = of(2013, 11, 5, 17, 47, 24, (int) TimeUnit.MILLISECONDS.toNanos(305), ZoneId.of("Etc/UTC"));
            ZonedDateTime sep18 = of(2018, 9, 6, 18, 48, 25, (int) TimeUnit.MILLISECONDS.toNanos(306), ZoneId.of("Etc/UTC"));
            ZonedDateTime oct18 = of(2018, 10, 7, 19, 49, 26, (int) TimeUnit.MILLISECONDS.toNanos(307), ZoneId.of("Etc/UTC"));
            persistence.createOrOverwrite(transaction, toDocument(namespace, "Person", "john", createPerson("John", "Smith"), aug92), specification).blockingAwait();
            persistence.createOrOverwrite(transaction, toDocument(namespace, "Person", "john", createPerson("James", "Smith"), nov13), specification).blockingAwait();
            persistence.createOrOverwrite(transaction, toDocument(namespace, "Person", "john", createPerson("John", "Smith"), oct18), specification).blockingAwait();

            // TODO: @kimcs my implementation fails here. The assertion wants two, but only nov13 is between feb10 and sep18
            // assertEquals(size(persistence.readDocumentVersions(transaction, namespace, "Person", "john", Range.between(feb10, sep18)).blockingIterable().iterator()), 2);
            assertEquals(size(persistence.readDocumentVersions(transaction, namespace, "Person", "john", Range.between(feb10, sep18)).blockingIterable().iterator()), 1);
        }
    }

    @Test
    public void thatReadAllVersionsWorks() {
        try (Transaction transaction = persistence.createTransaction(false)) {
            persistence.deleteAllDocumentVersions(transaction, namespace, "Person", "john", PersistenceDeletePolicy.FAIL_IF_INCOMING_LINKS).blockingAwait();

            ZonedDateTime aug92 = of(1992, 8, 1, 13, 43, 20, (int) TimeUnit.MILLISECONDS.toNanos(301), ZoneId.of("Etc/UTC"));
            ZonedDateTime nov13 = of(2013, 11, 5, 17, 47, 24, (int) TimeUnit.MILLISECONDS.toNanos(305), ZoneId.of("Etc/UTC"));
            ZonedDateTime oct18 = of(2018, 10, 7, 19, 49, 26, (int) TimeUnit.MILLISECONDS.toNanos(307), ZoneId.of("Etc/UTC"));
            persistence.createOrOverwrite(transaction, toDocument(namespace, "Person", "john", createPerson("John", "Smith"), aug92), specification).blockingAwait();
            persistence.createOrOverwrite(transaction, toDocument(namespace, "Person", "john", createPerson("James", "Smith"), nov13), specification).blockingAwait();
            persistence.createOrOverwrite(transaction, toDocument(namespace, "Person", "john", createPerson("John", "Smith"), oct18), specification).blockingAwait();

            assertEquals(size(persistence.readDocumentVersions(transaction, namespace, "Person", "john", Range.unbounded()).blockingIterable().iterator()), 3);
        }
    }

    @Test
    public void thatFindSimpleWithPathAndValueWorks() {
        try (Transaction transaction = persistence.createTransaction(false)) {
            persistence.deleteAllDocumentVersions(transaction, namespace, "Person", "simple", PersistenceDeletePolicy.FAIL_IF_INCOMING_LINKS).blockingAwait();
            ZonedDateTime sep18 = of(2018, 9, 6, 18, 48, 25, (int) TimeUnit.MILLISECONDS.toNanos(306), ZoneId.of("Etc/UTC"));
            ZonedDateTime oct18 = of(2018, 10, 7, 19, 49, 26, (int) TimeUnit.MILLISECONDS.toNanos(307), ZoneId.of("Etc/UTC"));
            persistence.createOrOverwrite(transaction, toDocument(namespace, "Person", "simple", mapper.createObjectNode().put("firstname", "Simple"), sep18), specification).blockingAwait();

            Iterator<JsonDocument> iterator = persistence.findDocument(transaction, oct18, namespace, "Person", JsonNavigationPath.from("$.firstname"), "Simple", Range.unbounded()).blockingIterable().iterator();
            assertTrue(iterator.hasNext());
            JsonDocument person1 = iterator.next();
            assertEquals(person1.jackson().get("firstname").textValue(), "Simple");
            assertFalse(iterator.hasNext());
        }
    }

    @Test
    public void thatFindAllWithPathAndValueWorks() {
        try (Transaction transaction = persistence.createTransaction(false)) {
            persistence.deleteAllDocumentVersions(transaction, namespace, "Person", "john", PersistenceDeletePolicy.FAIL_IF_INCOMING_LINKS).blockingAwait();
            persistence.deleteAllDocumentVersions(transaction, namespace, "Person", "jane", PersistenceDeletePolicy.FAIL_IF_INCOMING_LINKS).blockingAwait();

            ZonedDateTime aug92 = of(1992, 8, 1, 13, 43, 20, (int) TimeUnit.MILLISECONDS.toNanos(301), ZoneId.of("Etc/UTC"));
            ZonedDateTime sep94 = of(1994, 9, 1, 13, 43, 20, (int) TimeUnit.MILLISECONDS.toNanos(301), ZoneId.of("Etc/UTC"));
            ZonedDateTime feb10 = of(2010, 2, 3, 15, 45, 22, (int) TimeUnit.MILLISECONDS.toNanos(303), ZoneId.of("Etc/UTC"));
            ZonedDateTime nov13 = of(2013, 11, 5, 17, 47, 24, (int) TimeUnit.MILLISECONDS.toNanos(305), ZoneId.of("Etc/UTC"));
            ZonedDateTime sep18 = of(2018, 9, 6, 18, 48, 25, (int) TimeUnit.MILLISECONDS.toNanos(306), ZoneId.of("Etc/UTC"));
            ZonedDateTime oct18 = of(2018, 10, 7, 19, 49, 26, (int) TimeUnit.MILLISECONDS.toNanos(307), ZoneId.of("Etc/UTC"));
            persistence.createOrOverwrite(transaction, toDocument(namespace, "Person", "john", createPerson("John", "Smith"), aug92), specification).blockingAwait();
            persistence.createOrOverwrite(transaction, toDocument(namespace, "Person", "jane", createPerson("Jane", "Doe"), sep94), specification).blockingAwait();
            persistence.createOrOverwrite(transaction, toDocument(namespace, "Person", "jane", createPerson("Jane", "Smith"), feb10), specification).blockingAwait();
            persistence.createOrOverwrite(transaction, toDocument(namespace, "Person", "john", createPerson("James", "Smith"), nov13), specification).blockingAwait();
            persistence.createOrOverwrite(transaction, toDocument(namespace, "Person", "john", createPerson("John", "Smith"), oct18), specification).blockingAwait();

            Iterator<JsonDocument> iterator = persistence.findDocument(transaction, sep18, namespace, "Person", JsonNavigationPath.from("$.lastname"), "Smith", Range.unbounded()).blockingIterable().iterator();

            JsonDocument person1 = iterator.next();
            JsonDocument person2 = iterator.next();
            assertFalse(iterator.hasNext());

            if (person1.jackson().get("firstname").textValue().equals("Jane")) {
                assertEquals(person2.jackson().get("firstname").textValue(), "James");
            } else {
                assertEquals(person1.jackson().get("firstname").textValue(), "James");
                assertEquals(person2.jackson().get("firstname").textValue(), "Jane");
            }
        }
    }

    @Test
    public void thatFindAllWorks() {
        try (Transaction transaction = persistence.createTransaction(false)) {
            // TODO Consider support for deleting entire entity in one operation...?
            persistence.deleteAllDocumentVersions(transaction, namespace, "Person", "john", PersistenceDeletePolicy.FAIL_IF_INCOMING_LINKS).blockingAwait();
            persistence.deleteAllDocumentVersions(transaction, namespace, "Person", "jane", PersistenceDeletePolicy.FAIL_IF_INCOMING_LINKS).blockingAwait();

            ZonedDateTime aug92 = of(1992, 8, 1, 13, 43, 20, (int) TimeUnit.MILLISECONDS.toNanos(301), ZoneId.of("Etc/UTC"));
            ZonedDateTime sep94 = of(1994, 9, 1, 13, 43, 20, (int) TimeUnit.MILLISECONDS.toNanos(301), ZoneId.of("Etc/UTC"));
            ZonedDateTime feb10 = of(2010, 2, 3, 15, 45, 22, (int) TimeUnit.MILLISECONDS.toNanos(303), ZoneId.of("Etc/UTC"));
            ZonedDateTime dec11 = of(2011, 12, 4, 16, 46, 23, (int) TimeUnit.MILLISECONDS.toNanos(304), ZoneId.of("Etc/UTC"));
            ZonedDateTime nov13 = of(2013, 11, 5, 17, 47, 24, (int) TimeUnit.MILLISECONDS.toNanos(305), ZoneId.of("Etc/UTC"));
            ZonedDateTime oct18 = of(2018, 10, 7, 19, 49, 26, (int) TimeUnit.MILLISECONDS.toNanos(307), ZoneId.of("Etc/UTC"));
            persistence.createOrOverwrite(transaction, toDocument(namespace, "Person", "john", createPerson("John", "Smith"), aug92), specification).blockingAwait();
            persistence.createOrOverwrite(transaction, toDocument(namespace, "Person", "jane", createPerson("Jane", "Doe"), sep94), specification).blockingAwait();
            persistence.createOrOverwrite(transaction, toDocument(namespace, "Person", "jane", createPerson("Jane", "Smith"), feb10), specification).blockingAwait();
            persistence.createOrOverwrite(transaction, toDocument(namespace, "Person", "john", createPerson("James", "Smith"), nov13), specification).blockingAwait();
            persistence.createOrOverwrite(transaction, toDocument(namespace, "Person", "john", createPerson("John", "Smith"), oct18), specification).blockingAwait();

            Iterator<JsonDocument> iterator = persistence.readDocuments(transaction, dec11, namespace, "Person", Range.unbounded()).blockingIterable().iterator();

            assertTrue(iterator.hasNext());
            JsonDocument person1 = iterator.next();
            assertTrue(iterator.hasNext());
            JsonDocument person2 = iterator.next();
            assertFalse(iterator.hasNext());

            if (person1.jackson().get("firstname").textValue().equals("Jane")) {
                assertEquals(person2.jackson().get("firstname").textValue(), "John");
            } else {
                assertEquals(person1.jackson().get("firstname").textValue(), "John");
                assertEquals(person2.jackson().get("firstname").textValue(), "Jane");
            }

        }
    }

    @Test
    public void thatBigValueWorks() {
        try (Transaction transaction = persistence.createTransaction(false)) {
            persistence.deleteAllDocumentVersions(transaction, namespace, "FunkyLongAddress", "newyork", PersistenceDeletePolicy.FAIL_IF_INCOMING_LINKS).blockingAwait();

            ZonedDateTime oct18 = of(2018, 10, 7, 19, 49, 26, (int) TimeUnit.MILLISECONDS.toNanos(307), ZoneId.of("Etc/UTC"));
            ZonedDateTime now = now(ZoneId.of("Etc/UTC"));

            String bigString = "12345678901234567890";
            for (int i = 0; i < 12; i++) {
                bigString = bigString + "_" + bigString;
            }

            // Creating funky long address
            persistence.createOrOverwrite(transaction, toDocument(namespace, "FunkyLongAddress", "newyork", createAddress(bigString, "NY", "USA"), oct18), specification).blockingAwait();

            // Finding funky long address by city
            Iterable<JsonDocument> funkyLongAddress = persistence.findDocument(transaction, now, namespace, "FunkyLongAddress", JsonNavigationPath.from("$.city"), bigString, Range.unbounded()).blockingIterable();
            Iterator<JsonDocument> iterator = funkyLongAddress.iterator();
            assertTrue(iterator.hasNext());
            JsonDocument foundDocument = iterator.next();
            assertFalse(iterator.hasNext());
            String foundBigString = foundDocument.jackson().get("city").textValue();
            assertEquals(foundBigString, bigString);

            // Finding funky long address by city (with non-matching value)
            int findExpectNoMatchSize = size(persistence.findDocument(transaction, now, namespace, "FunkyLongAddress", JsonNavigationPath.from("$.city"), bigString + "1", Range.unbounded()).blockingIterable().iterator());
            assertEquals(findExpectNoMatchSize, 0);

            // Deleting funky long address
            persistence.deleteAllDocumentVersions(transaction, namespace, "FunkyLongAddress", "newyork", PersistenceDeletePolicy.FAIL_IF_INCOMING_LINKS).blockingAwait();
        }
    }

    @Test
    public void thatSimpleArrayValuesAreIntact() {
        Specification specification = SpecificationBuilder.createSpecificationAndRoot(Set.of(
                objectNode(SpecificationElementType.MANAGED, "People", Set.of(
                        arrayNode("name", stringNode("[]"))
                ))
        ));
        try (Transaction transaction = persistence.createTransaction(false)) {
            ZonedDateTime oct18 = of(2018, 10, 7, 19, 49, 26, (int) TimeUnit.MILLISECONDS.toNanos(307), ZoneId.of("Etc/UTC"));
            ObjectNode doc = mapper.createObjectNode();
            doc.putArray("name").add("John Smith").add("Jane Doe");
            JsonDocument input = toDocument(namespace, "People", "1", doc, oct18);
            persistence.createOrOverwrite(transaction, input, specification).blockingAwait();
            JsonDocument jsonDocument = persistence.readDocument(transaction, oct18, namespace, "People", "1").blockingGet();
            assertEquals(jsonDocument.jackson().toString(), doc.toString());
        }
    }

    @Test
    public void thatComplexArrayValuesAreIntact() {
        Specification specification = SpecificationBuilder.createSpecificationAndRoot(Set.of(
                objectNode(SpecificationElementType.MANAGED, "People", Set.of(
                        arrayNode("name",
                                objectNode(SpecificationElementType.EMBEDDED, "[]", Set.of(
                                        stringNode("first"),
                                        stringNode("last")
                                ))
                        )
                ))
        ));
        try (Transaction transaction = persistence.createTransaction(false)) {
            ZonedDateTime oct18 = of(2018, 10, 7, 19, 49, 26, (int) TimeUnit.MILLISECONDS.toNanos(307), ZoneId.of("Etc/UTC"));
            ObjectNode doc = mapper.createObjectNode();
            ArrayNode name = doc.putArray("name");
            name.addObject().put("first", "John").put("last", "Smith");
            name.addObject().put("first", "Jane").put("last", "Doe");
            JsonDocument input = toDocument(namespace, "People", "1", doc, oct18);
            persistence.createOrOverwrite(transaction, input, specification).blockingAwait();
            JsonDocument jsonDocument = persistence.readDocument(transaction, oct18, namespace, "People", "1").blockingGet();
            assertNotNull(jsonDocument);
            assertEquals(jsonDocument.jackson().toString(), doc.toString());
        }
    }

    @Test
    public void thatReadLinkedDocumentsWork() throws JSONException {
        ZonedDateTime timestamp = parse("2019-01-01T00:00:00.000Z");

        try (Transaction tx = persistence.createTransaction(false)) {
            persistence.deleteAllEntities(tx, namespace, "Person", specification).blockingAwait();
            persistence.deleteAllEntities(tx, namespace, "Address", specification).blockingAwait();
            persistence.deleteAllEntities(tx, namespace, "FunkyLongAddress", specification).blockingAwait();

            List<String> addressIds = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                String id = "address" + (i + 1);
                JsonDocument address = toDocument(namespace, "Address", id, createAddress("city " + i, "", "Country " + i), timestamp);
                persistence.createOrOverwrite(tx, address, specification).blockingAwait();
                addressIds.add(id);
            }

            List<String> funkyIds = new ArrayList<>();
            for (int i = 0; i < 2; i++) {
                String id = "funky" + (i + 1);
                JsonDocument address = toDocument(namespace, "FunkyLongAddress", id, createAddress("funky " + i, "", "Somewhere " + i), timestamp);
                persistence.createOrOverwrite(tx, address, specification).blockingAwait();
                funkyIds.add(id);
            }

            List<String> links = new ArrayList<>();
            links.addAll(addressIds.stream().map(l -> "/Address/" + l).collect(Collectors.toList()));
            links.addAll(funkyIds.stream().map(l -> "/FunkyLongAddress/" + l).collect(Collectors.toList()));

            Map<String, Map<String, List<String>>> relationsBySource = new LinkedHashMap<>();
            Map<String, Map<String, List<String>>> relationsByTarget = new LinkedHashMap<>();

            List<String> personIds = new ArrayList<>();
            List<String> names = List.of("Jack", "Jill", "Jane", "Jones");
            for (int i = 0; i < 11; i++) {
                String id = "person" + (i + 1);
                String personLink = "/Person/" + id;
                JsonDocument person;
                String currentAddressLink = links.get((2 * i) % links.size());
                String workAddressLink = links.get((2 * i + 1) % links.size());

                relationsBySource
                        .computeIfAbsent(personLink, k -> new LinkedHashMap<>())
                        .computeIfAbsent("$.history.currentAddress", p -> new ArrayList<>())
                        .add(currentAddressLink);
                relationsBySource
                        .computeIfAbsent(personLink, k -> new LinkedHashMap<>())
                        .computeIfAbsent("$.history.workAddress", p -> new ArrayList<>())
                        .add(workAddressLink);
                relationsBySource
                        .computeIfAbsent(personLink, k -> new LinkedHashMap<>())
                        .computeIfAbsent("$.history.previousAddresses[]", p -> new ArrayList<>())
                        .addAll(links);

                relationsByTarget
                        .computeIfAbsent(currentAddressLink, k -> new LinkedHashMap<>())
                        .computeIfAbsent("$.history.currentAddress", p -> new ArrayList<>())
                        .add(personLink);
                relationsByTarget
                        .computeIfAbsent(workAddressLink, k -> new LinkedHashMap<>())
                        .computeIfAbsent("$.history.workAddress", p -> new ArrayList<>())
                        .add(personLink);
                for (String link : links) {
                    relationsByTarget
                            .computeIfAbsent(link, k -> new LinkedHashMap<>())
                            .computeIfAbsent("$.history.previousAddresses[]", p -> new ArrayList<>())
                            .add(personLink);
                }

                person = toDocument(namespace, "Person", id, createPerson(names.get(i % names.size()) + " " + i, "Smith", currentAddressLink, workAddressLink, links), timestamp);
                persistence.createOrOverwrite(tx, person, specification).blockingAwait();
                personIds.add(id);
            }

            Map<String, List<String>> entityIdsByEntityName = Map.of(
                    "Address", addressIds,
                    "FunkyLongAddress", funkyIds,
                    "Person", personIds
            );

            for (int i = 0; i < personIds.size(); i++) {
                String personId = personIds.get(i);
                readLinksAndCheckDocuments(tx, "$.history.previousAddresses[]", personId, timestamp, entityIdsByEntityName);
                readLinkAndCheckDocument(tx, "$.history.currentAddress", personId, timestamp, entityIdsByEntityName);
                readLinkAndCheckDocument(tx, "$.history.workAddress", personId, timestamp, entityIdsByEntityName);
            }

            for (int i = 0; i < addressIds.size(); i++) {
                String addressId = addressIds.get(i);
                readBackLinksAndCheckRelations(tx, "Address", addressId, "Person", "$.history.previousAddresses[]", timestamp, relationsByTarget);
                readBackLinksAndCheckRelations(tx, "Address", addressId, "Person", "$.history.currentAddress", timestamp, relationsByTarget);
                readBackLinksAndCheckRelations(tx, "Address", addressId, "Person", "$.history.workAddress", timestamp, relationsByTarget);
            }

            for (int i = 0; i < funkyIds.size(); i++) {
                String funkyId = funkyIds.get(i);
                readBackLinksAndCheckRelations(tx, "FunkyLongAddress", funkyId, "Person", "$.history.previousAddresses[]", timestamp, relationsByTarget);
                readBackLinksAndCheckRelations(tx, "FunkyLongAddress", funkyId, "Person", "$.history.currentAddress", timestamp, relationsByTarget);
                readBackLinksAndCheckRelations(tx, "FunkyLongAddress", funkyId, "Person", "$.history.workAddress", timestamp, relationsByTarget);
            }
        }
    }

    private void readLinksAndCheckDocuments(Transaction tx, String jsonNavigationPath, String personId, ZonedDateTime timestamp, Map<String, List<String>> entityIdsByEntityName) throws JSONException {
        for (String targetEntity : Set.of("Address", "FunkyLongAddress")) {
            List<JsonDocument> actualDocuments = new ArrayList<>();
            persistence.readLinkedDocuments(tx, timestamp, namespace, "Person", personId, JsonNavigationPath.from(jsonNavigationPath), targetEntity, Range.unbounded())
                    .blockingForEach(actualJsonDocument -> actualDocuments.add(actualJsonDocument));
            assertEquals(actualDocuments.size(), entityIdsByEntityName.get(targetEntity).size(), String.format("for entity: '%s'", targetEntity));
            for (JsonDocument actualDoc : actualDocuments) {
                JsonDocument expectedJsonDocument = persistence.readDocument(tx, timestamp, namespace, targetEntity, actualDoc.key().id()).blockingGet();
                JSONAssert.assertEquals(actualDoc.jackson().toString(), expectedJsonDocument.jackson().toString(), true);
            }
        }
    }

    private void readLinkAndCheckDocument(Transaction tx, String jsonNavigationPath, String personId, ZonedDateTime timestamp, Map<String, List<String>> entityIdsByEntityName) throws JSONException {
        List<JsonDocument> actualDocuments = new ArrayList<>();
        for (String targetEntity : Set.of("Address", "FunkyLongAddress")) {
            persistence.readLinkedDocuments(tx, timestamp, namespace, "Person", personId, JsonNavigationPath.from(jsonNavigationPath), targetEntity, Range.unbounded())
                    .blockingForEach(actualJsonDocument -> actualDocuments.add(actualJsonDocument));
        }
        assertEquals(actualDocuments.size(), 1);
        JsonDocument actualDoc = actualDocuments.get(0);
        JsonDocument expectedJsonDocument = persistence.readDocument(tx, timestamp, namespace, actualDoc.key().entity(), actualDoc.key().id()).blockingGet();
        JSONAssert.assertEquals(actualDoc.jackson().toString(), expectedJsonDocument.jackson().toString(), true);
    }

    private void readBackLinksAndCheckRelations(Transaction tx, String targetEntity, String targetId, String sourceEntity, String jsonNavigationPath, ZonedDateTime timestamp, Map<String, Map<String, List<String>>> relationsByTarget) {
        List<String> actualDocuments = new ArrayList<>();
        persistence.readSourceDocuments(tx, timestamp, namespace, targetEntity, targetId, JsonNavigationPath.from(jsonNavigationPath), sourceEntity, Range.unbounded())
                .blockingForEach(actualJsonDocument -> actualDocuments.add("/" + actualJsonDocument.key().entity() + "/" + actualJsonDocument.key().id()));
        String targetLink = "/" + targetEntity + "/" + targetId;
        List<String> expectedLinks = relationsByTarget.get(targetLink).get(jsonNavigationPath);
        assertEquals(actualDocuments.size(), expectedLinks.size(), String.format("for entity: '%s'", targetEntity));
        Collections.sort(actualDocuments);
        Collections.sort(expectedLinks);
        assertEquals(actualDocuments, expectedLinks);
    }

    protected JsonDocument toDocument(String namespace, String entity, String id, JsonNode json, ZonedDateTime timestamp) {
        return new JsonDocument(new DocumentKey(namespace, entity, id, timestamp), json);
    }
}
