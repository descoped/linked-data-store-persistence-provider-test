package no.ssb.lds.core.persistence.test;

import no.ssb.lds.api.persistence.DocumentKey;
import no.ssb.lds.api.persistence.PersistenceDeletePolicy;
import no.ssb.lds.api.persistence.Transaction;
import no.ssb.lds.api.persistence.json.JsonDocument;
import no.ssb.lds.api.persistence.json.JsonPersistence;
import no.ssb.lds.api.specification.Specification;
import no.ssb.lds.api.specification.SpecificationElementType;
import org.json.JSONArray;
import org.json.JSONObject;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static no.ssb.lds.core.persistence.test.SpecificationBuilder.arrayNode;
import static no.ssb.lds.core.persistence.test.SpecificationBuilder.booleanNode;
import static no.ssb.lds.core.persistence.test.SpecificationBuilder.numericNode;
import static no.ssb.lds.core.persistence.test.SpecificationBuilder.objectNode;
import static no.ssb.lds.core.persistence.test.SpecificationBuilder.stringNode;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public abstract class PersistenceIntegrationTest {

    protected final Specification specification;
    protected final String namespace;
    protected JsonPersistence persistence;

    protected PersistenceIntegrationTest(String namespace) {
        this.namespace = namespace;
        this.specification = buildSpecification();
    }

    protected Specification buildSpecification() {
        return SpecificationBuilder.createSpecificationAndRoot(Set.of(
                objectNode(SpecificationElementType.MANAGED, "Person", Set.of(
                        stringNode("firstname"),
                        stringNode("lastname"),
                        numericNode("born"),
                        numericNode("bornWeightKg"),
                        booleanNode("isHuman")
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

    @Test
    public void thatDeleteAllVersionsWorks() {
        try (Transaction transaction = persistence.createTransaction(false)) {
            persistence.deleteAllVersions(transaction, namespace, "Address", "newyork", PersistenceDeletePolicy.FAIL_IF_INCOMING_LINKS).join();

            ZonedDateTime jan1624 = ZonedDateTime.of(1624, 1, 1, 12, 0, 0, (int) TimeUnit.MILLISECONDS.toNanos(0), ZoneId.of("Etc/UTC"));
            ZonedDateTime jan1626 = ZonedDateTime.of(1626, 1, 1, 12, 0, 0, (int) TimeUnit.MILLISECONDS.toNanos(0), ZoneId.of("Etc/UTC"));
            ZonedDateTime jan1664 = ZonedDateTime.of(1664, 1, 1, 12, 0, 0, (int) TimeUnit.MILLISECONDS.toNanos(0), ZoneId.of("Etc/UTC"));
            JsonDocument input0 = toDocument(namespace, "Address", "newyork", createAddress("", "NY", "USA"), jan1624);
            persistence.createOrOverwrite(transaction, input0, specification).join();
            JsonDocument input1 = toDocument(namespace, "Address", "newyork", createAddress("New Amsterdam", "NY", "USA"), jan1626);
            persistence.createOrOverwrite(transaction, input1, specification).join();
            JsonDocument input2 = toDocument(namespace, "Address", "newyork", createAddress("New York", "NY", "USA"), jan1664);
            persistence.createOrOverwrite(transaction, input2, specification).join();
            Iterator<JsonDocument> iteratorWithDocuments = persistence.readAllVersions(transaction, namespace, "Address", "newyork", null, 100).join().iterator();

            assertEquals(size(iteratorWithDocuments), 3);

            persistence.deleteAllVersions(transaction, namespace, "Address", "newyork", PersistenceDeletePolicy.FAIL_IF_INCOMING_LINKS).join();

            Iterator<JsonDocument> iterator = persistence.readAllVersions(transaction, namespace, "Address", "newyork", null, 100).join().iterator();

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
    public void thatBasicCreateThenReadWorks() {
        try (Transaction transaction = persistence.createTransaction(false)) {
            persistence.deleteAllVersions(transaction, namespace, "Person", "john", PersistenceDeletePolicy.FAIL_IF_INCOMING_LINKS).join();

            ZonedDateTime oct18 = ZonedDateTime.of(2018, 10, 7, 19, 49, 26, (int) TimeUnit.MILLISECONDS.toNanos(307), ZoneId.of("Etc/UTC"));
            JsonDocument input = toDocument(namespace, "Person", "john", createPerson("John", "Smith"), oct18);
            CompletableFuture<Void> completableFuture = persistence.createOrOverwrite(transaction, input, specification);
            completableFuture.join();
            CompletableFuture<JsonDocument> completableDocumentIterator = persistence.read(transaction, oct18, namespace, "Person", "john");
            JsonDocument output = completableDocumentIterator.join();
            assertNotNull(output);
            assertFalse(output == input);
            assertEquals(output.document().toString(), input.document().toString());
        }
    }

    @Test
    public void thatCreateWithSameVersionDoesOverwriteInsteadOfCreatingDuplicateVersions() {
        try (Transaction transaction = persistence.createTransaction(false)) {
            persistence.deleteAllVersions(transaction, namespace, "Person", "john", PersistenceDeletePolicy.FAIL_IF_INCOMING_LINKS).join();

            ZonedDateTime oct18 = ZonedDateTime.of(2018, 10, 7, 19, 49, 26, (int) TimeUnit.MILLISECONDS.toNanos(307), ZoneId.of("Etc/UTC"));
            JsonDocument input = toDocument(namespace, "Person", "john", createPerson("Jimmy", "Smith"), oct18);
            JsonDocument input2 = toDocument(namespace, "Person", "john", createPerson("John", "Smith"), oct18);
            CompletableFuture<Void> completableFuture = persistence.createOrOverwrite(transaction, input, specification);
            completableFuture.join();
            CompletableFuture<Void> completableFuture2 = persistence.createOrOverwrite(transaction, input2, specification);
            completableFuture2.join();
            CompletableFuture<Iterable<JsonDocument>> completableDocumentIterator = persistence.readAllVersions(transaction, namespace, "Person", "john", null, 10);
            Iterable<JsonDocument> iterable = completableDocumentIterator.join();
            Iterator<JsonDocument> iterator = iterable.iterator();
            assertTrue(iterator.hasNext());
            assertNotNull(iterator.next());
            assertFalse(iterator.hasNext());
        }
    }

    @Test
    public void thatBasicTimeBasedVersioningWorks() {
        try (Transaction transaction = persistence.createTransaction(false)) {
            persistence.deleteAllVersions(transaction, namespace, "Address", "newyork", PersistenceDeletePolicy.FAIL_IF_INCOMING_LINKS).join();

            ZonedDateTime jan1624 = ZonedDateTime.of(1624, 1, 1, 12, 0, 0, (int) TimeUnit.MILLISECONDS.toNanos(0), ZoneId.of("Etc/UTC"));
            ZonedDateTime jan1626 = ZonedDateTime.of(1626, 1, 1, 12, 0, 0, (int) TimeUnit.MILLISECONDS.toNanos(0), ZoneId.of("Etc/UTC"));
            ZonedDateTime jan1664 = ZonedDateTime.of(1664, 1, 1, 12, 0, 0, (int) TimeUnit.MILLISECONDS.toNanos(0), ZoneId.of("Etc/UTC"));
            JsonDocument input0 = toDocument(namespace, "Address", "newyork", createAddress("", "NY", "USA"), jan1624);
            persistence.createOrOverwrite(transaction, input0, specification).join();
            JsonDocument input2 = toDocument(namespace, "Address", "newyork", createAddress("New York", "NY", "USA"), jan1664);
            persistence.createOrOverwrite(transaction, input2, specification).join();
            JsonDocument input1a = toDocument(namespace, "Address", "newyork", createAddress("1a New Amsterdam", "NY", "USA"), jan1626);
            JsonDocument input1b = toDocument(namespace, "Address", "newyork", createAddress("1b New Amsterdam", "NY", "USA"), jan1626);
            persistence.createOrOverwrite(transaction, input1a, specification).join();
            persistence.createOrOverwrite(transaction, input1b, specification).join();
            Iterator<JsonDocument> iterator = persistence.readAllVersions(transaction, namespace, "Address", "newyork", null, 100).join().iterator();
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
            persistence.deleteAllVersions(transaction, namespace, "Address", "newyork", PersistenceDeletePolicy.FAIL_IF_INCOMING_LINKS).join();

            ZonedDateTime jan1624 = ZonedDateTime.of(1624, 1, 1, 12, 0, 0, (int) TimeUnit.MILLISECONDS.toNanos(0), ZoneId.of("Etc/UTC"));
            ZonedDateTime jan1626 = ZonedDateTime.of(1626, 1, 1, 12, 0, 0, (int) TimeUnit.MILLISECONDS.toNanos(0), ZoneId.of("Etc/UTC"));
            ZonedDateTime feb1663 = ZonedDateTime.of(1663, 2, 1, 0, 0, 0, (int) TimeUnit.MILLISECONDS.toNanos(0), ZoneId.of("Etc/UTC"));
            ZonedDateTime jan1664 = ZonedDateTime.of(1664, 1, 1, 12, 0, 0, (int) TimeUnit.MILLISECONDS.toNanos(0), ZoneId.of("Etc/UTC"));

            persistence.createOrOverwrite(transaction, toDocument(namespace, "Address", "newyork", createAddress("", "NY", "USA"), jan1624), specification).join();
            persistence.createOrOverwrite(transaction, toDocument(namespace, "Address", "newyork", createAddress("New Amsterdam", "NY", "USA"), jan1626), specification).join();
            persistence.createOrOverwrite(transaction, toDocument(namespace, "Address", "newyork", createAddress("New York", "NY", "USA"), jan1664), specification).join();

            assertEquals(size(persistence.readAllVersions(transaction, namespace, "Address", "newyork", null, 100).join().iterator()), 3);

            persistence.markDeleted(transaction, namespace, "Address", "newyork", feb1663, PersistenceDeletePolicy.FAIL_IF_INCOMING_LINKS).join();

            assertEquals(size(persistence.readAllVersions(transaction, namespace, "Address", "newyork", null, 100).join().iterator()), 4);

            persistence.delete(transaction, namespace, "Address", "newyork", feb1663, PersistenceDeletePolicy.FAIL_IF_INCOMING_LINKS).join();

            assertEquals(size(persistence.readAllVersions(transaction, namespace, "Address", "newyork", null, 100).join().iterator()), 3);

            persistence.markDeleted(transaction, namespace, "Address", "newyork", feb1663, PersistenceDeletePolicy.FAIL_IF_INCOMING_LINKS).join();

            assertEquals(size(persistence.readAllVersions(transaction, namespace, "Address", "newyork", null, 100).join().iterator()), 4);
        }
    }

    @Test
    public void thatReadVersionsInRangeWorks() {
        try (Transaction transaction = persistence.createTransaction(false)) {
            persistence.deleteAllVersions(transaction, namespace, "Person", "john", PersistenceDeletePolicy.FAIL_IF_INCOMING_LINKS).join();

            ZonedDateTime aug92 = ZonedDateTime.of(1992, 8, 1, 13, 43, 20, (int) TimeUnit.MILLISECONDS.toNanos(301), ZoneId.of("Etc/UTC"));
            ZonedDateTime feb10 = ZonedDateTime.of(2010, 2, 3, 15, 45, 22, (int) TimeUnit.MILLISECONDS.toNanos(303), ZoneId.of("Etc/UTC"));
            ZonedDateTime nov13 = ZonedDateTime.of(2013, 11, 5, 17, 47, 24, (int) TimeUnit.MILLISECONDS.toNanos(305), ZoneId.of("Etc/UTC"));
            ZonedDateTime sep18 = ZonedDateTime.of(2018, 9, 6, 18, 48, 25, (int) TimeUnit.MILLISECONDS.toNanos(306), ZoneId.of("Etc/UTC"));
            ZonedDateTime oct18 = ZonedDateTime.of(2018, 10, 7, 19, 49, 26, (int) TimeUnit.MILLISECONDS.toNanos(307), ZoneId.of("Etc/UTC"));
            JsonDocument expected2 = toDocument(namespace, "Person", "john", createPerson("John", "Smith"), aug92);
            persistence.createOrOverwrite(transaction, expected2, specification).join();
            JsonDocument expected1 = toDocument(namespace, "Person", "john", createPerson("James", "Smith"), nov13);
            persistence.createOrOverwrite(transaction, expected1, specification).join();
            persistence.createOrOverwrite(transaction, toDocument(namespace, "Person", "john", createPerson("Jones", "Smith"), oct18), specification).join();

            Iterator<JsonDocument> iterator = persistence.readVersions(transaction, feb10.minusYears(1), sep18.plusHours(1), namespace, "Person", "john", null, 100).join().iterator();
            JsonDocument doc1 = iterator.next();
            JsonDocument doc2 = iterator.next();
            assertFalse(iterator.hasNext(), "expected to contain exactly 2 elements");
            assertEquals(expected1.document().toString(2), doc1.document().toString(2));
            assertEquals(expected2.document().toString(2), doc2.document().toString(2));
        }
    }

    @Test
    public void thatReadVersionsInRangeWhenOnlyOneVersionExistsWorks() {
        try (Transaction transaction = persistence.createTransaction(false)) {
            persistence.deleteAllVersions(transaction, namespace, "Person", "john", PersistenceDeletePolicy.FAIL_IF_INCOMING_LINKS).join();

            ZonedDateTime nov13 = ZonedDateTime.of(2013, 11, 5, 17, 47, 24, (int) TimeUnit.MILLISECONDS.toNanos(305), ZoneId.of("Etc/UTC"));
            JsonDocument expected1 = toDocument(namespace, "Person", "john", createPerson("John", "Smith"), nov13);
            persistence.createOrOverwrite(transaction, expected1, specification).join();

            Iterator<JsonDocument> iterator = persistence.readVersions(transaction, nov13.minusYears(1), nov13.plusYears(1), namespace, "Person", "john", null, 100).join().iterator();
            JsonDocument doc1 = iterator.next();
            assertFalse(iterator.hasNext(), "expected to contain exactly 1 document");
            assertEquals(expected1.document().toString(2), doc1.document().toString(2));
        }
    }

    @Test
    public void thatReadVersionsWithFromAndToCornerCasesWorks() {
        try (Transaction transaction = persistence.createTransaction(false)) {
            persistence.deleteAllVersions(transaction, namespace, "Person", "john", PersistenceDeletePolicy.FAIL_IF_INCOMING_LINKS).join();

            ZonedDateTime nov13 = ZonedDateTime.of(2013, 11, 5, 17, 47, 24, (int) TimeUnit.MILLISECONDS.toNanos(305), ZoneId.of("Etc/UTC"));
            JsonDocument expected1 = toDocument(namespace, "Person", "john", createPerson("John", "Smith"), nov13);
            persistence.createOrOverwrite(transaction, expected1, specification).join();

            {
                // case 1, from == version and to == version. Expect no document because to argument uses exclusive syntax
                Iterator<JsonDocument> iterator = persistence.readVersions(transaction, nov13, nov13, namespace, "Person", "john", null, 100).join().iterator();
                assertEquals(size(iterator), 0);
            }
            {
                // case 2, from == version and to == tick(version). Expect one document. This is the tightest range possible without yielding empty result.
                Iterator<JsonDocument> iterator = persistence.readVersions(transaction, nov13, nov13.plus(1, ChronoUnit.MILLIS), namespace, "Person", "john", null, 100).join().iterator();
                assertEquals(size(iterator), 1);
            }
            {
                // case 3, from == version and to == leap(version). Expect one document. To is a whole year after version.
                Iterator<JsonDocument> iterator = persistence.readVersions(transaction, nov13, nov13.plusYears(1), namespace, "Person", "john", null, 100).join().iterator();
                assertEquals(size(iterator), 1);
            }
            {
                // case 4, from == backtick(version) and to == version. Expect no document because to argument uses exclusive syntax
                Iterator<JsonDocument> iterator = persistence.readVersions(transaction, nov13.minus(1, ChronoUnit.MILLIS), nov13, namespace, "Person", "john", null, 100).join().iterator();
                assertEquals(size(iterator), 0);
            }
            {
                // case 5, from == backleap(version) and to == backtick(version). Expect no document because to argument is less than version.
                Iterator<JsonDocument> iterator = persistence.readVersions(transaction, nov13.minusYears(1), nov13.minus(1, ChronoUnit.MILLIS), namespace, "Person", "john", null, 100).join().iterator();
                assertEquals(size(iterator), 0);
            }
            {
                // case 5, from == backleap(version) and to == leap(version). Expect one document in middle of range.
                Iterator<JsonDocument> iterator = persistence.readVersions(transaction, nov13.minusYears(1), nov13.plusYears(1), namespace, "Person", "john", null, 100).join().iterator();
                assertEquals(size(iterator), 1);
            }
            {
                // case 6, from == leap(version) and to == backleap(version). Expect exception due to negative range.
                try {
                    persistence.readVersions(transaction, nov13.plusYears(1), nov13.minusYears(1), namespace, "Person", "john", null, 100).join().iterator();
                    Assert.fail("Expected IllegalArgumentException");
                } catch (IllegalArgumentException e) {
                }
            }
        }
    }

    @Test
    public void thatReadAllVersionsWorks() {
        try (Transaction transaction = persistence.createTransaction(false)) {
            persistence.deleteAllVersions(transaction, namespace, "Person", "john", PersistenceDeletePolicy.FAIL_IF_INCOMING_LINKS).join();

            ZonedDateTime aug92 = ZonedDateTime.of(1992, 8, 1, 13, 43, 20, (int) TimeUnit.MILLISECONDS.toNanos(301), ZoneId.of("Etc/UTC"));
            ZonedDateTime feb10 = ZonedDateTime.of(2010, 2, 3, 15, 45, 22, (int) TimeUnit.MILLISECONDS.toNanos(303), ZoneId.of("Etc/UTC"));
            ZonedDateTime nov13 = ZonedDateTime.of(2013, 11, 5, 17, 47, 24, (int) TimeUnit.MILLISECONDS.toNanos(305), ZoneId.of("Etc/UTC"));
            ZonedDateTime sep18 = ZonedDateTime.of(2018, 9, 6, 18, 48, 25, (int) TimeUnit.MILLISECONDS.toNanos(306), ZoneId.of("Etc/UTC"));
            ZonedDateTime oct18 = ZonedDateTime.of(2018, 10, 7, 19, 49, 26, (int) TimeUnit.MILLISECONDS.toNanos(307), ZoneId.of("Etc/UTC"));
            persistence.createOrOverwrite(transaction, toDocument(namespace, "Person", "john", createPerson("John", "Smith"), aug92), specification).join();
            persistence.createOrOverwrite(transaction, toDocument(namespace, "Person", "john", createPerson("James", "Smith"), nov13), specification).join();
            persistence.createOrOverwrite(transaction, toDocument(namespace, "Person", "john", createPerson("John", "Smith"), oct18), specification).join();

            assertEquals(size(persistence.readAllVersions(transaction, namespace, "Person", "john", null, 100).join().iterator()), 3);
        }
    }

    @Test
    public void thatFindSimpleWithPathAndValueWorks() {
        try (Transaction transaction = persistence.createTransaction(false)) {
            persistence.deleteAllVersions(transaction, namespace, "Person", "simple", PersistenceDeletePolicy.FAIL_IF_INCOMING_LINKS).join();
            ZonedDateTime sep18 = ZonedDateTime.of(2018, 9, 6, 18, 48, 25, (int) TimeUnit.MILLISECONDS.toNanos(306), ZoneId.of("Etc/UTC"));
            ZonedDateTime oct18 = ZonedDateTime.of(2018, 10, 7, 19, 49, 26, (int) TimeUnit.MILLISECONDS.toNanos(307), ZoneId.of("Etc/UTC"));
            persistence.createOrOverwrite(transaction, toDocument(namespace, "Person", "simple", new JSONObject().put("firstname", "Simple"), sep18), specification).join();

            Iterator<JsonDocument> iterator = persistence.find(transaction, oct18, namespace, "Person", "$.firstname", "Simple", null, 100).join().iterator();
            assertTrue(iterator.hasNext());
            JsonDocument person1 = iterator.next();
            assertEquals(person1.document().getString("firstname"), "Simple");
            assertFalse(iterator.hasNext());
        }
    }

    //@Test
    public void thatFindAllWithPathAndValueWorks() {
        try (Transaction transaction = persistence.createTransaction(false)) {
            persistence.deleteAllVersions(transaction, namespace, "Person", "john", PersistenceDeletePolicy.FAIL_IF_INCOMING_LINKS).join();
            persistence.deleteAllVersions(transaction, namespace, "Person", "jane", PersistenceDeletePolicy.FAIL_IF_INCOMING_LINKS).join();

            ZonedDateTime aug92 = ZonedDateTime.of(1992, 8, 1, 13, 43, 20, (int) TimeUnit.MILLISECONDS.toNanos(301), ZoneId.of("Etc/UTC"));
            ZonedDateTime sep94 = ZonedDateTime.of(1994, 9, 1, 13, 43, 20, (int) TimeUnit.MILLISECONDS.toNanos(301), ZoneId.of("Etc/UTC"));
            ZonedDateTime feb10 = ZonedDateTime.of(2010, 2, 3, 15, 45, 22, (int) TimeUnit.MILLISECONDS.toNanos(303), ZoneId.of("Etc/UTC"));
            ZonedDateTime nov13 = ZonedDateTime.of(2013, 11, 5, 17, 47, 24, (int) TimeUnit.MILLISECONDS.toNanos(305), ZoneId.of("Etc/UTC"));
            ZonedDateTime sep18 = ZonedDateTime.of(2018, 9, 6, 18, 48, 25, (int) TimeUnit.MILLISECONDS.toNanos(306), ZoneId.of("Etc/UTC"));
            ZonedDateTime oct18 = ZonedDateTime.of(2018, 10, 7, 19, 49, 26, (int) TimeUnit.MILLISECONDS.toNanos(307), ZoneId.of("Etc/UTC"));
            persistence.createOrOverwrite(transaction, toDocument(namespace, "Person", "john", createPerson("John", "Smith"), aug92), specification).join();
            persistence.createOrOverwrite(transaction, toDocument(namespace, "Person", "jane", createPerson("Jane", "Doe"), sep94), specification).join();
            persistence.createOrOverwrite(transaction, toDocument(namespace, "Person", "jane", createPerson("Jane", "Smith"), feb10), specification).join();
            persistence.createOrOverwrite(transaction, toDocument(namespace, "Person", "john", createPerson("James", "Smith"), nov13), specification).join();
            persistence.createOrOverwrite(transaction, toDocument(namespace, "Person", "john", createPerson("John", "Smith"), oct18), specification).join();

            Iterator<JsonDocument> iterator = persistence.find(transaction, sep18, namespace, "Person", "$.lastname", "Smith", null, 100).join().iterator();

            JsonDocument person1 = iterator.next();
            JsonDocument person2 = iterator.next();
            assertFalse(iterator.hasNext());

            if (person1.document().getString("firstname").equals("Jane")) {
                assertEquals(person2.document().getString("firstname"), "James");
            } else {
                assertEquals(person1.document().getString("firstname"), "James");
                assertEquals(person2.document().getString("firstname"), "Jane");
            }
        }
    }


    @Test
    public void thatFindAllWorks() {
        try (Transaction transaction = persistence.createTransaction(false)) {
            // TODO Consider support for deleting entire entity in one operation...?
            persistence.deleteAllVersions(transaction, namespace, "Person", "john", PersistenceDeletePolicy.FAIL_IF_INCOMING_LINKS).join();
            persistence.deleteAllVersions(transaction, namespace, "Person", "jane", PersistenceDeletePolicy.FAIL_IF_INCOMING_LINKS).join();

            ZonedDateTime aug92 = ZonedDateTime.of(1992, 8, 1, 13, 43, 20, (int) TimeUnit.MILLISECONDS.toNanos(301), ZoneId.of("Etc/UTC"));
            ZonedDateTime sep94 = ZonedDateTime.of(1994, 9, 1, 13, 43, 20, (int) TimeUnit.MILLISECONDS.toNanos(301), ZoneId.of("Etc/UTC"));
            ZonedDateTime feb10 = ZonedDateTime.of(2010, 2, 3, 15, 45, 22, (int) TimeUnit.MILLISECONDS.toNanos(303), ZoneId.of("Etc/UTC"));
            ZonedDateTime dec11 = ZonedDateTime.of(2011, 12, 4, 16, 46, 23, (int) TimeUnit.MILLISECONDS.toNanos(304), ZoneId.of("Etc/UTC"));
            ZonedDateTime nov13 = ZonedDateTime.of(2013, 11, 5, 17, 47, 24, (int) TimeUnit.MILLISECONDS.toNanos(305), ZoneId.of("Etc/UTC"));
            ZonedDateTime oct18 = ZonedDateTime.of(2018, 10, 7, 19, 49, 26, (int) TimeUnit.MILLISECONDS.toNanos(307), ZoneId.of("Etc/UTC"));
            persistence.createOrOverwrite(transaction, toDocument(namespace, "Person", "john", createPerson("John", "Smith"), aug92), specification).join();
            persistence.createOrOverwrite(transaction, toDocument(namespace, "Person", "jane", createPerson("Jane", "Doe"), sep94), specification).join();
            persistence.createOrOverwrite(transaction, toDocument(namespace, "Person", "jane", createPerson("Jane", "Smith"), feb10), specification).join();
            persistence.createOrOverwrite(transaction, toDocument(namespace, "Person", "john", createPerson("James", "Smith"), nov13), specification).join();
            persistence.createOrOverwrite(transaction, toDocument(namespace, "Person", "john", createPerson("John", "Smith"), oct18), specification).join();

            Iterator<JsonDocument> iterator = persistence.findAll(transaction, dec11, namespace, "Person", null, 100).join().iterator();

            assertTrue(iterator.hasNext());
            JsonDocument person1 = iterator.next();
            assertTrue(iterator.hasNext());
            JsonDocument person2 = iterator.next();
            assertFalse(iterator.hasNext());

            if (person1.document().getString("firstname").equals("Jane")) {
                assertEquals(person2.document().getString("firstname"), "John");
            } else {
                assertEquals(person1.document().getString("firstname"), "John");
                assertEquals(person2.document().getString("firstname"), "Jane");
            }

        }
    }

    @Test
    public void thatBigValueWorks() {
        try (Transaction transaction = persistence.createTransaction(false)) {
            persistence.deleteAllVersions(transaction, namespace, "FunkyLongAddress", "newyork", PersistenceDeletePolicy.FAIL_IF_INCOMING_LINKS).join();

            ZonedDateTime oct18 = ZonedDateTime.of(2018, 10, 7, 19, 49, 26, (int) TimeUnit.MILLISECONDS.toNanos(307), ZoneId.of("Etc/UTC"));
            ZonedDateTime now = ZonedDateTime.now(ZoneId.of("Etc/UTC"));

            String bigString = "12345678901234567890";
            for (int i = 0; i < 12; i++) {
                bigString = bigString + "_" + bigString;
            }

            // Creating funky long address
            persistence.createOrOverwrite(transaction, toDocument(namespace, "FunkyLongAddress", "newyork", createAddress(bigString, "NY", "USA"), oct18), specification).join();

            // Finding funky long address by city
            Iterable<JsonDocument> funkyLongAddress = persistence.find(transaction, now, namespace, "FunkyLongAddress", "$.city", bigString, null, 100).join();
            Iterator<JsonDocument> iterator = funkyLongAddress.iterator();
            assertTrue(iterator.hasNext());
            JsonDocument foundDocument = iterator.next();
            assertFalse(iterator.hasNext());
            String foundBigString = foundDocument.document().getString("city");
            assertEquals(foundBigString, bigString);

            // Finding funky long address by city (with non-matching value)
            int findExpectNoMatchSize = size(persistence.find(transaction, now, namespace, "FunkyLongAddress", "$.city", bigString + "1", null, 100).join().iterator());
            assertEquals(findExpectNoMatchSize, 0);

            // Deleting funky long address
            persistence.deleteAllVersions(transaction, namespace, "FunkyLongAddress", "newyork", PersistenceDeletePolicy.FAIL_IF_INCOMING_LINKS).join();
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
            ZonedDateTime oct18 = ZonedDateTime.of(2018, 10, 7, 19, 49, 26, (int) TimeUnit.MILLISECONDS.toNanos(307), ZoneId.of("Etc/UTC"));
            JSONObject doc = new JSONObject().put("name", new JSONArray()
                    .put("John Smith")
                    .put("Jane Doe")
            );
            JsonDocument input = toDocument(namespace, "People", "1", doc, oct18);
            persistence.createOrOverwrite(transaction, input, specification).join();
            JsonDocument jsonDocument = persistence.read(transaction, oct18, namespace, "People", "1").join();
            assertEquals(jsonDocument.document().toString(), doc.toString());
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
            ZonedDateTime oct18 = ZonedDateTime.of(2018, 10, 7, 19, 49, 26, (int) TimeUnit.MILLISECONDS.toNanos(307), ZoneId.of("Etc/UTC"));
            JSONObject doc = new JSONObject().put("name", new JSONArray()
                    .put(new JSONObject().put("first", "John").put("last", "Smith"))
                    .put(new JSONObject().put("first", "Jane").put("last", "Doe"))
            );
            JsonDocument input = toDocument(namespace, "People", "1", doc, oct18);
            persistence.createOrOverwrite(transaction, input, specification).join();
            JsonDocument jsonDocument = persistence.read(transaction, oct18, namespace, "People", "1").join();
            assertNotNull(jsonDocument);
            //System.out.format("%s%n", jsonDocument.document().toString());
            assertEquals(jsonDocument.document().toString(), doc.toString());
        }
    }

    protected static JSONObject createPerson(String firstname, String lastname) {
        JSONObject person = new JSONObject();
        person.put("firstname", firstname);
        person.put("lastname", lastname);
        person.put("born", 1998);
        person.put("bornWeightKg", 3.82);
        person.put("isHuman", true);
        return person;
    }

    protected static JSONObject createAddress(String city, String state, String country) {
        JSONObject address = new JSONObject();
        address.put("city", city);
        address.put("state", state);
        address.put("country", country);
        return address;
    }

    protected JsonDocument toDocument(String namespace, String entity, String id, JSONObject json, ZonedDateTime timestamp) {
        return new JsonDocument(new DocumentKey(namespace, entity, id, timestamp), json);
    }
}
