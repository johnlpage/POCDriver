package com.johnlpage.pocdriver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import com.mongodb.bulk.BulkWriteError;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.UpdateManyModel;
import com.mongodb.client.model.WriteModel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.bson.BsonDocument;
import org.bson.Document;
import org.junit.Test;

public class BulkRetryTest {

    private static List<WriteModel<Document>> inserts(int n) {
        List<WriteModel<Document>> ops = new ArrayList<WriteModel<Document>>();
        for (int i = 0; i < n; i++) {
            ops.add(new InsertOneModel<Document>(new Document("_id", new Document("w", 1).append("i", i))));
        }
        return ops;
    }

    private static BulkWriteError dup(int index) {
        return new BulkWriteError(BulkRetry.DUPLICATE_KEY, "E11000 duplicate key error", new BsonDocument(), index);
    }

    @Test
    public void orderedResubmitsOnlyOpsAfterTheDuplicate() {
        List<WriteModel<Document>> ops = inserts(5);
        BulkRetry r = BulkRetry.afterWriteErrors(ops, Collections.singletonList(dup(2)), 2, 0, true, false, true);
        assertEquals(3, r.inserted); // 2 applied before the error + the duplicate from the unknown attempt
        assertEquals(0, r.preexisting);
        assertEquals(ops.subList(3, 5), r.remaining);
    }

    @Test
    public void duplicateWithoutPriorUnknownAttemptIsPreexistingNotCounted() {
        List<WriteModel<Document>> ops = inserts(5);
        BulkRetry r = BulkRetry.afterWriteErrors(ops, Collections.singletonList(dup(0)), 0, 0, true, false, false);
        assertEquals(0, r.inserted);
        assertEquals(1, r.preexisting);
        assertEquals(ops.subList(1, 5), r.remaining);
    }

    @Test
    public void allDuplicatesLeavesNothingToResubmit() {
        // the old code crashed here: every op removed, no BulkWriteResult, NullPointerException
        List<WriteModel<Document>> ops = inserts(3);
        BulkRetry r = BulkRetry.afterWriteErrors(ops, Arrays.asList(dup(0), dup(1), dup(2)), 0, 0, false, false, true);
        assertEquals(3, r.inserted);
        assertTrue(r.remaining.isEmpty());
    }

    @Test
    public void unorderedCountsAppliedPlusDuplicatesAndResubmitsNothing() {
        List<WriteModel<Document>> ops = inserts(6);
        BulkRetry r = BulkRetry.afterWriteErrors(ops, Arrays.asList(dup(1), dup(4)), 4, 0, false, false, true);
        assertEquals(6, r.inserted);
        assertTrue(r.remaining.isEmpty());
    }

    @Test
    public void transactionCountsNothingAppliedAndResubmitsAllButDuplicates() {
        List<WriteModel<Document>> ops = inserts(4);
        BulkRetry r = BulkRetry.afterWriteErrors(ops, Collections.singletonList(dup(1)), 1, 0, true, true, true);
        assertEquals(1, r.inserted); // only the duplicate (committed by the earlier unknown attempt)
        assertEquals(3, r.remaining.size());
        assertSame(ops.get(0), r.remaining.get(0));
        assertSame(ops.get(2), r.remaining.get(1));
        assertSame(ops.get(3), r.remaining.get(2));
    }

    @Test(expected = IllegalStateException.class)
    public void otherWriteErrorsAreNotRetried() {
        BulkRetry.afterWriteErrors(inserts(2),
                Collections.singletonList(new BulkWriteError(121, "Document failed validation", new BsonDocument(), 0)),
                0, 0, true, false, false);
    }

    @Test(expected = IllegalStateException.class)
    public void duplicateKeyOnAnUpdateIsNotTreatedAsAlreadyInserted() {
        List<WriteModel<Document>> ops = new ArrayList<WriteModel<Document>>();
        ops.add(new UpdateManyModel<Document>(new Document("_id", 1), new Document("$set", new Document("a", 1))));
        BulkRetry.afterWriteErrors(ops, Collections.singletonList(dup(0)), 0, 0, true, false, true);
    }
}
