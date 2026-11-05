package com.johnlpage.pocdriver;

import com.mongodb.bulk.BulkWriteError;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.WriteModel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.bson.Document;

/**
 * Decides what to count and what to resubmit after a bulk write attempt failed with per-operation
 * write errors (a MongoBulkWriteException).
 *
 * <p>Only duplicate key errors on inserts are recoverable: the document is already there. If an
 * earlier attempt of the same batch had an unknown outcome (network error, timeout, failover), that
 * attempt is assumed to have inserted it, so it is counted as inserted. Otherwise the document
 * existed before this batch (e.g. a reused --threadIdStart) and it is reported as pre-existing,
 * not counted. Any other write error is not recoverable and is thrown.
 */
final class BulkRetry {
    static final int DUPLICATE_KEY = 11000;

    final List<WriteModel<Document>> remaining;
    final int inserted;
    final int matched;
    final int preexisting;

    private BulkRetry(List<WriteModel<Document>> remaining, int inserted, int matched, int preexisting) {
        this.remaining = remaining;
        this.inserted = inserted;
        this.matched = matched;
        this.preexisting = preexisting;
    }

    /**
     * @param attempted the operations submitted in the failed attempt, in submission order
     * @param errors the write errors of that attempt (indexes refer to {@code attempted})
     * @param appliedInserted inserted count reported for the failed attempt
     * @param appliedMatched matched count reported for the failed attempt
     * @param ordered whether the bulk write was ordered (the server stops at the first error)
     * @param inTransaction whether the attempt ran in a transaction (aborted: nothing was applied)
     * @param priorAttemptUnknown whether an earlier attempt of this batch had an unknown outcome
     */
    static BulkRetry afterWriteErrors(List<WriteModel<Document>> attempted, List<BulkWriteError> errors,
            int appliedInserted, int appliedMatched, boolean ordered, boolean inTransaction,
            boolean priorAttemptUnknown) {
        if (errors.isEmpty()) {
            throw new IllegalArgumentException("a failed bulk write must report at least one write error");
        }
        Set<Integer> duplicates = new HashSet<Integer>();
        for (BulkWriteError e : errors) {
            if (e.getCode() != DUPLICATE_KEY || !(attempted.get(e.getIndex()) instanceof InsertOneModel<?>)) {
                throw new IllegalStateException("Unrecoverable bulk write error at op " + e.getIndex()
                        + " (code " + e.getCode() + "): " + e.getMessage());
            }
            duplicates.add(e.getIndex());
        }
        int ours = priorAttemptUnknown ? duplicates.size() : 0;
        int preexisting = duplicates.size() - ours;

        if (inTransaction) {
            // the transaction was aborted: resubmit everything except the documents that already exist
            List<WriteModel<Document>> remaining = new ArrayList<WriteModel<Document>>();
            for (int i = 0; i < attempted.size(); i++) {
                if (!duplicates.contains(i)) {
                    remaining.add(attempted.get(i));
                }
            }
            return new BulkRetry(remaining, ours, 0, preexisting);
        }
        if (ordered) {
            // the server applied the ops before the error and did not attempt the ones after it
            if (errors.size() != 1) {
                throw new IllegalStateException("ordered bulk write reported " + errors.size() + " write errors");
            }
            int failedAt = errors.get(0).getIndex();
            List<WriteModel<Document>> remaining =
                    new ArrayList<WriteModel<Document>>(attempted.subList(failedAt + 1, attempted.size()));
            return new BulkRetry(remaining, appliedInserted + ours, appliedMatched, preexisting);
        }
        // unordered: every op was attempted
        return new BulkRetry(Collections.<WriteModel<Document>>emptyList(), appliedInserted + ours, appliedMatched,
                preexisting);
    }
}
