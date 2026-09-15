/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.nvidia.spark.history.local;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import com.nvidia.spark.history.HistoryMetricCatalog;

/** Bounded filesystem publication and read layer for the package-private local snapshot codec. */
final class LocalSnapshotFiles {
  private static final int EXACT_BUFFER_BYTES = 8192;
  private static final LocalMetricStorePlanningAdapter.Ticker SYSTEM_TICKER =
      new LocalMetricStorePlanningAdapter.Ticker() {
        @Override
        public long readNanos() {
          return System.nanoTime();
        }
      };
  private static final GuardWaiter SYSTEM_GUARD_WAITER =
      new GuardWaiter() {
        @Override
        public long await(Condition condition, long remainingNanos)
            throws InterruptedException {
          return condition.awaitNanos(remainingNanos);
        }
      };
  private static final FileOperations SYSTEM_FILE_OPERATIONS =
      new SystemFileOperations();
  private static final Object GUARD_MAP_LOCK = new Object();
  private static final Map<Path, GuardEntry> PATH_GUARDS =
      new HashMap<Path, GuardEntry>();

  private LocalSnapshotFiles() {
  }

  static void save(
      Path target,
      LocalSnapshotState state,
      HistoryMetricCatalog catalog,
      Duration maximumPlanningAge,
      Duration timeout) throws LocalSnapshotException {
    LocalSnapshotDeadline deadline =
        LocalSnapshotDeadline.start(timeout, SYSTEM_TICKER);
    saveWithDeadline(
        target,
        state,
        catalog,
        maximumPlanningAge,
        deadline,
        SYSTEM_FILE_OPERATIONS,
        SYSTEM_GUARD_WAITER);
  }

  static LocalSnapshotState load(
      Path source,
      HistoryMetricCatalog catalog,
      Duration maximumPlanningAge,
      Duration timeout) throws LocalSnapshotException {
    LocalSnapshotDeadline deadline =
        LocalSnapshotDeadline.start(timeout, SYSTEM_TICKER);
    return loadWithDeadline(
        source,
        catalog,
        maximumPlanningAge,
        deadline,
        SYSTEM_FILE_OPERATIONS,
        SYSTEM_GUARD_WAITER);
  }

  static FileOperations systemFileOperations() {
    return SYSTEM_FILE_OPERATIONS;
  }

  static void saveForTest(
      Path target,
      LocalSnapshotState state,
      HistoryMetricCatalog catalog,
      Duration maximumPlanningAge,
      Duration timeout,
      LocalMetricStorePlanningAdapter.Ticker ticker,
      FileOperations operations,
      GuardWaiter waiter) throws LocalSnapshotException {
    LocalSnapshotDeadline deadline =
        LocalSnapshotDeadline.start(timeout, ticker);
    saveWithDeadline(
        target,
        state,
        catalog,
        maximumPlanningAge,
        deadline,
        operations,
        waiter);
  }

  static LocalSnapshotState loadForTest(
      Path source,
      HistoryMetricCatalog catalog,
      Duration maximumPlanningAge,
      Duration timeout,
      LocalMetricStorePlanningAdapter.Ticker ticker,
      FileOperations operations,
      GuardWaiter waiter) throws LocalSnapshotException {
    LocalSnapshotDeadline deadline =
        LocalSnapshotDeadline.start(timeout, ticker);
    return loadWithDeadline(
        source,
        catalog,
        maximumPlanningAge,
        deadline,
        operations,
        waiter);
  }

  static void saveWithDeadline(
      Path target,
      final LocalSnapshotState state,
      HistoryMetricCatalog catalog,
      Duration maximumPlanningAge,
      LocalSnapshotDeadline deadline,
      FileOperations operations,
      GuardWaiter waiter) throws LocalSnapshotException {
    Objects.requireNonNull(state, "state");
    saveCapturedWithDeadline(
        target,
        catalog,
        maximumPlanningAge,
        deadline,
        operations,
        waiter,
        new SnapshotStateCapture() {
          @Override
          public LocalSnapshotState capture() {
            return state;
          }
        });
  }

  static SaveResult saveCapturedWithDeadline(
      Path target,
      HistoryMetricCatalog catalog,
      Duration maximumPlanningAge,
      LocalSnapshotDeadline deadline,
      FileOperations operations,
      GuardWaiter waiter,
      SnapshotStateCapture capture) throws LocalSnapshotException {
    Objects.requireNonNull(target, "target");
    validateReadArguments(
        catalog, maximumPlanningAge, deadline, operations, waiter);
    Objects.requireNonNull(capture, "capture");

    Path normalized = normalize(target, operations);
    try (GuardLease ignored = acquire(normalized, deadline, waiter)) {
      LocalSnapshotState state =
          Objects.requireNonNull(capture.capture(), "captured snapshot state");
      return saveGuarded(
          normalized,
          state,
          catalog,
          maximumPlanningAge,
          deadline,
          operations);
    }
  }

  private static SaveResult saveGuarded(
      Path normalized,
      LocalSnapshotState state,
      HistoryMetricCatalog catalog,
      Duration maximumPlanningAge,
      LocalSnapshotDeadline deadline,
      FileOperations operations) throws LocalSnapshotException {
      deadline.throwIfExpired();
      Path parent = normalized.getParent();
      if (parent == null || normalized.getFileName() == null) {
        throw failure(
            LocalSnapshotException.Reason.IO,
            "snapshot target must have an existing parent directory");
      }

      Path temporary = null;
      boolean moveStarted = false;
      byte[] intendedImage = null;
      Path priorTemporary = null;
      PriorTarget priorTarget = null;
      LocalSnapshotException primary = null;
      boolean committed = false;
      boolean cleanupFailed = false;
      try {
        operations.requireDirectory(parent);
        deadline.throwIfExpired();

        intendedImage = LocalSnapshotCodec.encode(state);
        if (intendedImage.length > LocalSnapshotCodec.MAX_FILE_BYTES) {
          throw failure(
              LocalSnapshotException.Reason.BOUNDS,
              "snapshot image exceeds the current file bound");
        }
        deadline.throwIfExpired();

        String prefix = "." + normalized.getFileName().toString() + ".history-";
        temporary = operations.createTemp(parent, prefix, ".tmp");
        deadline.throwIfExpired();

        operations.writeAndSync(temporary, intendedImage);
        deadline.throwIfExpired();

        LocalSnapshotState validated =
            readState(
                temporary,
                catalog,
                maximumPlanningAge,
                deadline,
                operations);
        if (!state.equals(validated)) {
          throw failure(
              LocalSnapshotException.Reason.FORMAT,
              "validated snapshot image differs from captured state");
        }
        deadline.throwIfExpired();
        if (exactMatch(temporary, intendedImage, deadline, operations) !=
            ExactMatch.MATCH) {
          throw failure(
              LocalSnapshotException.Reason.FORMAT,
              "validated snapshot temporary bytes differ from encoded state");
        }
        deadline.throwIfExpired();

        priorTarget = probePriorTarget(normalized, deadline, operations);
        if (priorTarget.kind == PriorKind.PRESENT) {
          priorTemporary =
              operations.createTemp(parent, prefix, ".prior.tmp");
          deadline.throwIfExpired();
          operations.requireRegularFile(normalized);
          deadline.throwIfExpired();
          operations.copyAndSync(
              normalized,
              priorTemporary,
              priorTarget.length,
              deadline);
          deadline.throwIfExpired();
          priorTarget = PriorTarget.present(priorTemporary, priorTarget.length);
        }
        moveStarted = true;
        operations.move(
            temporary,
            normalized,
            StandardCopyOption.ATOMIC_MOVE,
            StandardCopyOption.REPLACE_EXISTING);
        committed = true;
      } catch (AtomicMoveNotSupportedException unsupported) {
        LocalSnapshotException.Reason reason = moveStarted
            ? LocalSnapshotException.Reason.ATOMIC_MOVE_UNSUPPORTED
            : LocalSnapshotException.Reason.IO;
        primary = failure(
            reason,
            moveStarted
                ? "atomic snapshot replacement is not supported"
                : "snapshot filesystem operation failed",
            redactedCause(moveStarted
                ? "atomic snapshot replacement is unsupported"
                : "snapshot filesystem operation failed"));
        throw primary;
      } catch (LocalSnapshotException expected) {
        primary = expected;
        throw expected;
      } catch (SecurityException denied) {
        primary = failure(
            LocalSnapshotException.Reason.IO,
            "snapshot filesystem access was denied",
            redactedCause("snapshot filesystem access was denied"));
        throw primary;
      } catch (UnsupportedOperationException unsupported) {
        if (moveStarted) {
          primary = failure(
              LocalSnapshotException.Reason.ATOMIC_MOVE_UNSUPPORTED,
              "atomic snapshot replacement is not supported",
              redactedCause("atomic snapshot replacement is unsupported"));
        } else {
          primary = failure(
              LocalSnapshotException.Reason.IO,
              "snapshot filesystem operation is unsupported",
              redactedCause("snapshot filesystem operation is unsupported"));
        }
        throw primary;
      } catch (IOException io) {
        if (moveStarted) {
          MoveVerification verification =
              verifyMoveOutcome(
                  normalized,
                  intendedImage,
                  priorTarget,
                  deadline,
                  operations);
          if (verification.outcome == MoveOutcome.COMMITTED) {
            committed = true;
          } else {
            String message = verification.outcome == MoveOutcome.UNCHANGED
                ? "snapshot atomic replacement failed; target is unchanged"
                : "snapshot atomic replacement failed; target outcome is unknown";
            primary = failure(
                LocalSnapshotException.Reason.IO,
                message,
                redactedCause("snapshot atomic replacement failed"));
            if (verification.suppressed != null) {
              primary.addSuppressed(verification.suppressed);
            }
          }
        } else {
          primary = failure(
              LocalSnapshotException.Reason.IO,
              "snapshot filesystem operation failed",
              redactedCause("snapshot filesystem operation failed"));
        }
        if (!committed) {
          throw primary;
        }
      } finally {
        cleanupFailed = cleanupTemporary(temporary, operations, primary);
        cleanupFailed =
            cleanupTemporary(priorTemporary, operations, primary) || cleanupFailed;
      }
      return cleanupFailed
          ? SaveResult.SUCCESS_WITH_CLEANUP_FAILURE
          : SaveResult.SUCCESS;
  }

  static LocalSnapshotState loadWithDeadline(
      Path source,
      HistoryMetricCatalog catalog,
      Duration maximumPlanningAge,
      LocalSnapshotDeadline deadline,
      FileOperations operations,
      GuardWaiter waiter) throws LocalSnapshotException {
    return loadAndPublishWithDeadline(
        source,
        catalog,
        maximumPlanningAge,
        deadline,
        operations,
        waiter,
        new StatePublisher<LocalSnapshotState>() {
          @Override
          public LocalSnapshotState publish(
              LocalSnapshotState state, LocalSnapshotDeadline ignored) {
            return state;
          }
        });
  }

  static <T> T loadAndPublishWithDeadline(
      Path source,
      HistoryMetricCatalog catalog,
      Duration maximumPlanningAge,
      LocalSnapshotDeadline deadline,
      FileOperations operations,
      GuardWaiter waiter,
      StatePublisher<T> publisher) throws LocalSnapshotException {
    Objects.requireNonNull(source, "source");
    validateReadArguments(
        catalog, maximumPlanningAge, deadline, operations, waiter);
    Objects.requireNonNull(publisher, "publisher");

    Path normalized = normalize(source, operations);
    try (GuardLease ignored = acquire(normalized, deadline, waiter)) {
      deadline.throwIfExpired();
      LocalSnapshotState state =
          readState(
              normalized,
              catalog,
              maximumPlanningAge,
              deadline,
              operations);
      deadline.throwIfExpired();
      return publisher.publish(state, deadline);
    } catch (LocalSnapshotException expected) {
      throw expected;
    } catch (SecurityException denied) {
      throw failure(
          LocalSnapshotException.Reason.IO,
          "snapshot filesystem access was denied",
          redactedCause("snapshot filesystem access was denied"));
    } catch (UnsupportedOperationException unsupported) {
      throw failure(
          LocalSnapshotException.Reason.IO,
          "snapshot filesystem operation is unsupported",
          redactedCause("snapshot filesystem operation is unsupported"));
    } catch (IOException io) {
      throw failure(
          LocalSnapshotException.Reason.IO,
          "snapshot filesystem operation failed",
          redactedCause("snapshot filesystem operation failed"));
    }
  }

  static final class SaveResult {
    private static final SaveResult SUCCESS = new SaveResult(false);
    private static final SaveResult SUCCESS_WITH_CLEANUP_FAILURE =
        new SaveResult(true);

    private final boolean cleanupFailed;

    private SaveResult(boolean cleanupFailed) {
      this.cleanupFailed = cleanupFailed;
    }

    boolean cleanupFailed() {
      return cleanupFailed;
    }
  }

  interface SnapshotStateCapture {
    LocalSnapshotState capture() throws LocalSnapshotException;
  }

  interface StatePublisher<T> {
    T publish(LocalSnapshotState state, LocalSnapshotDeadline deadline)
        throws LocalSnapshotException;
  }

  static GuardWaiter systemGuardWaiter() {
    return SYSTEM_GUARD_WAITER;
  }

  static int guardEntryCountForTest() {
    synchronized (GUARD_MAP_LOCK) {
      return PATH_GUARDS.size();
    }
  }

  private static LocalSnapshotState readState(
      Path source,
      HistoryMetricCatalog catalog,
      Duration maximumPlanningAge,
      LocalSnapshotDeadline deadline,
      FileOperations operations)
      throws IOException, LocalSnapshotException {
    long size = operations.size(source);
    deadline.throwIfExpired();
    if (size < 0L || size > LocalSnapshotCodec.MAX_FILE_BYTES) {
      throw failure(
          LocalSnapshotException.Reason.BOUNDS,
          "snapshot file size exceeds the current bound");
    }

    operations.requireRegularFile(source);
    deadline.throwIfExpired();
    LocalSnapshotState state;
    try (InputStream in = operations.open(source)) {
      state =
          LocalSnapshotCodec.decode(
              in, size, catalog, maximumPlanningAge);
    }
    deadline.throwIfExpired();
    return state;
  }

  private static PriorTarget probePriorTarget(
      Path target,
      LocalSnapshotDeadline deadline,
      FileOperations operations)
      throws IOException, LocalSnapshotException {
    deadline.throwIfExpired();
    long length;
    try {
      length = operations.size(target);
    } catch (NoSuchFileException missing) {
      return PriorTarget.absent();
    }
    deadline.throwIfExpired();
    if (length < 0L || length > LocalSnapshotCodec.MAX_FILE_BYTES) {
      return PriorTarget.unknown();
    }
    try {
      operations.requireRegularFile(target);
    } catch (NoSuchFileException missing) {
      return PriorTarget.absent();
    }
    deadline.throwIfExpired();
    return PriorTarget.present(null, length);
  }

  private static ExactMatch exactMatch(
      Path path,
      byte[] expected,
      LocalSnapshotDeadline deadline,
      FileOperations operations)
      throws IOException, LocalSnapshotException {
    deadline.throwIfExpired();
    long length;
    try {
      length = operations.size(path);
    } catch (NoSuchFileException missing) {
      return ExactMatch.MISSING;
    }
    deadline.throwIfExpired();
    if (length != expected.length) {
      return ExactMatch.DIFFERENT;
    }
    try {
      operations.requireRegularFile(path);
    } catch (NoSuchFileException missing) {
      return ExactMatch.MISSING;
    }
    deadline.throwIfExpired();

    InputStream opened;
    try {
      opened = operations.open(path);
    } catch (NoSuchFileException missing) {
      return ExactMatch.MISSING;
    }
    byte[] buffer = new byte[EXACT_BUFFER_BYTES];
    int offset = 0;
    try (InputStream in = opened) {
      while (offset < expected.length) {
        int requested = Math.min(buffer.length, expected.length - offset);
        readExactChunk(in, buffer, requested);
        for (int index = 0; index < requested; index++) {
          if (buffer[index] != expected[offset + index]) {
            return ExactMatch.DIFFERENT;
          }
        }
        offset += requested;
        deadline.throwIfExpired();
      }
      if (in.read() >= 0) {
        return ExactMatch.DIFFERENT;
      }
    }
    deadline.throwIfExpired();
    return ExactMatch.MATCH;
  }

  private static ExactMatch exactMatch(
      Path target,
      Path witness,
      long expectedLength,
      LocalSnapshotDeadline deadline,
      FileOperations operations)
      throws IOException, LocalSnapshotException {
    deadline.throwIfExpired();
    long targetLength;
    try {
      targetLength = operations.size(target);
    } catch (NoSuchFileException missing) {
      return ExactMatch.MISSING;
    }
    long witnessLength = operations.size(witness);
    deadline.throwIfExpired();
    if (targetLength != expectedLength || witnessLength != expectedLength) {
      return ExactMatch.DIFFERENT;
    }
    try {
      operations.requireRegularFile(target);
    } catch (NoSuchFileException missing) {
      return ExactMatch.MISSING;
    }
    operations.requireRegularFile(witness);
    deadline.throwIfExpired();

    InputStream openedTarget;
    try {
      openedTarget = operations.open(target);
    } catch (NoSuchFileException missing) {
      return ExactMatch.MISSING;
    }
    byte[] targetBuffer = new byte[EXACT_BUFFER_BYTES];
    byte[] witnessBuffer = new byte[EXACT_BUFFER_BYTES];
    long offset = 0L;
    try (InputStream targetIn = openedTarget;
        InputStream witnessIn = operations.open(witness)) {
      while (offset < expectedLength) {
        int requested =
            (int) Math.min((long) targetBuffer.length, expectedLength - offset);
        readExactChunk(targetIn, targetBuffer, requested);
        readExactChunk(witnessIn, witnessBuffer, requested);
        for (int index = 0; index < requested; index++) {
          if (targetBuffer[index] != witnessBuffer[index]) {
            return ExactMatch.DIFFERENT;
          }
        }
        offset += requested;
        deadline.throwIfExpired();
      }
      if (targetIn.read() >= 0 || witnessIn.read() >= 0) {
        return ExactMatch.DIFFERENT;
      }
    }
    deadline.throwIfExpired();
    return ExactMatch.MATCH;
  }

  private static void readExactChunk(
      InputStream in, byte[] buffer, int length) throws IOException {
    int offset = 0;
    while (offset < length) {
      int count = in.read(buffer, offset, length - offset);
      if (count < 0) {
        throw new IOException(
            "snapshot file changed during bounded exact comparison");
      }
      if (count == 0) {
        int one = in.read();
        if (one < 0) {
          throw new IOException(
              "snapshot file changed during bounded exact comparison");
        }
        buffer[offset++] = (byte) one;
      } else {
        offset += count;
      }
    }
  }

  private static MoveVerification verifyMoveOutcome(
      Path target,
      byte[] intended,
      PriorTarget prior,
      LocalSnapshotDeadline deadline,
      FileOperations operations) {
    try {
      deadline.throwIfExpired();
      ExactMatch intendedMatch =
          exactMatch(target, intended, deadline, operations);
      if (intendedMatch == ExactMatch.MATCH) {
        return MoveVerification.of(MoveOutcome.COMMITTED);
      }
      if (prior != null && prior.kind == PriorKind.ABSENT &&
          intendedMatch == ExactMatch.MISSING) {
        return MoveVerification.of(MoveOutcome.UNCHANGED);
      }
      if (prior != null && prior.kind == PriorKind.PRESENT &&
          exactMatch(
              target,
              prior.witness,
              prior.length,
              deadline,
              operations) == ExactMatch.MATCH) {
        return MoveVerification.of(MoveOutcome.UNCHANGED);
      }
      deadline.throwIfExpired();
      return MoveVerification.of(MoveOutcome.UNKNOWN);
    } catch (LocalSnapshotException timeout) {
      return MoveVerification.unknownWithSuppressed(
          "snapshot target verification exceeded its monotonic budget");
    } catch (IOException | SecurityException | UnsupportedOperationException failure) {
      return MoveVerification.unknownWithSuppressed(
          "snapshot target verification failed");
    }
  }

  private static boolean cleanupTemporary(
      Path temporary,
      FileOperations operations,
      LocalSnapshotException primary) {
    if (temporary == null) {
      return false;
    }
    try {
      operations.deleteIfExists(temporary);
      return false;
    } catch (IOException | SecurityException | UnsupportedOperationException cleanup) {
      if (primary != null) {
        primary.addSuppressed(
            new IOException("snapshot temporary cleanup failed"));
      }
      return true;
    }
  }

  private static void validateReadArguments(
      HistoryMetricCatalog catalog,
      Duration maximumPlanningAge,
      LocalSnapshotDeadline deadline,
      FileOperations operations,
      GuardWaiter waiter) {
    Objects.requireNonNull(catalog, "catalog");
    Objects.requireNonNull(maximumPlanningAge, "maximumPlanningAge");
    if (maximumPlanningAge.isNegative()) {
      throw new IllegalArgumentException(
          "maximumPlanningAge must not be negative");
    }
    Objects.requireNonNull(deadline, "deadline");
    Objects.requireNonNull(operations, "operations");
    Objects.requireNonNull(waiter, "waiter");
  }

  private static Path normalize(Path path, FileOperations operations)
      throws LocalSnapshotException {
    try {
      return operations.normalize(path);
    } catch (SecurityException denied) {
      throw failure(
          LocalSnapshotException.Reason.IO,
          "snapshot path normalization was denied",
          redactedCause("snapshot path normalization was denied"));
    } catch (UnsupportedOperationException unsupported) {
      throw failure(
          LocalSnapshotException.Reason.IO,
          "snapshot path normalization is unsupported",
          redactedCause("snapshot path normalization is unsupported"));
    }
  }

  private static GuardLease acquire(
      Path path, LocalSnapshotDeadline deadline, GuardWaiter waiter)
      throws LocalSnapshotException {
    GuardEntry entry;
    synchronized (GUARD_MAP_LOCK) {
      entry = PATH_GUARDS.get(path);
      if (entry == null) {
        entry = new GuardEntry();
        PATH_GUARDS.put(path, entry);
      }
      entry.references++;
    }

    boolean acquired = false;
    entry.lock.lock();
    try {
      if (entry.held && deadline.isZero()) {
        throw failure(
            LocalSnapshotException.Reason.BUSY,
            "another snapshot operation currently owns this path");
      }
      while (entry.held) {
        long remaining = deadline.remainingNanos();
        if (remaining == 0L) {
          throw failure(
              LocalSnapshotException.Reason.TIMEOUT,
              "snapshot path coordination exceeded its monotonic budget");
        }
        long waitResult;
        try {
          waitResult = waiter.await(entry.released, remaining);
        } catch (InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          throw failure(
              LocalSnapshotException.Reason.IO,
              "snapshot path coordination was interrupted",
              redactedCause("snapshot path coordination was interrupted"));
        }
        deadline.throwIfExpired();
        if (waitResult <= 0L && entry.held) {
          throw failure(
              LocalSnapshotException.Reason.TIMEOUT,
              "snapshot path coordination exceeded its monotonic budget");
        }
      }
      entry.held = true;
      acquired = true;
      return new GuardLease(path, entry);
    } finally {
      entry.lock.unlock();
      if (!acquired) {
        releaseReference(path, entry);
      }
    }
  }

  private static void releaseReference(Path path, GuardEntry entry) {
    synchronized (GUARD_MAP_LOCK) {
      entry.references--;
      if (entry.references == 0) {
        PATH_GUARDS.remove(path, entry);
      }
    }
  }

  private static LocalSnapshotException failure(
      LocalSnapshotException.Reason reason, String message) {
    return new LocalSnapshotException(reason, message);
  }

  private static LocalSnapshotException failure(
      LocalSnapshotException.Reason reason,
      String message,
      Throwable cause) {
    return new LocalSnapshotException(reason, message, cause);
  }

  private static IOException redactedCause(String message) {
    return new IOException(message);
  }

  interface GuardWaiter {
    long await(Condition condition, long remainingNanos)
        throws InterruptedException;
  }

  interface FileOperations {
    Path normalize(Path path);

    void requireDirectory(Path parent) throws IOException;

    void requireRegularFile(Path path) throws IOException;

    Path createTemp(Path parent, String prefix, String suffix)
        throws IOException;

    void writeAndSync(Path path, byte[] image) throws IOException;

    void copyAndSync(
        Path source,
        Path destination,
        long expectedLength,
        LocalSnapshotDeadline deadline)
        throws IOException, LocalSnapshotException;

    long size(Path path) throws IOException;

    InputStream open(Path path) throws IOException;

    Path move(Path source, Path destination, CopyOption... options)
        throws IOException;

    boolean deleteIfExists(Path path) throws IOException;
  }

  private enum MoveOutcome {
    COMMITTED,
    UNCHANGED,
    UNKNOWN
  }

  private static final class MoveVerification {
    private final MoveOutcome outcome;
    private final IOException suppressed;

    private MoveVerification(MoveOutcome outcome, IOException suppressed) {
      this.outcome = outcome;
      this.suppressed = suppressed;
    }

    private static MoveVerification of(MoveOutcome outcome) {
      return new MoveVerification(outcome, null);
    }

    private static MoveVerification unknownWithSuppressed(String diagnostic) {
      return new MoveVerification(
          MoveOutcome.UNKNOWN, new IOException(diagnostic));
    }
  }

  private enum ExactMatch {
    MATCH,
    DIFFERENT,
    MISSING
  }

  private enum PriorKind {
    ABSENT,
    PRESENT,
    UNKNOWN
  }

  private static final class PriorTarget {
    private final PriorKind kind;
    private final Path witness;
    private final long length;

    private PriorTarget(PriorKind kind, Path witness, long length) {
      this.kind = kind;
      this.witness = witness;
      this.length = length;
    }

    private static PriorTarget absent() {
      return new PriorTarget(PriorKind.ABSENT, null, 0L);
    }

    private static PriorTarget unknown() {
      return new PriorTarget(PriorKind.UNKNOWN, null, 0L);
    }

    private static PriorTarget present(Path witness, long length) {
      return new PriorTarget(PriorKind.PRESENT, witness, length);
    }
  }

  private static final class GuardEntry {
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition released = lock.newCondition();
    private int references;
    private boolean held;
  }

  private static final class GuardLease implements AutoCloseable {
    private final Path path;
    private final GuardEntry entry;
    private boolean closed;

    private GuardLease(Path path, GuardEntry entry) {
      this.path = path;
      this.entry = entry;
    }

    @Override
    public void close() {
      boolean release = false;
      entry.lock.lock();
      try {
        if (!closed) {
          closed = true;
          entry.held = false;
          entry.released.signalAll();
          release = true;
        }
      } finally {
        entry.lock.unlock();
      }
      if (release) {
        releaseReference(path, entry);
      }
    }
  }

  private static final class SystemFileOperations implements FileOperations {
    @Override
    public Path normalize(Path path) {
      return path.toAbsolutePath().normalize();
    }

    @Override
    public void requireDirectory(Path parent) throws IOException {
      BasicFileAttributes attributes =
          Files.readAttributes(parent, BasicFileAttributes.class);
      if (!attributes.isDirectory()) {
        throw new IOException("snapshot parent is not a directory");
      }
    }

    @Override
    public void requireRegularFile(Path path) throws IOException {
      BasicFileAttributes attributes =
          Files.readAttributes(path, BasicFileAttributes.class);
      if (!attributes.isRegularFile()) {
        throw new IOException("snapshot input is not a regular file");
      }
    }

    @Override
    public Path createTemp(Path parent, String prefix, String suffix)
        throws IOException {
      return Files.createTempFile(parent, prefix, suffix);
    }

    @Override
    public void writeAndSync(Path path, byte[] image) throws IOException {
      try (FileOutputStream out = new FileOutputStream(path.toFile())) {
        out.write(image);
        out.flush();
        out.getFD().sync();
      }
    }

    @Override
    public void copyAndSync(
        Path source,
        Path destination,
        long expectedLength,
        LocalSnapshotDeadline deadline)
        throws IOException, LocalSnapshotException {
      byte[] buffer = new byte[EXACT_BUFFER_BYTES];
      long copied = 0L;
      // This narrows the blocking-open risk; a path can still change after its attributes are read.
      requireRegularFile(source);
      try (InputStream in = Files.newInputStream(source);
          FileOutputStream out = new FileOutputStream(destination.toFile())) {
        while (copied < expectedLength) {
          int requested =
              (int) Math.min((long) buffer.length, expectedLength - copied);
          readExactChunk(in, buffer, requested);
          out.write(buffer, 0, requested);
          copied += requested;
          deadline.throwIfExpired();
        }
        if (in.read() >= 0) {
          throw new IOException(
              "snapshot prior target changed during bounded witness copy");
        }
        out.flush();
        out.getFD().sync();
      }
    }

    @Override
    public long size(Path path) throws IOException {
      return Files.size(path);
    }

    @Override
    public InputStream open(Path path) throws IOException {
      // This narrows the blocking-open risk; a path can still change after its attributes are read.
      requireRegularFile(path);
      return Files.newInputStream(path);
    }

    @Override
    public Path move(
        Path source, Path destination, CopyOption... options)
        throws IOException {
      return Files.move(source, destination, options);
    }

    @Override
    public boolean deleteIfExists(Path path) throws IOException {
      return Files.deleteIfExists(path);
    }
  }
}
