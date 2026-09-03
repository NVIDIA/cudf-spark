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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.zip.CRC32;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.nvidia.spark.history.HistoryMetricCatalog;
import com.nvidia.spark.history.LocalTestCatalog;

class LocalSnapshotFilesTest {
  private static final HistoryMetricCatalog CATALOG =
      LocalTestCatalog.builder().addLive(1, "metric-one").build();
  private static final Duration MAXIMUM_AGE = Duration.ofDays(1);
  private static final Duration TIMEOUT = Duration.ofSeconds(5);

  private Path directory;

  @BeforeEach
  void createDirectory() throws Exception {
    directory = Files.createTempDirectory("history-snapshot-files-");
  }

  @AfterEach
  void removeDirectory() throws Exception {
    if (directory != null && Files.exists(directory)) {
      try (Stream<Path> paths = Files.walk(directory)) {
        paths.sorted(Comparator.reverseOrder()).forEach(path -> {
          try {
            Files.deleteIfExists(path);
          } catch (IOException cleanup) {
            throw new CleanupFailure(cleanup);
          }
        });
      } catch (CleanupFailure failure) {
        throw failure.cause;
      }
    }
    assertEquals(0, LocalSnapshotFiles.guardEntryCountForTest());
  }

  @Test
  void saveSyncsValidatesAndAtomicallyPublishesThenLoadRoundTrips() throws Exception {
    RecordingFileOperations operations = new RecordingFileOperations();
    Path target = directory.resolve("history.bin");

    LocalSnapshotFiles.saveForTest(
        target, state(), CATALOG, MAXIMUM_AGE, TIMEOUT,
        new MutableTicker(), operations, LocalSnapshotFiles.systemGuardWaiter());
    LocalSnapshotState loaded = LocalSnapshotFiles.loadForTest(
        target, CATALOG, MAXIMUM_AGE, TIMEOUT,
        new MutableTicker(), operations, LocalSnapshotFiles.systemGuardWaiter());

    assertEquals(state(), loaded);
    assertEquals(1, operations.writeAndSyncCalls);
    assertEquals(1, operations.moveCalls);
    assertTrue(operations.moveOptions.contains(StandardCopyOption.ATOMIC_MOVE));
    assertTrue(operations.moveOptions.contains(StandardCopyOption.REPLACE_EXISTING));
    assertEquals(3, operations.openCalls);
    assertTrue(Files.exists(target));
    assertFalse(Files.exists(operations.temporaryPaths.get(0)));
  }

  @Test
  void sequentialSavesUseUniqueSiblingTempsAndLeaveNoGuardEntries() throws Exception {
    RecordingFileOperations operations = new RecordingFileOperations();
    Path target = directory.resolve("history.bin");

    save(target, operations);
    save(target, operations);

    assertEquals(3, operations.temporaryPaths.size());
    assertNotEquals(operations.temporaryPaths.get(0), operations.temporaryPaths.get(1));
    assertNotEquals(operations.temporaryPaths.get(0), operations.temporaryPaths.get(2));
    assertNotEquals(operations.temporaryPaths.get(1), operations.temporaryPaths.get(2));
    for (Path temporary : operations.temporaryPaths) {
      assertEquals(directory, temporary.getParent());
      assertFalse(Files.exists(temporary));
    }
    assertEquals(0, LocalSnapshotFiles.guardEntryCountForTest());
  }

  @Test
  void heldPathIsBusyOnlyAtZeroAndExpiredPositiveIsTimeout() throws Exception {
    BlockingWriteOperations operations = new BlockingWriteOperations();
    Path target = directory.resolve("history.bin");
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      Future<?> first = executor.submit(() -> {
        save(target, operations);
        return null;
      });
      assertTrue(operations.writeEntered.await(5, TimeUnit.SECONDS));

      Path lexicalAlias = directory.resolve(".").resolve("history.bin");
      assertReason(LocalSnapshotException.Reason.BUSY,
          () -> LocalSnapshotFiles.saveForTest(
              lexicalAlias, state(), CATALOG, MAXIMUM_AGE, Duration.ZERO,
              new MutableTicker(), new RecordingFileOperations(),
              LocalSnapshotFiles.systemGuardWaiter()));
      assertReason(LocalSnapshotException.Reason.BUSY,
          () -> LocalSnapshotFiles.loadForTest(
              lexicalAlias, CATALOG, MAXIMUM_AGE, Duration.ZERO,
              new MutableTicker(), new RecordingFileOperations(),
              LocalSnapshotFiles.systemGuardWaiter()));
      assertReason(LocalSnapshotException.Reason.TIMEOUT,
          () -> LocalSnapshotFiles.saveForTest(
              lexicalAlias, state(), CATALOG, MAXIMUM_AGE, Duration.ofNanos(1),
              new ScriptedTicker(0L, 1L), new RecordingFileOperations(),
              LocalSnapshotFiles.systemGuardWaiter()));

      operations.releaseWrite.countDown();
      first.get(5, TimeUnit.SECONDS);
      save(lexicalAlias, new RecordingFileOperations());
      Path freeTarget = directory.resolve("free.bin");
      assertReason(LocalSnapshotException.Reason.TIMEOUT,
          () -> LocalSnapshotFiles.saveForTest(
              freeTarget, state(), CATALOG, MAXIMUM_AGE, Duration.ZERO,
              new ScriptedTicker(0L, 1L), new RecordingFileOperations(),
              LocalSnapshotFiles.systemGuardWaiter()));
      assertFalse(Files.exists(freeTarget));
      assertEquals(0, LocalSnapshotFiles.guardEntryCountForTest());
    } finally {
      operations.releaseWrite.countDown();
      executor.shutdownNow();
    }
  }

  @Test
  void positiveSamePathGuardExpiryIsTimeoutWithoutSleep() throws Exception {
    BlockingWriteOperations operations = new BlockingWriteOperations();
    Path target = directory.resolve("history.bin");
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      Future<?> first = executor.submit(() -> {
        save(target, operations);
        return null;
      });
      assertTrue(operations.writeEntered.await(5, TimeUnit.SECONDS));

      MutableTicker ticker = new MutableTicker();
      LocalSnapshotFiles.GuardWaiter expiringWaiter =
          new LocalSnapshotFiles.GuardWaiter() {
            @Override
            public long await(Condition condition, long remainingNanos) {
              ticker.advance(remainingNanos + 1);
              return 0L;
            }
          };
      assertReason(LocalSnapshotException.Reason.TIMEOUT,
          () -> LocalSnapshotFiles.saveForTest(
              target, state(), CATALOG, MAXIMUM_AGE, Duration.ofNanos(5),
              ticker, new RecordingFileOperations(), expiringWaiter));
      operations.releaseWrite.countDown();
      first.get(5, TimeUnit.SECONDS);
    } finally {
      operations.releaseWrite.countDown();
      executor.shutdownNow();
    }
  }

  @Test
  void distinctNormalizedPathsDoNotShareGuard() throws Exception {
    BlockingWriteOperations operations = new BlockingWriteOperations();
    Path firstTarget = directory.resolve("first.bin");
    Path secondTarget = directory.resolve("second.bin");
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      Future<?> first = executor.submit(() -> {
        save(firstTarget, operations);
        return null;
      });
      assertTrue(operations.writeEntered.await(5, TimeUnit.SECONDS));

      save(secondTarget, new RecordingFileOperations());
      assertTrue(Files.exists(secondTarget));

      operations.releaseWrite.countDown();
      first.get(5, TimeUnit.SECONDS);
    } finally {
      operations.releaseWrite.countDown();
      executor.shutdownNow();
    }
  }

  @Test
  void missingParentAndSourceAreCheckedIoAndNoDirectoryIsCreated() throws Exception {
    Path missingParent = directory.resolve("missing").resolve("history.bin");
    assertReason(LocalSnapshotException.Reason.IO,
        () -> LocalSnapshotFiles.save(
            missingParent, state(), CATALOG, MAXIMUM_AGE, TIMEOUT));
    assertFalse(Files.exists(missingParent.getParent()));

    assertReason(LocalSnapshotException.Reason.IO,
        () -> LocalSnapshotFiles.load(
            directory.resolve("missing.bin"), CATALOG, MAXIMUM_AGE, TIMEOUT));
  }

  @Test
  void preMoveTimeoutPreservesOldTargetAndCleansTemp() throws Exception {
    Path target = directory.resolve("history.bin");
    byte[] old = new byte[] {9, 8, 7};
    Files.write(target, old);
    MutableTicker ticker = new MutableTicker();
    AdvancingWriteOperations operations =
        new AdvancingWriteOperations(ticker, 10L);

    assertReason(LocalSnapshotException.Reason.TIMEOUT,
        () -> LocalSnapshotFiles.saveForTest(
            target, state(), CATALOG, MAXIMUM_AGE, Duration.ofNanos(5),
            ticker, operations, LocalSnapshotFiles.systemGuardWaiter()));

    assertArrayEquals(old, Files.readAllBytes(target));
    assertEquals(0, operations.moveCalls);
    assertEquals(1, operations.deleteCalls);
    assertFalse(Files.exists(operations.temporaryPaths.get(0)));
  }

  @Test
  void nonRegularExistingSourceAndPriorTargetFailBeforeAnyBlockingOpen() throws Exception {
    Path source = directory.resolve("non-regular-source");
    Files.write(source, LocalSnapshotCodec.encode(state()));
    NonRegularPathOperations loadOperations = new NonRegularPathOperations(source);

    assertReason(LocalSnapshotException.Reason.IO,
        () -> LocalSnapshotFiles.loadForTest(
            source, CATALOG, MAXIMUM_AGE, TIMEOUT,
            new MutableTicker(), loadOperations, LocalSnapshotFiles.systemGuardWaiter()));
    assertEquals(1, loadOperations.regularFileChecks);
    assertEquals(0, loadOperations.forbiddenOpenCalls);

    Path target = directory.resolve("non-regular-prior-target");
    Files.write(target, new byte[] {1, 2, 3});
    NonRegularPathOperations saveOperations = new NonRegularPathOperations(target);

    assertReason(LocalSnapshotException.Reason.IO,
        () -> LocalSnapshotFiles.saveForTest(
            target, state(), CATALOG, MAXIMUM_AGE, TIMEOUT,
            new MutableTicker(), saveOperations, LocalSnapshotFiles.systemGuardWaiter()));
    assertEquals(1, saveOperations.rejectedPathChecks);
    assertEquals(0, saveOperations.forbiddenOpenCalls);
    assertEquals(0, saveOperations.copyAndSyncCalls);
    assertArrayEquals(new byte[] {1, 2, 3}, Files.readAllBytes(target));
  }

  @Test
  void loadTimeoutAfterSizeCheckDoesNotOpenOrAllocatePayload() throws Exception {
    Path source = directory.resolve("history.bin");
    Files.write(source, LocalSnapshotCodec.encode(state()));
    MutableTicker ticker = new MutableTicker();
    RecordingFileOperations operations = new RecordingFileOperations() {
      @Override
      public long size(Path path) throws IOException {
        long size = super.size(path);
        ticker.advance(10L);
        return size;
      }
    };

    assertReason(LocalSnapshotException.Reason.TIMEOUT,
        () -> LocalSnapshotFiles.loadForTest(
            source, CATALOG, MAXIMUM_AGE, Duration.ofNanos(5),
            ticker, operations, LocalSnapshotFiles.systemGuardWaiter()));
    assertEquals(0, operations.openCalls);
  }

  @Test
  void atomicMoveUnsupportedPreservesOldTargetAndCleansTemp() throws Exception {
    Path target = directory.resolve("history.bin");
    byte[] old = new byte[] {1, 2, 3};
    Files.write(target, old);
    RecordingFileOperations operations = new RecordingFileOperations() {
      @Override
      public Path move(Path source, Path destination, CopyOption... options)
          throws IOException {
        moveCalls++;
        moveOptions = Arrays.asList(options);
        throw new AtomicMoveNotSupportedException(
            source.toString(), destination.toString(), "not supported");
      }
    };

    assertReason(LocalSnapshotException.Reason.ATOMIC_MOVE_UNSUPPORTED,
        () -> save(target, operations));

    assertArrayEquals(old, Files.readAllBytes(target));
    assertEquals(2, operations.deleteCalls);
  }

  @Test
  void genericMoveIoThatCommittedTheIntendedImageReturnsSuccess() throws Exception {
    Path target = directory.resolve("history.bin");
    Files.write(target, new byte[] {4});
    RecordingFileOperations operations = new RecordingFileOperations() {
      @Override
      public Path move(Path source, Path destination, CopyOption... options)
          throws IOException {
        super.move(source, destination, options);
        throw new IOException("injected move failure after commit");
      }
    };

    save(target, operations);

    assertEquals(state(), LocalSnapshotFiles.load(
        target, CATALOG, MAXIMUM_AGE, TIMEOUT));
    assertEquals(1, operations.moveCalls);
  }

  @Test
  void priorImageEqualToIntendedIsSuccessAndBothTempsAreCleaned() throws Exception {
    Path target = directory.resolve("history.bin");
    byte[] intended = LocalSnapshotCodec.encode(state());
    Files.write(target, intended);
    RecordingFileOperations operations = new RecordingFileOperations() {
      @Override
      public Path move(Path source, Path destination, CopyOption... options)
          throws IOException {
        moveCalls++;
        throw new IOException("injected overlap move failure");
      }
    };

    save(target, operations);

    assertArrayEquals(intended, Files.readAllBytes(target));
    assertEquals(2, operations.temporaryPaths.size());
    assertEquals(1, operations.copyAndSyncCalls);
    assertEquals(2, operations.deleteCalls);
    for (Path temporary : operations.temporaryPaths) {
      assertFalse(Files.exists(temporary));
    }
  }

  @Test
  void genericMoveIoThatPreservedPriorImageIsKnownUncommitted() throws Exception {
    Path target = directory.resolve("history.bin");
    byte[] old = new byte[] {4, 5, 6};
    Files.write(target, old);
    RecordingFileOperations operations = new RecordingFileOperations() {
      @Override
      public Path move(Path source, Path destination, CopyOption... options)
          throws IOException {
        moveCalls++;
        throw new IOException("injected unchanged move failure");
      }

      @Override
      public boolean deleteIfExists(Path path) throws IOException {
        deleteCalls++;
        throw new IOException("sensitive cleanup failure");
      }
    };

    LocalSnapshotException failure = assertReason(
        LocalSnapshotException.Reason.IO, () -> save(target, operations));

    assertTrue(failure.getMessage().contains("unchanged"));
    assertArrayEquals(old, Files.readAllBytes(target));
    assertEquals(2, operations.deleteCalls);
    assertEquals(2, failure.getSuppressed().length);
    for (Throwable suppressed : failure.getSuppressed()) {
      assertFalse(suppressed.getMessage().contains("sensitive"));
    }
  }

  @Test
  void genericMoveIoRecognizesAnAbsentPriorTargetAsUnchanged() throws Exception {
    Path target = directory.resolve("history.bin");
    RecordingFileOperations operations = new RecordingFileOperations() {
      @Override
      public Path move(Path source, Path destination, CopyOption... options)
          throws IOException {
        moveCalls++;
        throw new IOException("injected absent-target move failure");
      }
    };

    LocalSnapshotException failure = assertReason(
        LocalSnapshotException.Reason.IO, () -> save(target, operations));

    assertTrue(failure.getMessage().contains("unchanged"));
    assertFalse(Files.exists(target));
  }

  @Test
  void sameLengthOneByteDifferenceNeverCountsAsCommitted() throws Exception {
    Path target = directory.resolve("history.bin");
    Files.write(target, new byte[] {4});
    byte[] different = LocalSnapshotCodec.encode(state());
    different[different.length - 1] ^= 1;
    RecordingFileOperations operations = new RecordingFileOperations() {
      @Override
      public Path move(Path source, Path destination, CopyOption... options)
          throws IOException {
        moveCalls++;
        Files.write(destination, different);
        throw new IOException("injected ambiguous move failure");
      }
    };

    LocalSnapshotException failure = assertReason(
        LocalSnapshotException.Reason.IO, () -> save(target, operations));

    assertTrue(failure.getMessage().contains("unknown"));
    assertArrayEquals(different, Files.readAllBytes(target));
  }

  @Test
  void moveVerificationFailureIsSuppressedAndOriginalMoveFailureWins() throws Exception {
    Path target = directory.resolve("history.bin");
    Files.write(target, new byte[] {4});
    RecordingFileOperations operations = new RecordingFileOperations() {
      private int targetSizeCalls;

      @Override
      public long size(Path path) throws IOException {
        if (path.equals(target) && ++targetSizeCalls == 2) {
          throw new IOException("sensitive verification failure");
        }
        return super.size(path);
      }

      @Override
      public Path move(Path source, Path destination, CopyOption... options)
          throws IOException {
        moveCalls++;
        throw new IOException("sensitive original move failure");
      }
    };

    LocalSnapshotException failure = assertReason(
        LocalSnapshotException.Reason.IO, () -> save(target, operations));

    assertTrue(failure.getMessage().contains("unknown"));
    assertEquals(1, failure.getSuppressed().length);
    assertFalse(failure.getCause().getMessage().contains("sensitive"));
    assertFalse(failure.getSuppressed()[0].getMessage().contains("sensitive"));
    assertArrayEquals(new byte[] {4}, Files.readAllBytes(target));
  }

  @Test
  void moveVerificationTimeoutIsSuppressedAndDoesNotReplaceOriginalIo()
      throws Exception {
    Path target = directory.resolve("history.bin");
    Files.write(target, new byte[] {4});
    MutableTicker ticker = new MutableTicker();
    RecordingFileOperations operations = new RecordingFileOperations() {
      @Override
      public Path move(Path source, Path destination, CopyOption... options)
          throws IOException {
        moveCalls++;
        ticker.advance(10L);
        throw new IOException("sensitive original move failure");
      }
    };

    LocalSnapshotException failure = assertReason(
        LocalSnapshotException.Reason.IO,
        () -> LocalSnapshotFiles.saveForTest(
            target, state(), CATALOG, MAXIMUM_AGE, Duration.ofNanos(5),
            ticker, operations, LocalSnapshotFiles.systemGuardWaiter()));

    assertTrue(failure.getMessage().contains("unknown"));
    assertEquals(1, failure.getSuppressed().length);
    assertFalse(failure.getSuppressed()[0].getMessage().contains("sensitive"));
    assertArrayEquals(new byte[] {4}, Files.readAllBytes(target));
  }

  @Test
  void successfulAtomicMoveIsCommitEvenWhenTickerAdvancesDuringMove() throws Exception {
    Path target = directory.resolve("history.bin");
    MutableTicker ticker = new MutableTicker();
    RecordingFileOperations operations = new RecordingFileOperations() {
      @Override
      public Path move(Path source, Path destination, CopyOption... options)
          throws IOException {
        Path moved = super.move(source, destination, options);
        ticker.advance(10L);
        return moved;
      }
    };

    LocalSnapshotFiles.saveForTest(
        target, state(), CATALOG, MAXIMUM_AGE, Duration.ofNanos(5),
        ticker, operations, LocalSnapshotFiles.systemGuardWaiter());

    assertTrue(Files.exists(target));
    assertEquals(state(), LocalSnapshotFiles.load(
        target, CATALOG, MAXIMUM_AGE, TIMEOUT));
  }

  @Test
  void validationFailureKeepsPrimaryReasonAndCleanupFailureIsRedacted()
      throws Exception {
    Path target = directory.resolve("history.bin");
    byte[] old = new byte[] {6, 6};
    Files.write(target, old);
    RecordingFileOperations operations = new RecordingFileOperations() {
      @Override
      public void writeAndSync(Path path, byte[] image) throws IOException {
        writeAndSyncCalls++;
        Files.write(path, new byte[] {1, 2, 3});
      }

      @Override
      public boolean deleteIfExists(Path path) throws IOException {
        deleteCalls++;
        throw new IOException("sensitive cleanup detail");
      }
    };

    LocalSnapshotException failure = assertReason(
        LocalSnapshotException.Reason.FORMAT, () -> save(target, operations));
    assertEquals(1, failure.getSuppressed().length);
    assertEquals("snapshot temporary cleanup failed",
        failure.getSuppressed()[0].getMessage());
    assertFalse(failure.getSuppressed()[0].getMessage().contains("sensitive"));
    assertArrayEquals(old, Files.readAllBytes(target));
    assertTrue(Files.exists(operations.temporaryPaths.get(0)));
  }

  @Test
  void loadAcceptsTheTotalMaximumButRejectsMaximumPlusOneBeforeOpening() throws Exception {
    Path source = directory.resolve("small.bin");
    Files.write(source, new byte[] {1});
    RecordingFileOperations overLimit = new RecordingFileOperations() {
      @Override
      public long size(Path path) {
        return (long) LocalSnapshotCodec.MAX_FILE_BYTES + 1;
      }
    };

    assertReason(LocalSnapshotException.Reason.BOUNDS,
        () -> LocalSnapshotFiles.loadForTest(
            source, CATALOG, MAXIMUM_AGE, TIMEOUT,
            new MutableTicker(), overLimit, LocalSnapshotFiles.systemGuardWaiter()));
    assertEquals(0, overLimit.openCalls);

    RecordingFileOperations atLimit = new RecordingFileOperations() {
      @Override
      public long size(Path path) {
        return LocalSnapshotCodec.MAX_FILE_BYTES;
      }
    };
    assertReason(LocalSnapshotException.Reason.FORMAT,
        () -> LocalSnapshotFiles.loadForTest(
            source, CATALOG, MAXIMUM_AGE, TIMEOUT,
            new MutableTicker(), atLimit, LocalSnapshotFiles.systemGuardWaiter()));
    assertEquals(1, atLimit.openCalls);
  }

  @Test
  void fileLoadPreservesSectionRemainingAndPhysicalTruncationReasons()
      throws Exception {
    Path source = directory.resolve("invalid.bin");
    Files.write(source, imageWithOuterSectionBeyondRemaining());
    assertReason(LocalSnapshotException.Reason.BOUNDS,
        () -> LocalSnapshotFiles.load(
            source, CATALOG, MAXIMUM_AGE, TIMEOUT));

    Files.write(source, imageWithTruncatedCatalogString());
    assertReason(LocalSnapshotException.Reason.FORMAT,
        () -> LocalSnapshotFiles.load(
            source, CATALOG, MAXIMUM_AGE, TIMEOUT));
  }

  @Test
  void operationalSecurityAndUnsupportedMappingsAreStable() throws Exception {
    Path target = directory.resolve("history.bin");
    RecordingFileOperations security = new RecordingFileOperations() {
      @Override
      public void requireDirectory(Path parent) {
        throw new SecurityException("sensitive path");
      }
    };
    LocalSnapshotException denied = assertReason(
        LocalSnapshotException.Reason.IO, () -> save(target, security));
    assertFalse(denied.getCause().getMessage().contains("sensitive"));

    RecordingFileOperations unsupportedCreate = new RecordingFileOperations() {
      @Override
      public Path createTemp(Path parent, String prefix, String suffix) {
        throw new UnsupportedOperationException("unsupported");
      }
    };
    assertReason(LocalSnapshotException.Reason.IO,
        () -> save(target, unsupportedCreate));

    RecordingFileOperations unsupportedMove = new RecordingFileOperations() {
      @Override
      public Path move(Path source, Path destination, CopyOption... options) {
        throw new UnsupportedOperationException("unsupported");
      }
    };
    assertReason(LocalSnapshotException.Reason.ATOMIC_MOVE_UNSUPPORTED,
        () -> save(target, unsupportedMove));

    RecordingFileOperations deniedLoad = new RecordingFileOperations() {
      @Override
      public long size(Path path) {
        throw new SecurityException("sensitive source");
      }
    };
    assertReason(LocalSnapshotException.Reason.IO,
        () -> LocalSnapshotFiles.loadForTest(
            target, CATALOG, MAXIMUM_AGE, TIMEOUT,
            new MutableTicker(), deniedLoad, LocalSnapshotFiles.systemGuardWaiter()));

    RecordingFileOperations unsupportedLoad = new RecordingFileOperations() {
      @Override
      public long size(Path path) {
        throw new UnsupportedOperationException("unsupported");
      }
    };
    assertReason(LocalSnapshotException.Reason.IO,
        () -> LocalSnapshotFiles.loadForTest(
            target, CATALOG, MAXIMUM_AGE, TIMEOUT,
            new MutableTicker(), unsupportedLoad, LocalSnapshotFiles.systemGuardWaiter()));
  }

  private static LocalSnapshotState state() {
    return LocalSnapshotState.capture(
        CATALOG,
        Collections.<LocalDeclarationSnapshot>emptyList(),
        Collections.<LocalObservationSnapshot>emptyList(),
        1L);
  }

  private static void save(Path target, RecordingFileOperations operations)
      throws Exception {
    LocalSnapshotFiles.saveForTest(
        target, state(), CATALOG, MAXIMUM_AGE, TIMEOUT,
        new MutableTicker(), operations, LocalSnapshotFiles.systemGuardWaiter());
  }

  private static byte[] imageWithOuterSectionBeyondRemaining() throws Exception {
    ByteArrayOutputStream payload = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(payload);
    out.writeInt(100);
    out.writeInt(0);
    out.writeInt(0);
    out.writeInt(0);
    return frame(payload.toByteArray());
  }

  private static byte[] imageWithTruncatedCatalogString() throws Exception {
    byte[] catalog = bytes(out -> {
      out.writeInt(1);
      out.writeInt(1);
      out.writeInt(4);
      out.writeByte('a');
      out.writeByte('b');
    });
    ByteArrayOutputStream payload = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(payload);
    writeSection(out, catalog);
    writeSection(out, bytes(data -> data.writeInt(0)));
    writeSection(out, bytes(data -> data.writeInt(0)));
    writeSection(out, bytes(data -> data.writeLong(1L)));
    return frame(payload.toByteArray());
  }

  private static byte[] frame(byte[] payload) throws Exception {
    ByteArrayOutputStream image = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(image);
    out.writeInt(LocalSnapshotCodec.MAGIC);
    out.writeShort(LocalSnapshotCodec.FORMAT_MAJOR);
    out.writeShort(LocalSnapshotCodec.FORMAT_MINOR);
    out.writeInt(1);
    out.writeInt(0);
    out.writeInt(payload.length);
    out.write(payload);
    CRC32 crc = new CRC32();
    crc.update(payload);
    out.writeInt((int) crc.getValue());
    return image.toByteArray();
  }

  private static void writeSection(DataOutputStream out, byte[] section)
      throws IOException {
    out.writeInt(section.length);
    out.write(section);
  }

  private static byte[] bytes(IoWriter writer) throws Exception {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(bytes);
    writer.write(out);
    return bytes.toByteArray();
  }

  private static LocalSnapshotException assertReason(
      LocalSnapshotException.Reason reason, ThrowingCall call) {
    LocalSnapshotException failure =
        assertThrows(LocalSnapshotException.class, call::run);
    assertEquals(reason, failure.reason());
    assertFalse(failure.getMessage().isEmpty());
    return failure;
  }

  private interface IoWriter {
    void write(DataOutputStream out) throws Exception;
  }

  private interface ThrowingCall {
    void run() throws Exception;
  }

  private static final class MutableTicker
      implements LocalMetricStorePlanningAdapter.Ticker {
    private final AtomicLong nanos = new AtomicLong();

    @Override
    public long readNanos() {
      return nanos.get();
    }

    private void advance(long amount) {
      nanos.addAndGet(amount);
    }
  }

  private static final class ScriptedTicker
      implements LocalMetricStorePlanningAdapter.Ticker {
    private final long[] values;
    private int index;

    private ScriptedTicker(long... values) {
      this.values = values.clone();
    }

    @Override
    public long readNanos() {
      int current = index;
      if (index < values.length - 1) {
        index++;
      }
      return values[current];
    }
  }

  private static class RecordingFileOperations
      implements LocalSnapshotFiles.FileOperations {
    protected final List<Path> temporaryPaths = new ArrayList<Path>();
    protected List<CopyOption> moveOptions = Collections.emptyList();
    protected int writeAndSyncCalls;
    protected int copyAndSyncCalls;
    protected int moveCalls;
    protected int deleteCalls;
    protected int openCalls;

    @Override
    public Path normalize(Path path) {
      return path.toAbsolutePath().normalize();
    }

    @Override
    public void requireDirectory(Path parent) throws IOException {
      if (!Files.isDirectory(parent)) {
        throw new IOException("parent is missing");
      }
    }

    @Override
    public void requireRegularFile(Path path) throws IOException {
      if (!Files.isRegularFile(path)) {
        throw new IOException("snapshot input is not a regular file");
      }
    }

    @Override
    public Path createTemp(Path parent, String prefix, String suffix)
        throws IOException {
      Path temporary = Files.createTempFile(parent, prefix, suffix);
      temporaryPaths.add(temporary);
      return temporary;
    }

    @Override
    public void writeAndSync(Path path, byte[] image) throws IOException {
      writeAndSyncCalls++;
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
      copyAndSyncCalls++;
      byte[] buffer = new byte[8192];
      long copied = 0L;
      try (InputStream in = Files.newInputStream(source);
          FileOutputStream out = new FileOutputStream(destination.toFile())) {
        while (copied < expectedLength) {
          int requested =
              (int) Math.min((long) buffer.length, expectedLength - copied);
          int offset = 0;
          while (offset < requested) {
            int count = in.read(buffer, offset, requested - offset);
            if (count < 0) {
              throw new IOException("source changed during copy");
            }
            offset += count;
          }
          out.write(buffer, 0, requested);
          copied += requested;
          deadline.throwIfExpired();
        }
        if (in.read() >= 0) {
          throw new IOException("source changed during copy");
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
      openCalls++;
      return Files.newInputStream(path);
    }

    @Override
    public Path move(Path source, Path destination, CopyOption... options)
        throws IOException {
      moveCalls++;
      moveOptions = Arrays.asList(options);
      return Files.move(source, destination, options);
    }

    @Override
    public boolean deleteIfExists(Path path) throws IOException {
      deleteCalls++;
      return Files.deleteIfExists(path);
    }
  }

  private static final class NonRegularPathOperations
      extends RecordingFileOperations {
    private final Path nonRegularPath;
    private int regularFileChecks;
    private int rejectedPathChecks;
    private int forbiddenOpenCalls;

    private NonRegularPathOperations(Path nonRegularPath) {
      this.nonRegularPath = nonRegularPath.toAbsolutePath().normalize();
    }

    @Override
    public void requireRegularFile(Path path) throws IOException {
      regularFileChecks++;
      if (path.toAbsolutePath().normalize().equals(nonRegularPath)) {
        rejectedPathChecks++;
        throw new IOException("snapshot input is not a regular file");
      }
      if (!Files.isRegularFile(path)) {
        throw new IOException("snapshot input is not a regular file");
      }
    }

    @Override
    public InputStream open(Path path) throws IOException {
      if (path.toAbsolutePath().normalize().equals(nonRegularPath)) {
        forbiddenOpenCalls++;
        throw new AssertionError("non-regular path must not be opened");
      }
      return super.open(path);
    }

    @Override
    public void copyAndSync(
        Path source,
        Path destination,
        long expectedLength,
        LocalSnapshotDeadline deadline)
        throws IOException, LocalSnapshotException {
      if (source.toAbsolutePath().normalize().equals(nonRegularPath)) {
        forbiddenOpenCalls++;
        throw new AssertionError("non-regular path must not be opened");
      }
      super.copyAndSync(source, destination, expectedLength, deadline);
    }
  }

  private static final class BlockingWriteOperations
      extends RecordingFileOperations {
    private final CountDownLatch writeEntered = new CountDownLatch(1);
    private final CountDownLatch releaseWrite = new CountDownLatch(1);

    @Override
    public void writeAndSync(Path path, byte[] image) throws IOException {
      super.writeAndSync(path, image);
      writeEntered.countDown();
      try {
        releaseWrite.await();
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
        throw new IOException("interrupted", interrupted);
      }
    }
  }

  private static final class AdvancingWriteOperations
      extends RecordingFileOperations {
    private final MutableTicker ticker;
    private final long advance;

    private AdvancingWriteOperations(MutableTicker ticker, long advance) {
      this.ticker = ticker;
      this.advance = advance;
    }

    @Override
    public void writeAndSync(Path path, byte[] image) throws IOException {
      super.writeAndSync(path, image);
      ticker.advance(advance);
    }
  }

  private static final class CleanupFailure extends RuntimeException {
    private final IOException cause;

    private CleanupFailure(IOException cause) {
      this.cause = cause;
    }
  }
}
