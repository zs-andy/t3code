// @effect-diagnostics nodeBuiltinImport:off -- tests construct realistic Antigravity SQLite session fixtures.
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";
import * as NodeURL from "node:url";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import { symlinksSupported } from "@t3tools/shared/testing/symlinks";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";

import { resolveAntigravityDataRoots } from "./AntigravityDataRoots.ts";
import {
  extractAntigravityUserText,
  isAntigravitySessionId,
  listAntigravityNativeSessions,
  materializeAntigravitySession,
  scanAntigravityRootSessions,
} from "./AntigravityExternalSessions.ts";

const SESSION_A = "11111111-1111-4111-8111-111111111111";
const SESSION_B = "22222222-2222-4222-8222-222222222222";
const SESSION_SUBAGENT = "33333333-3333-4333-8333-333333333333";
const SESSION_PARENT_TABLE = "44444444-4444-4444-8444-444444444444";
const SESSION_NO_SUMMARY = "55555555-5555-4555-8555-555555555555";

const varint = (value: number): number[] => {
  const bytes: number[] = [];
  while (value >= 0x80) {
    bytes.push((value & 0x7f) | 0x80);
    value = Math.floor(value / 0x80);
  }
  return [...bytes, value];
};
const protoField = (number: number, value: string | Uint8Array) => {
  const bytes = typeof value === "string" ? new TextEncoder().encode(value) : value;
  return [...varint(number * 8 + 2), ...varint(bytes.length), ...bytes];
};
const proto = (...parts: number[][]) => Uint8Array.from(parts.flat());

function encodeTrajectoryMetadata(input: {
  readonly sessionId: string;
  readonly workspacePaths: ReadonlyArray<string>;
}) {
  const uris = input.workspacePaths.map(
    (workspacePath) => NodeURL.pathToFileURL(workspacePath).href,
  );
  return proto(
    ...uris.map((uri) => protoField(1, proto(protoField(1, uri)))),
    protoField(6, input.sessionId),
    ...uris.map((uri) => protoField(7, uri)),
  );
}

async function createConversationDb(
  rootDir: string,
  sessionId: string,
  workspacePaths: ReadonlyArray<string>,
  options?: { readonly walMode?: boolean; readonly extraMessage?: string },
): Promise<string> {
  const conversationsDir = NodePath.join(rootDir, "conversations");
  await NodeFSP.mkdir(conversationsDir, { recursive: true });
  const dbPath = NodePath.join(conversationsDir, `${sessionId}.db`);
  const db = new NodeSqlite.DatabaseSync(dbPath);
  try {
    if (options?.walMode) {
      db.exec("PRAGMA journal_mode = WAL;");
    }
    db.exec(
      "CREATE TABLE trajectory_metadata_blob (id TEXT PRIMARY KEY, data BLOB);" +
        "CREATE TABLE notes (id INTEGER PRIMARY KEY, body TEXT);",
    );
    db.prepare("INSERT INTO trajectory_metadata_blob (id, data) VALUES (?, ?)").run(
      "main",
      encodeTrajectoryMetadata({ sessionId, workspacePaths }),
    );
    db.prepare("INSERT INTO notes (body) VALUES (?)").run(options?.extraMessage ?? "initial");
  } finally {
    db.close();
  }
  return dbPath;
}

async function writeSummaryDb(
  rootDir: string,
  rows: ReadonlyArray<{
    readonly conversationId: string;
    readonly title?: string | null;
    readonly preview?: string | null;
    readonly lastModifiedTime?: string | null;
    readonly workspacePaths?: ReadonlyArray<string>;
    readonly parentConversationId?: string | null;
    readonly nestingDepth?: number;
  }>,
  parentTableIds: ReadonlyArray<string> = [],
): Promise<string> {
  await NodeFSP.mkdir(rootDir, { recursive: true });
  const dbPath = NodePath.join(rootDir, "conversation_summaries.db");
  const db = new NodeSqlite.DatabaseSync(dbPath);
  try {
    db.exec(`
      CREATE TABLE conversation_summaries (
        conversation_id TEXT PRIMARY KEY,
        title TEXT,
        preview TEXT,
        last_modified_time TEXT,
        workspace_uris TEXT,
        parent_conversation_id TEXT,
        nesting_depth INTEGER
      );
      CREATE TABLE conversation_parents (
        conversation_id TEXT PRIMARY KEY,
        parent_id TEXT NOT NULL
      );
    `);
    const insertSummary = db.prepare(`
      INSERT INTO conversation_summaries (
        conversation_id, title, preview, last_modified_time, workspace_uris, parent_conversation_id, nesting_depth
      ) VALUES (?, ?, ?, ?, ?, ?, ?)
    `);
    for (const row of rows) {
      insertSummary.run(
        row.conversationId,
        row.title ?? "",
        row.preview ?? "",
        row.lastModifiedTime ?? "",
        JSON.stringify(
          (row.workspacePaths ?? []).map(
            (workspacePath) => NodeURL.pathToFileURL(workspacePath).href,
          ),
        ),
        row.parentConversationId ?? "",
        row.nestingDepth ?? 0,
      );
    }
    const insertParent = db.prepare(
      "INSERT INTO conversation_parents (conversation_id, parent_id) VALUES (?, ?)",
    );
    for (const childId of parentTableIds) {
      insertParent.run(childId, SESSION_A);
    }
  } finally {
    db.close();
  }
  return dbPath;
}

async function writeTranscript(
  rootDir: string,
  sessionId: string,
  records: ReadonlyArray<Record<string, unknown>>,
  filename: "transcript_full.jsonl" | "transcript.jsonl" = "transcript_full.jsonl",
): Promise<string> {
  const logsDir = NodePath.join(rootDir, "brain", sessionId, ".system_generated", "logs");
  await NodeFSP.mkdir(logsDir, { recursive: true });
  const transcriptPath = NodePath.join(logsDir, filename);
  await NodeFSP.writeFile(
    transcriptPath,
    records.map((record) => JSON.stringify(record)).join("\n") + "\n",
    "utf8",
  );
  return transcriptPath;
}

describe("extractAntigravityUserText", () => {
  it("unwraps <USER_REQUEST> envelopes and strips trailing metadata blocks", () => {
    const raw = [
      "<USER_REQUEST>",
      "Investigate the flaky login test",
      "</USER_REQUEST>",
      "<ADDITIONAL_METADATA>",
      "Current time: 2026-10-01T12:00:00Z",
      "</ADDITIONAL_METADATA>",
      "<USER_SETTINGS_CHANGE>",
      "mode=auto",
      "</USER_SETTINGS_CHANGE>",
    ].join("\n");
    expect(extractAntigravityUserText(raw)).toBe("Investigate the flaky login test");
  });

  it("preserves literal </USER_REQUEST> text inside a user prompt", () => {
    const raw =
      "<USER_REQUEST>\nFix parsing of </USER_REQUEST> inside code blocks\n</USER_REQUEST>\n<ADDITIONAL_METADATA>meta</ADDITIONAL_METADATA>";
    expect(extractAntigravityUserText(raw)).toBe(
      "Fix parsing of </USER_REQUEST> inside code blocks",
    );
  });

  it("strips T3 instruction wrappers when present", () => {
    const raw =
      "<USER_REQUEST>\n<t3_code_instructions>\nInternal prompt\n</t3_code_instructions>\n<user_request>\nShip the release\n</user_request>\n</USER_REQUEST>";
    expect(extractAntigravityUserText(raw)).toBe("Ship the release");
  });

  it("validates Antigravity UUID session identifiers", () => {
    expect(isAntigravitySessionId(SESSION_A)).toBe(true);
    expect(isAntigravitySessionId("../escape")).toBe(false);
    expect(isAntigravitySessionId("not-a-uuid")).toBe(false);
  });
});

it.layer(NodeServices.layer)("resolveAntigravityDataRoots", (it) => {
  it.effect("expands ANTIGRAVITY_DATA_DIR, deduplicates, and excludes T3-managed paths", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-agy-roots-" });
      const realRoot = yield* fs.realPath(root);
      const fakeHome = path.join(realRoot, "home");
      const customA = path.join(fakeHome, "custom-a");
      const t3Base = path.join(realRoot, "t3-base");
      const nestedInsideT3 = path.join(t3Base, "state", "antigravity-acp");
      yield* fs.makeDirectory(customA, { recursive: true });
      yield* fs.makeDirectory(nestedInsideT3, { recursive: true });

      const defaults = yield* resolveAntigravityDataRoots({
        environment: {},
        homeDir: fakeHome,
        excludeRoots: [t3Base],
      });
      expect(defaults).toEqual([
        path.join(fakeHome, ".gemini", "antigravity"),
        path.join(fakeHome, ".gemini", "antigravity-cli"),
        path.join(fakeHome, ".gemini", "antigravity-ide"),
        path.join(fakeHome, ".gemini", "antigravity-backup"),
        path.join(fakeHome, ".config", "antigravity"),
      ]);

      const overridden = yield* resolveAntigravityDataRoots({
        environment: {
          ANTIGRAVITY_DATA_DIR: `~/custom-a, ${customA}, ${nestedInsideT3}`,
        },
        homeDir: fakeHome,
        excludeRoots: [t3Base],
      });
      expect(overridden).toEqual([customA]);
    }),
  );
});

it.layer(NodeServices.layer)("AntigravityExternalSessions", (it) => {
  it.effect(
    "scans Desktop/CLI roots, filters subagents, handles Go zero-time, and falls back to protobuf metadata",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-agy-scan-" });
        const realRoot = yield* fs.realPath(root);
        const workspace = path.join(realRoot, "project");
        const dataRoot = path.join(realRoot, "antigravity");
        yield* fs.makeDirectory(workspace, { recursive: true });

        yield* Effect.promise(async () => {
          await createConversationDb(dataRoot, SESSION_A, [workspace]);
          await createConversationDb(dataRoot, SESSION_B, [workspace]);
          await createConversationDb(dataRoot, SESSION_SUBAGENT, [workspace]);
          await createConversationDb(dataRoot, SESSION_PARENT_TABLE, [workspace]);
          await createConversationDb(dataRoot, SESSION_NO_SUMMARY, [workspace]);

          await writeTranscript(dataRoot, SESSION_A, [
            {
              type: "USER_INPUT",
              source: "USER_EXPLICIT",
              content: "<USER_REQUEST>\nHello A\n</USER_REQUEST>",
              created_at: "2026-10-10T10:00:00Z",
            },
          ]);
          await writeTranscript(
            dataRoot,
            SESSION_B,
            [
              {
                type: "USER_INPUT",
                source: "USER_EXPLICIT",
                content: "<USER_REQUEST>\nFallback compact transcript\n</USER_REQUEST>",
              },
            ],
            "transcript.jsonl",
          );

          await writeSummaryDb(
            dataRoot,
            [
              {
                conversationId: SESSION_A,
                title: "Desktop session title",
                lastModifiedTime: "2026-10-10 12:00:00+00:00",
                workspacePaths: [workspace],
              },
              {
                conversationId: SESSION_B,
                title: "",
                preview: "<USER_REQUEST>\nPreview fallback title\n</USER_REQUEST>",
                lastModifiedTime: "0001-01-01 00:00:00+00:00",
                workspacePaths: [workspace],
              },
              {
                conversationId: SESSION_SUBAGENT,
                title: "Hidden subagent",
                lastModifiedTime: "2026-10-10 12:05:00+00:00",
                workspacePaths: [workspace],
                parentConversationId: SESSION_A,
                nestingDepth: 1,
              },
              {
                conversationId: SESSION_PARENT_TABLE,
                title: "Parent-table subagent",
                lastModifiedTime: "2026-10-10 12:06:00+00:00",
                workspacePaths: [workspace],
              },
            ],
            [SESSION_PARENT_TABLE],
          );
        });

        const sessions = yield* Effect.promise(() =>
          scanAntigravityRootSessions(dataRoot, "external"),
        );
        const byId = new Map(sessions.map((session) => [session.sessionId, session]));
        expect([...byId.keys()].toSorted()).toEqual(
          [SESSION_A, SESSION_B, SESSION_NO_SUMMARY].toSorted(),
        );
        expect(byId.get(SESSION_A)?.title).toBe("Desktop session title");
        expect(byId.get(SESSION_A)?.updatedAtMs).toBe(Date.parse("2026-10-10T12:00:00Z"));
        expect(byId.get(SESSION_A)?.transcriptPath).toContain("transcript_full.jsonl");
        expect(byId.get(SESSION_B)?.title).toBe("Preview fallback title");
        // Go zero-time must not produce a negative/pre-epoch timestamp.
        expect(byId.get(SESSION_B)?.updatedAtMs).toBeGreaterThan(0);
        expect(byId.get(SESSION_B)?.transcriptPath).toContain("transcript.jsonl");
        expect(byId.get(SESSION_NO_SUMMARY)?.workspacePaths).toEqual([workspace]);
        expect(byId.get(SESSION_NO_SUMMARY)?.transcriptPath).toBeNull();
      }),
  );

  it.effect(
    "performs WAL-safe atomic materialization while a live writer holds uncommitted WAL pages",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-agy-wal-" });
        const realRoot = yield* fs.realPath(root);
        const workspace = path.join(realRoot, "workspace");
        const externalRoot = path.join(realRoot, "antigravity-cli");
        const profileDirectory = path.join(realRoot, "t3-profile");
        yield* fs.makeDirectory(workspace, { recursive: true });

        const result = yield* Effect.promise(async () => {
          const sourceDbPath = await createConversationDb(externalRoot, SESSION_A, [workspace], {
            walMode: true,
            extraMessage: "committed-before-wal",
          });
          await writeTranscript(externalRoot, SESSION_A, [
            {
              type: "USER_INPUT",
              source: "USER_EXPLICIT",
              content: "<USER_REQUEST>\nKeep brain files\n</USER_REQUEST>",
            },
          ]);
          const annotationsDir = NodePath.join(externalRoot, "annotations");
          await NodeFSP.mkdir(annotationsDir, { recursive: true });
          await NodeFSP.writeFile(
            NodePath.join(annotationsDir, `${SESSION_A}.pbtxt`),
            "annotation: true\n",
            "utf8",
          );
          if (symlinksSupported) {
            const secret = NodePath.join(realRoot, "outside-secret.txt");
            await NodeFSP.writeFile(secret, "do-not-copy", "utf8");
            await NodeFSP.symlink(
              secret,
              NodePath.join(externalRoot, "brain", SESSION_A, "sneaky-link.txt"),
            );
          }

          // Keep a live WAL writer open with a row that only exists in the -wal file.
          const liveWriter = new NodeSqlite.DatabaseSync(sourceDbPath);
          try {
            liveWriter.exec("PRAGMA wal_autocheckpoint = 0;");
            liveWriter.prepare("INSERT INTO notes (body) VALUES (?)").run("live-wal-row");

            const [first, second] = await Promise.all([
              materializeAntigravitySession({
                profileDirectory,
                sessionId: SESSION_A,
                externalRoots: [externalRoot],
              }),
              materializeAntigravitySession({
                profileDirectory,
                sessionId: SESSION_A,
                externalRoots: [externalRoot],
              }),
            ]);
            const statuses = [first.status, second.status].toSorted();
            return { statuses };
          } finally {
            liveWriter.close();
          }
        });

        expect(result.statuses).toEqual(["already_present", "materialized"]);

        const targetBase = path.join(profileDirectory, "antigravity-acp");
        const copiedDbPath = path.join(targetBase, "conversations", `${SESSION_A}.db`);
        expect(yield* fs.exists(copiedDbPath)).toBe(true);
        expect(yield* fs.exists(`${copiedDbPath}-wal`)).toBe(false);
        expect(yield* fs.exists(`${copiedDbPath}-shm`)).toBe(false);
        expect(
          yield* fs.exists(
            path.join(
              targetBase,
              "brain",
              SESSION_A,
              ".system_generated",
              "logs",
              "transcript_full.jsonl",
            ),
          ),
        ).toBe(true);
        expect(yield* fs.exists(path.join(targetBase, "annotations", `${SESSION_A}.pbtxt`))).toBe(
          true,
        );
        if (symlinksSupported) {
          expect(
            yield* fs.exists(path.join(targetBase, "brain", SESSION_A, "sneaky-link.txt")),
          ).toBe(false);
        }

        const copiedState = yield* Effect.sync(() => {
          const copied = new NodeSqlite.DatabaseSync(copiedDbPath, { readOnly: true });
          try {
            const modeRow = copied.prepare("PRAGMA journal_mode").get() as {
              readonly journal_mode?: string;
            };
            const notes = copied
              .prepare("SELECT body FROM notes ORDER BY id")
              .all()
              .map((row) => (row as { readonly body: string }).body);
            return { journalMode: modeRow.journal_mode, notes };
          } finally {
            copied.close();
          }
        });
        expect(copiedState.journalMode).toBe("delete");
        expect(copiedState.notes).toEqual(["committed-before-wal", "live-wal-row"]);

        // Idempotent repeat does not overwrite T3's copy.
        const third = yield* Effect.promise(() =>
          materializeAntigravitySession({
            profileDirectory,
            sessionId: SESSION_A,
            externalRoots: [externalRoot],
          }),
        );
        expect(third.status).toBe("already_present");

        const missing = yield* Effect.promise(() =>
          materializeAntigravitySession({
            profileDirectory,
            sessionId: SESSION_B,
            externalRoots: [externalRoot],
          }),
        );
        expect(missing.status).toBe("not_found");
      }),
  );

  it.effect(
    "lists sessions across internal and external roots, matches linked git worktrees, and derives fallback titles",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-agy-list-" });
        const realRoot = yield* fs.realPath(root);
        const repo = path.join(realRoot, "repo");
        const worktree = path.join(realRoot, "repo-feature");
        const worktreeGitDir = path.join(repo, ".git", "worktrees", "feature");
        const otherProject = path.join(realRoot, "other");
        const profileDirectory = path.join(realRoot, "profile");
        const internalRoot = path.join(profileDirectory, "antigravity-acp");
        const desktopRoot = path.join(realRoot, "desktop");
        const cliRoot = path.join(realRoot, "cli");

        yield* fs.makeDirectory(worktreeGitDir, { recursive: true });
        yield* fs.makeDirectory(worktree, { recursive: true });
        yield* fs.makeDirectory(otherProject, { recursive: true });
        yield* fs.writeFileString(path.join(repo, ".git", "config"), "[core]\n");
        yield* fs.writeFileString(path.join(worktreeGitDir, "commondir"), "../..\n");
        yield* fs.writeFileString(path.join(worktree, ".git"), `gitdir: ${worktreeGitDir}\n`);

        yield* Effect.promise(async () => {
          // SESSION_A exists in both internal (no summary title) and desktop (with summary title).
          await createConversationDb(internalRoot, SESSION_A, [repo]);
          await createConversationDb(desktopRoot, SESSION_A, [repo]);
          await writeSummaryDb(desktopRoot, [
            {
              conversationId: SESSION_A,
              title: "Inherited desktop title",
              lastModifiedTime: "2026-10-11 10:00:00+00:00",
              workspacePaths: [repo],
            },
          ]);
          // SESSION_B ran in a linked worktree of `repo` in CLI, without a summary title -> derive from transcript.
          await createConversationDb(cliRoot, SESSION_B, [worktree]);
          await writeTranscript(cliRoot, SESSION_B, [
            {
              type: "USER_INPUT",
              source: "USER_EXPLICIT",
              content: "<USER_REQUEST>\nFix worktree path resolution\n</USER_REQUEST>",
            },
          ]);
          // SESSION_NO_SUMMARY belongs to another project and must be filtered out.
          await createConversationDb(cliRoot, SESSION_NO_SUMMARY, [otherProject]);
        });

        const listed = yield* listAntigravityNativeSessions({
          profileDirectory,
          externalRoots: [desktopRoot, cliRoot],
          cwd: repo,
        });
        expect(listed.canLoad).toBe(true);
        expect(listed.canResume).toBe(true);
        expect(listed.canDelete).toBe(false);
        expect(listed.nextCursor).toBeNull();
        expect(listed.sessions.map((session) => session.sessionId).toSorted()).toEqual(
          [SESSION_A, SESSION_B].toSorted(),
        );
        const listedById = new Map(listed.sessions.map((session) => [session.sessionId, session]));
        expect(listedById.get(SESSION_A)?.title).toBe("Inherited desktop title");
        expect(listedById.get(SESSION_B)?.title).toBe("Fix worktree path resolution");
        expect(listedById.get(SESSION_B)?.cwd).toBe(worktree);

        const invalidCursor = yield* listAntigravityNativeSessions({
          profileDirectory,
          externalRoots: [desktopRoot, cliRoot],
          cwd: repo,
          cursor: "not-a-number",
        }).pipe(Effect.flip);
        expect(invalidCursor._tag).toBe("AcpRegistryOperationError");
      }),
  );
});
