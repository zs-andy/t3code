// @effect-diagnostics nodeBuiltinImport:off -- Reads and backs up native Antigravity SQLite session databases and transcripts.
import * as NodeCrypto from "node:crypto";
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";
import * as NodeURL from "node:url";

import {
  AcpRegistryOperationError,
  type AcpRegistryListSessionsResult,
  type AcpRegistrySession,
} from "@t3tools/contracts";
import { splitsSurrogatePair } from "@t3tools/shared/assistantCitations";
import { isBareGitConfig } from "@t3tools/shared/git";
import { normalizeProjectPathForComparison } from "@t3tools/shared/path";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";

import { antigravityManagedPath, antigravityProfileRoot } from "./AntigravityProfilePaths.ts";
import { readAntigravityDatabaseWorkspacePaths } from "./AntigravitySessionFiles.ts";

const ANTIGRAVITY_SESSION_ID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

const NATIVE_SESSIONS_PAGE_SIZE = 50;
const TITLE_SNIFF_MAX_BYTES = 64 * 1024;
const TITLE_MAX_LENGTH = 100;
const TRAILING_ANTIGRAVITY_METADATA =
  /(?:\s*<(?:ADDITIONAL_METADATA|USER_SETTINGS_CHANGE)\b[^>]*>[\s\S]*?<\/(?:ADDITIONAL_METADATA|USER_SETTINGS_CHANGE)>)+$/;
const T3_INSTRUCTIONS_PREFIX =
  /^\s*<t3_code_instructions\b[^>]*>[\s\S]*?<\/t3_code_instructions>\s*/;
const T3_USER_REQUEST_WRAPPER = /^\s*<user_request\b[^>]*>([\s\S]*)<\/user_request>\s*$/;

const materializationLocks = new Map<string, Promise<unknown>>();

export interface AntigravitySessionSummary {
  readonly sessionId: string;
  readonly origin: "internal" | "external";
  readonly rootDirectory: string;
  readonly databasePath: string;
  readonly transcriptPath: string | null;
  readonly workspacePaths: ReadonlyArray<string>;
  readonly title: string | null;
  readonly updatedAtMs: number;
}

export interface AntigravityMaterializationResult {
  readonly status: "already_present" | "materialized" | "not_found";
  readonly sourceRoot?: string;
}

export function isAntigravitySessionId(value: string): boolean {
  return ANTIGRAVITY_SESSION_ID_PATTERN.test(value);
}

/**
 * Unwraps Antigravity `USER_INPUT` envelopes (`<USER_REQUEST>...</USER_REQUEST>` plus trailing
 * `<ADDITIONAL_METADATA>` / `<USER_SETTINGS_CHANGE>` blocks, and T3 prompt wrappers when present).
 */
export function extractAntigravityUserText(content: string): string {
  let text = content.trim();
  if (text.length === 0) return "";
  const withoutTrailingMetadata = text.replace(TRAILING_ANTIGRAVITY_METADATA, "").trim();
  if (
    withoutTrailingMetadata.startsWith("<USER_REQUEST>") &&
    withoutTrailingMetadata.endsWith("</USER_REQUEST>")
  ) {
    text = withoutTrailingMetadata.slice("<USER_REQUEST>".length, -"</USER_REQUEST>".length).trim();
  } else {
    const inlineMatch = /<USER_REQUEST>([\s\S]*?)<\/USER_REQUEST>/.exec(withoutTrailingMetadata);
    text = inlineMatch?.[1] !== undefined ? inlineMatch[1].trim() : withoutTrailingMetadata;
  }
  const withoutInstructions = text.replace(T3_INSTRUCTIONS_PREFIX, "").trim();
  const unwrappedT3 = T3_USER_REQUEST_WRAPPER.exec(withoutInstructions);
  return (unwrappedT3?.[1] ?? withoutInstructions).trim();
}

function summarizeTitleLine(text: string): string | null {
  const line = text
    .split(/\r?\n/)
    .map((segment) => segment.trim())
    .find((segment) => segment.length > 0);
  if (line === undefined) return null;
  const max = splitsSurrogatePair(line, TITLE_MAX_LENGTH) ? TITLE_MAX_LENGTH - 1 : TITLE_MAX_LENGTH;
  const trimmed = line.slice(0, max).trim();
  return trimmed.length > 0 ? trimmed : null;
}

function parseSummaryTimestampMs(value: unknown): number | null {
  if (typeof value === "number") {
    return Number.isFinite(value) && value > 0 ? value : null;
  }
  if (typeof value !== "string") return null;
  const trimmed = value.trim();
  if (trimmed.length === 0) return null;
  const isoLike = trimmed.replace(/^(\d{4}-\d{2}-\d{2}) (\d{2}:\d{2}:\d{2})/, "$1T$2");
  const parsed = Date.parse(isoLike);
  // Reject Go zero-time ("0001-01-01 00:00:00+00:00") and other pre-epoch sentinels.
  return Number.isFinite(parsed) && parsed > 0 ? parsed : null;
}

function parseWorkspaceUrisJson(value: unknown): ReadonlyArray<string> {
  if (typeof value !== "string" || value.trim().length === 0) return [];
  let parsed: unknown;
  try {
    parsed = JSON.parse(value);
  } catch {
    return [];
  }
  if (!Array.isArray(parsed)) return [];
  const paths: Array<string> = [];
  const seen = new Set<string>();
  for (const item of parsed) {
    if (typeof item !== "string" || item.trim().length === 0) continue;
    let filePath: string;
    try {
      filePath = item.startsWith("file://") ? NodeURL.fileURLToPath(item) : item;
    } catch {
      continue;
    }
    const trimmed = filePath.trim();
    if (trimmed.length === 0 || !NodePath.isAbsolute(trimmed) || seen.has(trimmed)) continue;
    seen.add(trimmed);
    paths.push(trimmed);
  }
  return paths;
}

interface SummaryIndexRow {
  readonly title: string | null;
  readonly workspacePaths: ReadonlyArray<string>;
  readonly updatedAtMs: number | null;
}

interface SummaryIndex {
  readonly rows: ReadonlyMap<string, SummaryIndexRow>;
  readonly subagentIds: ReadonlySet<string>;
}

async function readConversationSummaries(canonicalRoot: string): Promise<SummaryIndex> {
  const rows = new Map<string, SummaryIndexRow>();
  const subagentIds = new Set<string>();
  const summaryDb = await antigravityManagedPath(
    canonicalRoot,
    ["conversation_summaries.db"],
    "file",
  );
  if (summaryDb === undefined) {
    return { rows, subagentIds };
  }
  let database: NodeSqlite.DatabaseSync | undefined;
  try {
    database = new NodeSqlite.DatabaseSync(summaryDb.path, { readOnly: true });
    database.exec("PRAGMA busy_timeout = 100");

    try {
      const parentRows = database.prepare("SELECT conversation_id FROM conversation_parents").all();
      for (const row of parentRows) {
        if (
          row !== null &&
          typeof row === "object" &&
          "conversation_id" in row &&
          typeof row.conversation_id === "string" &&
          isAntigravitySessionId(row.conversation_id)
        ) {
          subagentIds.add(row.conversation_id);
        }
      }
    } catch {
      // Optional table in older or minimal databases.
    }

    const columns = new Set<string>();
    const tableInfo = database.prepare("PRAGMA table_info(conversation_summaries)").all();
    for (const column of tableInfo) {
      if (
        column !== null &&
        typeof column === "object" &&
        "name" in column &&
        typeof column.name === "string"
      ) {
        columns.add(column.name);
      }
    }
    if (!columns.has("conversation_id")) {
      return { rows, subagentIds };
    }

    const selectColumns = [
      "conversation_id",
      columns.has("title") ? "title" : "NULL AS title",
      columns.has("preview") ? "preview" : "NULL AS preview",
      columns.has("last_modified_time") ? "last_modified_time" : "NULL AS last_modified_time",
      columns.has("workspace_uris") ? "workspace_uris" : "NULL AS workspace_uris",
      columns.has("parent_conversation_id")
        ? "parent_conversation_id"
        : "NULL AS parent_conversation_id",
      columns.has("nesting_depth") ? "nesting_depth" : "0 AS nesting_depth",
    ];
    const summaryRows = database
      .prepare(`SELECT ${selectColumns.join(", ")} FROM conversation_summaries`)
      .all();

    for (const row of summaryRows) {
      if (row === null || typeof row !== "object") continue;
      const record = row as Record<string, unknown>;
      const sessionId =
        typeof record.conversation_id === "string" ? record.conversation_id.trim() : "";
      if (!isAntigravitySessionId(sessionId)) continue;

      const parentId =
        typeof record.parent_conversation_id === "string"
          ? record.parent_conversation_id.trim()
          : "";
      const nestingDepth =
        typeof record.nesting_depth === "number" && Number.isFinite(record.nesting_depth)
          ? record.nesting_depth
          : 0;
      if (parentId.length > 0 || nestingDepth > 0) {
        subagentIds.add(sessionId);
        continue;
      }

      const rawTitle = typeof record.title === "string" ? record.title.trim() : "";
      const rawPreview = typeof record.preview === "string" ? record.preview.trim() : "";
      const title =
        rawTitle.length > 0
          ? summarizeTitleLine(extractAntigravityUserText(rawTitle))
          : rawPreview.length > 0
            ? summarizeTitleLine(extractAntigravityUserText(rawPreview))
            : null;
      rows.set(sessionId, {
        title,
        workspacePaths: parseWorkspaceUrisJson(record.workspace_uris),
        updatedAtMs: parseSummaryTimestampMs(record.last_modified_time),
      });
    }
  } catch {
    // Fall back to direct conversation .db inspection when summary index is absent or unreadable.
  } finally {
    database?.close();
  }
  return { rows, subagentIds };
}

/**
 * Discovers top-level Antigravity sessions under a single data root (Desktop, CLI, IDE, backup,
 * or T3's managed `antigravity-acp` directory). Subagent conversations and symlinked paths are
 * skipped.
 */
export async function scanAntigravityRootSessions(
  rootDirectory: string,
  origin: "internal" | "external",
): Promise<ReadonlyArray<AntigravitySessionSummary>> {
  const canonicalRoot = await antigravityProfileRoot(rootDirectory);
  if (canonicalRoot === undefined) return [];

  const conversationsDir = await antigravityManagedPath(
    canonicalRoot,
    ["conversations"],
    "directory",
  );
  if (conversationsDir === undefined) return [];

  const entries = await NodeFSP.readdir(conversationsDir.path).catch((): Array<string> => []);
  if (entries.length === 0) return [];

  const summaryIndex = await readConversationSummaries(canonicalRoot);
  const sessions: Array<AntigravitySessionSummary> = [];

  for (const entry of entries) {
    if (!entry.endsWith(".db")) continue;
    const sessionId = entry.slice(0, -".db".length);
    if (!isAntigravitySessionId(sessionId) || summaryIndex.subagentIds.has(sessionId)) continue;

    const dbFile = await antigravityManagedPath(canonicalRoot, ["conversations", entry], "file");
    if (dbFile === undefined) continue;

    const transcriptFull = await antigravityManagedPath(
      canonicalRoot,
      ["brain", sessionId, ".system_generated", "logs", "transcript_full.jsonl"],
      "file",
    );
    const transcriptCompact =
      transcriptFull ??
      (await antigravityManagedPath(
        canonicalRoot,
        ["brain", sessionId, ".system_generated", "logs", "transcript.jsonl"],
        "file",
      ));

    const summary = summaryIndex.rows.get(sessionId);
    const workspacePaths =
      summary !== undefined && summary.workspacePaths.length > 0
        ? summary.workspacePaths
        : (readAntigravityDatabaseWorkspacePaths(dbFile.path, sessionId, {
            ignoreInvalidUris: true,
          }) ?? []);
    if (workspacePaths.length === 0) continue;

    const fileMtimeMs = Math.max(dbFile.info.mtimeMs, transcriptCompact?.info.mtimeMs ?? 0);
    const updatedAtMs = summary?.updatedAtMs ?? fileMtimeMs;

    sessions.push({
      sessionId,
      origin,
      rootDirectory: canonicalRoot,
      databasePath: dbFile.path,
      transcriptPath: transcriptCompact?.path ?? null,
      workspacePaths,
      title: summary?.title ?? null,
      updatedAtMs,
    });
  }

  return sessions;
}

async function withMaterializationLock<A>(key: string, run: () => Promise<A>): Promise<A> {
  const previous = materializationLocks.get(key) ?? Promise.resolve();
  const next = previous.then(run, run);
  const cleanup = next.then(
    () => {
      if (materializationLocks.get(key) === cleanup) {
        materializationLocks.delete(key);
      }
    },
    () => {
      if (materializationLocks.get(key) === cleanup) {
        materializationLocks.delete(key);
      }
    },
  );
  materializationLocks.set(key, cleanup);
  return next;
}

async function copyRegularDirectoryTree(sourceDir: string, destinationDir: string): Promise<void> {
  await NodeFSP.mkdir(destinationDir, { recursive: true });
  const entries = await NodeFSP.readdir(sourceDir);
  for (const entry of entries) {
    const sourceEntry = NodePath.join(sourceDir, entry);
    const destEntry = NodePath.join(destinationDir, entry);
    const stats = await NodeFSP.lstat(sourceEntry).catch(() => undefined);
    if (stats === undefined || stats.isSymbolicLink()) continue;
    if (stats.isDirectory()) {
      await copyRegularDirectoryTree(sourceEntry, destEntry);
    } else if (stats.isFile()) {
      await NodeFSP.copyFile(sourceEntry, destEntry, NodeFS.constants.COPYFILE_FICLONE);
      await NodeFSP.utimes(destEntry, stats.atime, stats.mtime).catch(() => undefined);
    }
  }
}

async function removeSqliteTempFiles(basePath: string): Promise<void> {
  for (const suffix of ["", "-wal", "-shm", "-journal"]) {
    await NodeFSP.rm(`${basePath}${suffix}`, { force: true }).catch(() => undefined);
  }
}

async function backupSqliteDatabaseAtomically(
  sourceDbPath: string,
  sourceStats: { readonly atime: Date; readonly mtime: Date },
  targetDbPath: string,
): Promise<void> {
  const tempDbPath = NodePath.join(
    NodePath.dirname(targetDbPath),
    `.${NodePath.basename(targetDbPath, ".db")}.${NodeCrypto.randomUUID()}.tmp.db`,
  );
  let sourceDb: NodeSqlite.DatabaseSync | undefined;
  let tempDb: NodeSqlite.DatabaseSync | undefined;
  try {
    sourceDb = new NodeSqlite.DatabaseSync(sourceDbPath, { readOnly: true });
    sourceDb.exec("PRAGMA busy_timeout = 500");
    await NodeSqlite.backup(sourceDb, tempDbPath);
    sourceDb.close();
    sourceDb = undefined;

    // SQLite backup preserves WAL mode on the copy; collapse WAL into the single .db file
    // before atomic rename so the materialized file never needs stray -wal/-shm sidecars.
    tempDb = new NodeSqlite.DatabaseSync(tempDbPath);
    tempDb.exec("PRAGMA wal_checkpoint(TRUNCATE); PRAGMA journal_mode = DELETE;");
    tempDb.close();
    tempDb = undefined;

    for (const suffix of ["-wal", "-shm", "-journal"]) {
      await NodeFSP.rm(`${tempDbPath}${suffix}`, { force: true }).catch(() => undefined);
    }
    await NodeFSP.utimes(tempDbPath, sourceStats.atime, sourceStats.mtime).catch(() => undefined);
    await NodeFSP.rename(tempDbPath, targetDbPath);
  } catch (error) {
    sourceDb?.close();
    tempDb?.close();
    await removeSqliteTempFiles(tempDbPath);
    throw error;
  }
}

/**
 * Lazily copies an external Antigravity session (SQLite DB via WAL-safe online backup, plus
 * `brain/<id>` and `annotations/<id>.pbtxt`) into T3's isolated `profileDirectory/antigravity-acp`
 * root. Idempotent, concurrency-safe per `(profile, sessionId)`, and never mutates external roots.
 */
export async function materializeAntigravitySession(input: {
  readonly profileDirectory: string;
  readonly sessionId: string;
  readonly externalRoots: ReadonlyArray<string>;
}): Promise<AntigravityMaterializationResult> {
  if (!isAntigravitySessionId(input.sessionId)) {
    return { status: "not_found" };
  }

  const acpDirectory = NodePath.join(input.profileDirectory, "antigravity-acp");
  await NodeFSP.mkdir(NodePath.join(acpDirectory, "conversations"), { recursive: true });
  const canonicalAcpRoot = await antigravityProfileRoot(acpDirectory);
  if (canonicalAcpRoot === undefined) {
    return { status: "not_found" };
  }

  const lockKey = `${canonicalAcpRoot}\0${input.sessionId}`;
  return withMaterializationLock(lockKey, async () => {
    const existing = await antigravityManagedPath(
      canonicalAcpRoot,
      ["conversations", `${input.sessionId}.db`],
      "file",
    );
    if (existing !== undefined) {
      return { status: "already_present" };
    }

    let bestSource:
      | {
          readonly root: string;
          readonly dbPath: string;
          readonly info: { readonly atime: Date; readonly mtime: Date; readonly mtimeMs: number };
        }
      | undefined;

    for (const externalRoot of input.externalRoots) {
      const canonicalExternal = await antigravityProfileRoot(externalRoot);
      if (canonicalExternal === undefined || canonicalExternal === canonicalAcpRoot) continue;
      const dbFile = await antigravityManagedPath(
        canonicalExternal,
        ["conversations", `${input.sessionId}.db`],
        "file",
      );
      if (dbFile === undefined) continue;
      if (bestSource === undefined || dbFile.info.mtimeMs > bestSource.info.mtimeMs) {
        bestSource = { root: canonicalExternal, dbPath: dbFile.path, info: dbFile.info };
      }
    }

    if (bestSource === undefined) {
      return { status: "not_found" };
    }

    // Stage companion brain/ and annotations/ before renaming the .db file so an interrupted
    // materialization never marks the session as already_present without its companion files.
    const sourceBrain = await antigravityManagedPath(
      bestSource.root,
      ["brain", input.sessionId],
      "directory",
    );
    if (sourceBrain !== undefined) {
      const targetBrainRoot = NodePath.join(canonicalAcpRoot, "brain");
      const targetBrainPath = NodePath.join(targetBrainRoot, input.sessionId);
      const existingBrain = await NodeFSP.lstat(targetBrainPath).catch(() => undefined);
      if (existingBrain === undefined) {
        await NodeFSP.mkdir(targetBrainRoot, { recursive: true });
        const tempBrainPath = NodePath.join(
          targetBrainRoot,
          `.${input.sessionId}.${NodeCrypto.randomUUID()}.tmp`,
        );
        try {
          await copyRegularDirectoryTree(sourceBrain.path, tempBrainPath);
          await NodeFSP.rename(tempBrainPath, targetBrainPath);
        } catch {
          await NodeFSP.rm(tempBrainPath, { recursive: true, force: true }).catch(() => undefined);
        }
      }
    }

    const sourceAnnotation = await antigravityManagedPath(
      bestSource.root,
      ["annotations", `${input.sessionId}.pbtxt`],
      "file",
    );
    if (sourceAnnotation !== undefined) {
      const targetAnnotationsRoot = NodePath.join(canonicalAcpRoot, "annotations");
      const targetAnnotationPath = NodePath.join(targetAnnotationsRoot, `${input.sessionId}.pbtxt`);
      const existingAnnotation = await NodeFSP.lstat(targetAnnotationPath).catch(() => undefined);
      if (existingAnnotation === undefined) {
        await NodeFSP.mkdir(targetAnnotationsRoot, { recursive: true });
        const tempAnnotationPath = NodePath.join(
          targetAnnotationsRoot,
          `.${input.sessionId}.${NodeCrypto.randomUUID()}.tmp.pbtxt`,
        );
        try {
          await NodeFSP.copyFile(
            sourceAnnotation.path,
            tempAnnotationPath,
            NodeFS.constants.COPYFILE_FICLONE,
          );
          await NodeFSP.utimes(
            tempAnnotationPath,
            sourceAnnotation.info.atime,
            sourceAnnotation.info.mtime,
          ).catch(() => undefined);
          await NodeFSP.rename(tempAnnotationPath, targetAnnotationPath);
        } catch {
          await NodeFSP.rm(tempAnnotationPath, { force: true }).catch(() => undefined);
        }
      }
    }

    const targetDbPath = NodePath.join(canonicalAcpRoot, "conversations", `${input.sessionId}.db`);
    await backupSqliteDatabaseAtomically(bestSource.dbPath, bestSource.info, targetDbPath);

    return { status: "materialized", sourceRoot: bestSource.root };
  });
}

async function sniffTranscriptTitle(transcriptPath: string): Promise<string | null> {
  let handle: NodeFSP.FileHandle | undefined;
  try {
    handle = await NodeFSP.open(transcriptPath, "r");
    const buffer = Buffer.allocUnsafe(TITLE_SNIFF_MAX_BYTES);
    const { bytesRead } = await handle.read(buffer, 0, TITLE_SNIFF_MAX_BYTES, 0);
    if (bytesRead === 0) return null;
    const chunk = buffer.subarray(0, bytesRead).toString("utf8");
    for (const line of chunk.split("\n")) {
      const trimmed = line.trim();
      if (trimmed.length === 0) continue;
      let parsed: unknown;
      try {
        parsed = JSON.parse(trimmed);
      } catch {
        continue;
      }
      if (parsed === null || typeof parsed !== "object") continue;
      const record = parsed as Record<string, unknown>;
      if (record.type !== "USER_INPUT") continue;
      if (record.source !== undefined && record.source !== "USER_EXPLICIT") continue;
      if (typeof record.content !== "string") continue;
      const extracted = extractAntigravityUserText(record.content);
      const title = summarizeTitleLine(extracted);
      if (title !== null) return title;
    }
    return null;
  } catch {
    return null;
  } finally {
    await handle?.close();
  }
}

async function canonicalPathKey(target: string): Promise<string> {
  const resolved = NodePath.resolve(target);
  const real = await NodeFSP.realpath(resolved).catch(() => resolved);
  return normalizeProjectPathForComparison(real);
}

async function resolveLinkedWorktreeMainCheckout(directory: string): Promise<string | null> {
  const gitPath = NodePath.join(directory, ".git");
  const stats = await NodeFSP.lstat(gitPath).catch(() => undefined);
  if (stats === undefined || !stats.isFile()) return null;
  const pointer = await NodeFSP.readFile(gitPath, "utf8").catch(() => "");
  const target = /^gitdir:\s*(.+)$/m.exec(pointer)?.[1]?.trim();
  if (target === undefined || target.length === 0) return null;
  const gitDir = NodePath.resolve(directory, target);
  if (!/[\\/]worktrees[\\/][^\\/]+[\\/]?$/.test(gitDir)) return null;
  const commonDirRaw = (
    await NodeFSP.readFile(NodePath.join(gitDir, "commondir"), "utf8").catch(() => "")
  ).trim();
  if (commonDirRaw.length === 0) return null;
  const commonDir = NodePath.resolve(gitDir, commonDirRaw);
  if (NodePath.basename(commonDir) !== ".git") return null;
  const configText = await NodeFSP.readFile(NodePath.join(commonDir, "config"), "utf8").catch(
    () => "",
  );
  if (configText.length === 0 || isBareGitConfig(configText)) return null;
  return NodePath.dirname(commonDir);
}

/**
 * Lists native Antigravity sessions matching `cwd` (including linked git worktrees of `cwd`)
 * across both T3's managed profile and external Antigravity Desktop/CLI/IDE roots, without
 * spawning the Antigravity binary.
 */
export const listAntigravityNativeSessions = Effect.fn("listAntigravityNativeSessions")(
  function* (input: {
    readonly profileDirectory: string;
    readonly externalRoots: ReadonlyArray<string>;
    readonly cwd: string;
    readonly cursor?: string;
  }): Effect.fn.Return<AcpRegistryListSessionsResult, AcpRegistryOperationError> {
    let offset = 0;
    if (input.cursor !== undefined && input.cursor.length > 0) {
      if (!/^\d+$/.test(input.cursor)) {
        return yield* new AcpRegistryOperationError({
          reason: "session_list_unsupported",
          message: "Invalid Antigravity session pagination cursor.",
        });
      }
      offset = Number.parseInt(input.cursor, 10);
      if (!Number.isSafeInteger(offset) || offset < 0) {
        return yield* new AcpRegistryOperationError({
          reason: "session_list_unsupported",
          message: "Invalid Antigravity session pagination cursor.",
        });
      }
    }

    return yield* Effect.tryPromise({
      try: async () => {
        const internalRoot = NodePath.join(input.profileDirectory, "antigravity-acp");
        const [internalSessions, ...externalBatches] = await Promise.all([
          scanAntigravityRootSessions(internalRoot, "internal"),
          ...input.externalRoots.map((root) => scanAntigravityRootSessions(root, "external")),
        ]);

        const merged = new Map<string, AntigravitySessionSummary>();
        const accept = (candidate: AntigravitySessionSummary) => {
          const existing = merged.get(candidate.sessionId);
          if (existing === undefined) {
            merged.set(candidate.sessionId, candidate);
            return;
          }
          const preferCandidate =
            existing.origin === "external" && candidate.origin === "internal"
              ? true
              : existing.origin === "internal" && candidate.origin === "external"
                ? false
                : candidate.updatedAtMs > existing.updatedAtMs;
          const winner = preferCandidate ? candidate : existing;
          const loser = preferCandidate ? existing : candidate;
          merged.set(candidate.sessionId, {
            ...winner,
            title: winner.title ?? loser.title,
            transcriptPath: winner.transcriptPath ?? loser.transcriptPath,
          });
        };

        for (const session of internalSessions) accept(session);
        for (const batch of externalBatches) {
          for (const session of batch) accept(session);
        }

        const targetKey = await canonicalPathKey(input.cwd);
        const workspaceKeyCache = new Map<string, string>();
        const worktreeMainKeyCache = new Map<string, string | null>();
        const keyFor = async (workspacePath: string) => {
          const cached = workspaceKeyCache.get(workspacePath);
          if (cached !== undefined) return cached;
          const key = await canonicalPathKey(workspacePath);
          workspaceKeyCache.set(workspacePath, key);
          return key;
        };
        const mainKeyFor = async (workspacePath: string) => {
          if (worktreeMainKeyCache.has(workspacePath)) {
            return worktreeMainKeyCache.get(workspacePath) ?? null;
          }
          const mainCheckout = await resolveLinkedWorktreeMainCheckout(workspacePath);
          const key = mainCheckout === null ? null : await keyFor(mainCheckout);
          worktreeMainKeyCache.set(workspacePath, key);
          return key;
        };

        const matched: Array<{
          readonly session: AntigravitySessionSummary;
          readonly matchedCwd: string;
          readonly additionalDirectories: ReadonlyArray<string>;
        }> = [];

        for (const session of merged.values()) {
          let matchedCwd: string | undefined;
          for (const workspacePath of session.workspacePaths) {
            if (
              (await keyFor(workspacePath)) === targetKey ||
              (await mainKeyFor(workspacePath)) === targetKey
            ) {
              matchedCwd = workspacePath;
              break;
            }
          }
          if (matchedCwd === undefined) continue;
          matched.push({
            session,
            matchedCwd,
            additionalDirectories: session.workspacePaths
              .filter((workspacePath) => workspacePath !== matchedCwd)
              .slice(0, 32),
          });
        }

        matched.sort(
          (left, right) =>
            right.session.updatedAtMs - left.session.updatedAtMs ||
            left.session.sessionId.localeCompare(right.session.sessionId),
        );

        const page = matched.slice(offset, offset + NATIVE_SESSIONS_PAGE_SIZE);
        const nextCursor =
          offset + NATIVE_SESSIONS_PAGE_SIZE < matched.length
            ? String(offset + NATIVE_SESSIONS_PAGE_SIZE)
            : null;

        const sessions: Array<AcpRegistrySession> = await Promise.all(
          page.map(async ({ session, matchedCwd, additionalDirectories }) => {
            const title =
              session.title ??
              (session.transcriptPath !== null
                ? await sniffTranscriptTitle(session.transcriptPath)
                : null);
            return {
              sessionId: session.sessionId,
              cwd: matchedCwd,
              additionalDirectories,
              title: title !== null ? title.slice(0, 1_024) : null,
              updatedAt:
                session.updatedAtMs > 0
                  ? DateTime.formatIso(DateTime.makeUnsafe(session.updatedAtMs))
                  : null,
              importedThreadId: null,
            };
          }),
        );

        return {
          sessions,
          nextCursor,
          canLoad: true,
          canResume: true,
          canDelete: false,
        } satisfies AcpRegistryListSessionsResult;
      },
      catch: (cause) =>
        new AcpRegistryOperationError({
          reason: "session_import_failed",
          message: "Could not list native Antigravity sessions.",
          cause,
        }),
    });
  },
);
