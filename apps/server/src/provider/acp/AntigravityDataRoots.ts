import { expandHomePath } from "@t3tools/provider-core/server/pathExpansion";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";

const DEFAULT_ANTIGRAVITY_GEMINI_DATA_DIRS = [
  "antigravity",
  "antigravity-cli",
  "antigravity-ide",
  "antigravity-backup",
] as const;

function normalizePathPrefix(target: string, separator: string): string {
  const normalized = target.replaceAll("\\", "/").replace(/\/+$/, "");
  const folded = separator === "\\" ? normalized.toLowerCase() : normalized;
  return `${folded}/`;
}

function isWithinExcludedRoot(
  candidate: string,
  excludedRoots: ReadonlyArray<string>,
  separator: string,
): boolean {
  const candidatePrefix = normalizePathPrefix(candidate, separator);
  return excludedRoots.some((excluded) =>
    candidatePrefix.startsWith(normalizePathPrefix(excluded, separator)),
  );
}

/**
 * Resolves Antigravity native data directories (Desktop, CLI, IDE, backup, or explicit
 * `ANTIGRAVITY_DATA_DIR` entries), canonicalized and optionally excluding T3-managed paths.
 */
export const resolveAntigravityDataRoots = Effect.fn("resolveAntigravityDataRoots")(
  function* (input: {
    readonly environment: NodeJS.ProcessEnv;
    readonly homeDir: string;
    readonly excludeRoots?: ReadonlyArray<string>;
  }): Effect.fn.Return<ReadonlyArray<string>, never, FileSystem.FileSystem | Path.Path> {
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const configured = input.environment["ANTIGRAVITY_DATA_DIR"]
      ?.split(",")
      .map((value) => value.trim())
      .filter(Boolean);
    const defaults = [
      ...DEFAULT_ANTIGRAVITY_GEMINI_DATA_DIRS.map((name) =>
        path.join(input.homeDir, ".gemini", name),
      ),
      path.join(input.homeDir, ".config", "antigravity"),
    ];
    const excluded: Array<string> = [];
    for (const root of input.excludeRoots ?? []) {
      const resolved = path.resolve(expandHomePath(root, input.homeDir));
      excluded.push(resolved);
      const real = yield* fileSystem.realPath(resolved).pipe(Effect.orElseSucceed(() => resolved));
      if (real !== resolved) excluded.push(real);
    }
    const canonical = new Set<string>();
    for (const root of configured?.length ? configured : defaults) {
      const resolved = path.resolve(expandHomePath(root, input.homeDir));
      const real = yield* fileSystem.realPath(resolved).pipe(Effect.orElseSucceed(() => resolved));
      if (
        excluded.length > 0 &&
        (isWithinExcludedRoot(resolved, excluded, path.sep) ||
          isWithinExcludedRoot(real, excluded, path.sep))
      ) {
        continue;
      }
      canonical.add(real);
    }
    return [...canonical];
  },
);
