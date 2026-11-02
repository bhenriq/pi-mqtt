/** Git helpers used for presence and (later) per-task repo preparation. */

import { execFileSync } from "node:child_process";
import { existsSync, mkdirSync, writeFileSync } from "node:fs";
import { basename, dirname, join } from "node:path";
import type { RepoResult, RepoSpec } from "./protocol.ts";

export interface GitInfo {
	repo?: string;
	branch?: string;
	headSha?: string;
}

function git(cwd: string, args: string[]): string {
	return execFileSync("git", ["-C", cwd, ...args], {
		encoding: "utf8",
		stdio: ["ignore", "pipe", "ignore"],
		timeout: 5000,
	}).trim();
}

export function readGitInfo(cwd: string): GitInfo {
	try {
		const toplevel = git(cwd, ["rev-parse", "--show-toplevel"]);
		const info: GitInfo = { repo: basename(toplevel) };
		try {
			info.branch = git(cwd, ["rev-parse", "--abbrev-ref", "HEAD"]);
		} catch {
			// detached HEAD or unborn branch
		}
		try {
			info.headSha = git(cwd, ["rev-parse", "HEAD"]);
		} catch {
			// no commits yet
		}
		return info;
	} catch {
		return {};
	}
}

export function isDirty(cwd: string): boolean {
	try {
		return git(cwd, ["status", "--porcelain"]).length > 0;
	} catch {
		return false;
	}
}

export function gitOutput(cwd: string, args: string[]): string {
	return git(cwd, args);
}

export interface RepoPrep {
	ok: boolean;
	error?: string;
	baseSha?: string;
	branch?: string;
}

/**
 * Bring the worker's checkout to the pinned base before a task runs.
 *
 * Refusing a dirty tree is deliberate: the worker is a long-lived session, so an
 * uncommitted leftover from the previous task would silently become part of this
 * one's diff.
 */
export function prepareRepo(cwd: string, spec: RepoSpec, workerId: string): RepoPrep {
	if (!existsSync(cwd)) return { ok: false, error: `working directory does not exist: ${cwd}` };
	try {
		git(cwd, ["rev-parse", "--show-toplevel"]);
	} catch {
		return { ok: false, error: `${cwd} is not a git repository` };
	}
	if (isDirty(cwd)) {
		return { ok: false, error: "working tree has uncommitted changes; refusing to start a task on top of them" };
	}

	try {
		git(cwd, ["fetch", "--prune", "--quiet", "origin"]);
	} catch {
		// Offline or no origin: still allow a local base that already exists.
	}

	const base = spec.baseSha ?? spec.baseRef ?? "HEAD";
	try {
		git(cwd, ["rev-parse", "--verify", "--quiet", `${base}^{commit}`]);
	} catch {
		return { ok: false, error: `unknown base ${base}` };
	}

	const branch = spec.branch ?? `worker/${workerId}`;
	try {
		git(cwd, ["checkout", "--quiet", "-B", branch, base]);
	} catch (error) {
		return { ok: false, error: `checkout failed: ${(error as Error).message}` };
	}
	return { ok: true, baseSha: git(cwd, ["rev-parse", "HEAD"]), branch };
}

/** Commit whatever the task changed and return provenance for the orchestrator. */
export function collectRepoResult(
	cwd: string,
	baseSha: string | undefined,
	branch: string | undefined,
	taskId: string,
	patchDir: string,
): RepoResult {
	const result: RepoResult = {};
	if (baseSha) result.baseSha = baseSha;
	if (branch) result.branch = branch;

	try {
		if (isDirty(cwd)) {
			git(cwd, ["add", "-A"]);
			git(cwd, ["commit", "--quiet", "-m", `pi-mqtt task ${taskId}`]);
		}
	} catch {
		// Leave the tree as it is; the orchestrator can still read the diff.
	}

	try {
		result.headSha = git(cwd, ["rev-parse", "HEAD"]);
	} catch {
		return result;
	}

	if (baseSha && result.headSha) {
		if (baseSha !== result.headSha) {
			try {
				result.diffstat = git(cwd, ["diff", "--stat", `${baseSha}..${result.headSha}`]);
				const patch = git(cwd, ["format-patch", "--stdout", `${baseSha}..${result.headSha}`]);
				const patchPath = join(patchDir, `${taskId}.patch`);
				mkdirSync(dirname(patchPath), { recursive: true });
				writeFileSync(patchPath, patch);
				result.patchPath = patchPath;
			} catch {
				// Diff output is a convenience, not a correctness requirement.
			}
		}
	}
	return result;
}
