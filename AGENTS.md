# AGENTS

This repo intentionally hosts many unrelated experiments. If you're an automated agent working here:

1. **Stay inside the experiment you're touching.** Each subdirectory under `experiments/` is self-contained with its own build/run instructions. Don't assume global tooling; read the experiment's README before making changes.
2. **Use out-of-tree builds.** Never drop build artifacts inside source directories. Prefer `build/<experiment>` or another clearly isolated path.
3. **Keep dependencies explicit.** If you add a new experiment, document prerequisites and CLI usage in that experiment's README so others (and future agents) can reproduce it.
4. **Don't rewrite other experiments.** Avoid touching directories unrelated to the task unless explicitly asked.

Following these rules keeps the repo tidy and ensures each blog post experiment remains reproducible on its own.
