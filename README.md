# Bits x Pages Experiments

Scratchpad repo for the "Bits x Pages" blog. Every post gets its own standalone project under `experiments/<topic>` so tools, dependencies, and build systems stay isolated. Nothing in the repo root assumes a particular language or framework—drop whatever you need into a new experiment folder and document how to run it in that folder.

## Layout

- `experiments/`: one subdirectory per post or idea. Each directory owns its source, build files, helper scripts, and README.
- `experiments/lsm-space-amp`: current RocksDB block-size/space amplification benchmark (C++/CMake).
- `build/`: example build tree for the LSM experiment (feel free to create separate `build/<experiment>` directories per project).
- `space_amp_runs/`: output from the LSM benchmark (delete between runs if desired).

## Adding a new experiment

1. Create `experiments/<topic>` and drop in your code.
2. Include a `README.md` that explains prerequisites and how to build/run.
3. Use whatever tooling fits (CMake, Bazel, Python scripts, notebooks, etc.). Keep builds out-of-tree (e.g., `build/<topic>`).

## Running an existing experiment

Navigate to its directory, read the README, and follow the instructions. For example, the LSM space amplification benchmark includes its own CMake project—build it with:

```bash
cmake -S experiments/lsm-space-amp -B build/lsm-space-amp
cmake --build build/lsm-space-amp -j
./build/lsm-space-amp/space_amp --block_sizes=4096,8192,32768
```

Future experiments can provide completely different commands; just keep everything self-contained inside the experiment directory so posts remain reproducible.
