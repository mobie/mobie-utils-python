# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`mobie` is a Python library (PyPI/conda name `mobie_utils`, import name `mobie`) for generating, populating, validating, and migrating [MoBIE](https://github.com/mobie/mobie) projects. A MoBIE project is a folder structure of large bioimage data plus JSON metadata that the MoBIE Fiji viewer reads. This library's job is to convert raw image/segmentation/spot data into MoBIE-compatible storage and to write spec-compliant metadata.

## Commands

```sh
# Environment (conda required — pulls in the C/Java-backed image deps)
conda env create -f environment.yaml
conda activate mobie
pip install -e .

# Tests (this is exactly what CI runs)
python -m unittest discover -s test -v

# A single test module / case / method
python -m unittest test.test_image_data
python -m unittest test.metadata.test_view_metadata.TestViewMetadata
python -m unittest test.test_image_data.TestImageData.test_add_image
```

Tests write scratch projects under `test/test-folder/` and `tmp*/` (both gitignored).

### Linting

We lint with **pyflakes** and **flake8**. There is no config file, so pass the line length on the command line — the codebase follows a ~120-char limit (the default 79 does not apply):

```sh
pyflakes mobie test
flake8 --max-line-length=120 mobie test
```

Run these before considering a change done.

## Architecture

The library is layered. A typical `mobie.add_*` call flows top-to-bottom through these layers:

1. **Public API (`mobie/__init__.py`)** — the `add_*` entry functions: `add_image`/`add_bdv_image` (`image_data.py`), `add_segmentation`, `add_spots`, `add_traces`, `add_registered_source`, `add_open_organelle_data`, plus view/source helpers. Every `add_*` function follows the same three-step shape: (a) `utils.require_dataset_and_view(...)` creates the project/dataset and the default view if needed, (b) the data is converted via the import layer, (c) `metadata.add_source_to_dataset(...)` writes the source + view into `dataset.json`. When changing one `add_*`, check whether the others need the same change — they are near-parallel.

2. **Import layer (`mobie/import_data/`)** — converts raw arrays/files into MoBIE storage formats (`ome.zarr`, `bdv.n5`, `bdv.hdf5`). This is the heavy compute path: it runs **cluster_tools luigi workflows** (`DownscalingWorkflow`, `DataStatisticsWorkflow`, `NodeLabelWorkflow`) for parallel downscaling/pyramid generation. The `target` argument (`"local"` or a cluster target like `"slurm"`) and `max_jobs` thread all the way down from the public API into these workflows. `import_data/utils.py` is the shared engine; `image.py`/`segmentation.py`/`traces.py` are thin format-specific wrappers.

3. **Metadata layer (`mobie/metadata/`)** — pure read/write/construct of the spec JSON. This is the core of the project structure:
   - `project_metadata.py` → `project.json` (lists datasets, default dataset)
   - `dataset_metadata.py` → `dataset.json` (sources + views) and the on-disk dataset folder layout
   - `source_metadata.py` → individual image/segmentation/spot/region source entries
   - `view_metadata.py` → constructs **views** (the central MoBIE concept: a named arrangement of sources with displays + transforms). `get_view`, `get_default_view`, displays (`get_image_display`, …), and source transforms (affine, crop, grid, merged-grid) all live here.
   - There is **no data here, only JSON** — keep heavy I/O out of this layer.

4. **Validation layer (`mobie/validation/`)** — validates metadata dicts against JSON schemas with `jsonschema`. Schemas are **downloaded from `mobie.github.io` and cached in `~/.mobie/`**. Gotcha: the cache is never invalidated automatically — after a spec change you must delete `~/.mobie/` (or the relevant `*.schema.json`) or validation runs against stale schemas. `add_*` functions validate views as they build them, so a schema mismatch surfaces during import, not just in the explicit `validate_*` CLIs.

5. **Tables (`mobie/tables/`)** — generates the TSV tables MoBIE attaches to segmentations, spots, regions, and traces.

6. **HTM (`mobie/htm/`)** — high-throughput / high-content microscopy: batch `add_images`/`add_segmentations` plus plate/well grid-view construction.

7. **Migration (`mobie/migration/`)** — upgrades existing projects between spec versions (`migrate_v1`/`v2`/`v3`). Run via `mobie.migrate_project` / `mobie.migrate_dataset`. These rewrite metadata **in place without backups** — guard with git or a manual copy.

### CLI

`setup.py` registers `console_scripts` of the form `mobie.<action>` (e.g. `mobie.add_image`, `mobie.validate_project`, `mobie.migrate_project`), each mapping to a `main()` in the corresponding module. The shared `utils.get_base_parser()` defines the common spatial arguments (`--resolution`, `--scale_factors`, `--chunks`, all JSON-encoded), so CLI changes for sources usually belong there.

## Conventions and gotchas

- **Axis order:** internal numpy/array data is `zyx`; MoBIE and BDV metadata are `xyz`. Use `utils.transformation_to_xyz()` to convert affine parameters before writing them — do not hand-build xyz transforms.
- **Supported formats** are the `FILE_FORMATS` list in `mobie/utils.py`. `add_image` defaults to `ome.zarr`. Format-specific path layout lives in `utils.get_internal_paths()` / `get_data_key()`.
- **Two versions** in `mobie/__version__.py`: `__version__` is the library release; `SPEC_VERSION` is the MoBIE spec version the library writes. Bump `SPEC_VERSION` only alongside a spec migration.
- **NumPy in JSON:** metadata is dumped with `metadata/utils.py::NPTypesEncoder`, which coerces numpy scalar types — return values from import code can carry np dtypes safely.
- **Docstrings:** use Google style (`Args:` / `Returns:` / `Raises:` sections), matching the existing public functions. Mark internal/helper functions with a `"""@private"""` docstring so they are excluded from the generated API docs.

## Current task: migrate from cluster_tools to bioimage-py

This work replaced the `cluster_tools` compute backend with **`bioimage-py`**. `environment.yaml` already swaps `cluster_tools`/`numba`/`python-elf<0.9` for `bioimage-py` + `python-elf>0.9`. **The migration is complete: `cluster_tools` is no longer imported anywhere in the mobie source, and both additional goals — dropping `vigra` and supporting ome.zarr on zarr v2 *and* v3 — are done too** (see the ome.zarr version-support bullet below).

**Progress so far:** the **foundation shim** (`mobie/utils.py::get_run_config`) and the **core downscaling path** (`mobie/import_data/utils.py::downscale`, `compute_max_id`, plus the new `mobie/import_data/_format_metadata.py`) are migrated to bioimage-py and tested. The import path now reads **all** inputs through `bp.open_source` consistently — `downscale` plus `image_data.py::_get_default_contrast_limits` — so mobie can ingest every format bioimage-py supports (hdf5/n5/zarr/**tif**/**mrc**/**nifti**/msr/knossos); mrc and nifti import are now tested (`test/import_data/test_image.py`). The 2d→3d promotion for bdv outputs no longer writes an on-disk temp file: `ensure_volume` was **deleted** and `downscale` promotes 2d sources on the fly via the new bioimage-py wrapper primitive **`ExpandDimsSource`** (added to `bioimage_py.wrapper`). The **segmentation-table morphology workflow** is migrated too: `tables/default_table.py` now drives `bp.morphology.morphology` (+ optional `bp.morphology.regionprops` for anchor correction) for a unified 2d/3d path, and the **HTM table path** (`htm/data_import.py::_add_tables`) computes tables by parallelizing `compute_default_table` over images via `bp.get_runner().map` (the old `htm/table_impl.py` luigi task was deleted). **`vigra` is now fully gone from the source** (the table paths were its last importers). The **HTM copy path** (`htm/data_import.py::_copy_image_data`, used by `add_images`/`add_segmentations`) is migrated too: it now parallelizes over sources via `bp.get_runner().map` (one task per source) and routes each through the single-source import path (`import_image_data` / `import_segmentation`) — so `htm/data_import.py` is fully cluster_tools/luigi-free. The **registration** paths (affine/transformix/coordinate) were migrated onto `bp.wrapper.AffineSource`/`bp.transformation` + the `transformix` CLI dispatched via `bp.get_runner().map`. Finally, the **node-label import** (`import_data/from_node_labels.py`) was migrated onto **`bp.segmentation.relabel`** and the outdated **paintera project import** (`import_data/paintera.py`) was removed — which made `utils.py::write_global_config`/`BaseClusterTask` dead code, so they were deleted. **`cluster_tools`/`luigi` are no longer imported anywhere in `mobie/`** (only historical references survive in comments).

The migration means rewriting the layer-2 import code to drive `bioimage-py` instead of cluster_tools luigi workflows. **The agreed scope, gap analysis, and per-decision rationale live in the project memory node [[bioimage-py-migration-decisions]] — read it before starting any migration work.** Key points to keep in mind:

- **bioimage-py provides *primitives*; mobie *composes* them.** Do NOT take "implement what is missing in bioimage-py" to mean adding off-the-shelf, mobie-specific high-level functions there. bioimage-py stays a clean, general primitive library (block-wise `copy`/`downsample`/`stats`/`morphology`, the runner, source wrappers). When something is missing, add the missing *primitive* (e.g. a `SourceWrapper`) — and keep MoBIE-specific orchestration and all spec/format-metadata writing here in mobie. Concretely: the multiscale-pyramid loop and the ome.zarr/bdv metadata writing (via pybdv / ome-zarr) belong in mobie, not bioimage-py.
- **Execution model:** bioimage-py replaces the luigi `*Workflow` + `luigi.build` + on-disk `*.config`/`global.config` model with a runner — every op takes `job_type` (`local`/`subprocess`/`slurm`), `num_workers`, `block_shape`, and a `RunnerConfig`/`SlurmConfig`. This collapses `write_global_config`, the per-task `.config` files, and `BaseClusterTask` into per-call kwargs + one config object. Concentrate changes in `mobie/import_data/utils.py` (the shared engine) and `mobie/utils.py`; the format-specific wrappers and the metadata/validation layers should be largely unaffected.
- Preserve the public-API surface (`target`, `max_jobs`, function signatures) so callers and the `add_*` flow described above keep working. Note: **LSF is dropped** (only `local`/`subprocess`/`slurm` are supported), and the legacy `int_to_uint` option has been **removed**.
- **ome.zarr version support (zarr v2 *and* v3): DONE.** The ome.zarr / NGFF version is selected via a `@` suffix on `file_format` (e.g. `file_format="ome.zarr@0.5"`; no suffix ⇒ default `"0.4"`). `mobie/utils.py::parse_file_format` splits the version off at each `add_*` entry — so the on-disk folder name and the `dataset.json` source key stay the canonical `ome.zarr` — and threads an internal `ome_zarr_version` through the import layer. `0.4`→zarr v2, `0.5`→zarr v3, **both written via z5py** (`z5py.File(..., zarr_format=3)`; z5py 3.0.2 supports v3 + sharding, so `elf.io.open_file` is still avoided for writing). v0.5 writes its NGFF metadata under `zarr.json`'s `attributes.ome` (version at the `ome` level); the v2-vs-v3 on-disk layout (`.zattrs`/`.zarray` vs `zarr.json`) is handled on read by helpers in `mobie/validation/utils.py` (`load_ngff_group_attrs`/`load_ngff_array_shape`/`ngff_multiscales`/`ngff_version`, shared by the local and s3 paths), and validation selects the matching NGFF 0.4 vs 0.5 schema (the 0.5 schema's external `$ref` is resolved via a `referencing.Registry`). A **`shards`** kwarg (on the `add_*`/import functions + a `--shards` CLI flag) enables zarr-v3 sharding and is only valid for `ome.zarr@0.5`. Full design + rationale: the 2026-07-03 increment in [[bioimage-py-migration-decisions]].
- **Additional goal — drop the `vigra` dependency: DONE.** No `import vigra` remains in the source (the morphology-table 2d path + `htm/table_impl.py` were its last users). `vigra` was never an explicit env-file entry, so no env change was needed; `cluster_tools` may still pull it in transitively until that dependency is removed.
