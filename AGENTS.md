# Project Guidelines

## Code Style
- Target Python 3.10+ and keep changes compatible with the existing style in each file.
- Use Ruff for lint/format:
  - `ruff check --fix`
  - `ruff format`
- Use 4 spaces for indentation and prefer f-strings.
- Avoid wildcard imports except where package export patterns require them (for example in `__init__.py`).
- Keep `__init__.py` focused on package organization/exports, not substantial implementation logic.
- Public APIs should include numpydoc-style docstrings.
- Prefer project-specific warning and exception classes (`swxsoc.util.exceptions.SWXWarning`, `SWXUserWarning`) over generic warnings.
- Use the package logger patterns from `swxsoc/__init__.py` and `swxsoc/util/logger.py`.

## Architecture
- `swxsoc/swxdata.py`: Core SWXData container for time series and metadata. Currently operates as a Data Class for managing Common Data Format (CDF) data. 
- `swxsoc/io/`: File format handlers (`base_handler.py`, `cdf_handler.py`), fill value logic (`fillval.py`), and S3 access (`s3.py`).
- `swxsoc/util/`: Shared utilities (config, logging, schema, validation, exceptions, Grafana annotations in `grafana.py`, reprocessing helpers in `reprocessing.py`).
- `swxsoc/net/`: Data discovery/retrieval client logic.
- `swxsoc/db/`: Two independent database integrations: `swxsoc/db/timeseries.py` writes scalar measurements to AWS Timestream for Grafana dashboards, and `swxsoc/db/tracker.py` + `swxsoc/db/tables/` track file-level provenance and processing status in a mission-aware relational schema. Both are optional (`tracker` extra) and guarded via `swxsoc/db/_optional.py`'s `require_tracker_dependencies()`.
- `swxsoc/comm/`: Outbound notifications via `CommsClient` subclasses (`SlackClient`, `MattermostClient`, `MultiCommsClient`); callers should use `get_comms_client()` in `factory.py`, which selects platforms from the `COMMS_PLATFORM` env var.
- `swxsoc/scripts/`: Standalone developer scripts run with `python` (no `[project.scripts]` entry points are registered).
- `swxsoc/data/`: Packaged config (`config.yml`) and CDF attribute schema YAML files.
- Keep cross-cutting helpers in `swxsoc/util/`; keep package-specific helpers close to their package.

## Optional Dependency Groups
- Extras are defined in `pyproject.toml` under `[project.optional-dependencies]`:
  - `cdf`: `spacepy`, `sammi-cdf`, `matplotlib` — required for all CDF read/write/validation and SWXData plotting.
  - `fits`: adds nothing new; it only re-lists `astropy`, which is already a base dependency.
  - `tracker`: `sqlalchemy`, `tenacity` — required for `swxsoc.db.tracker`/`swxsoc.db.tables` (MetaTracker relational file tracking). Not required for `swxsoc.db.timeseries` (Timestream writer), which only needs the base dependencies.
  - `all`: `swxsoc[cdf,fits,tracker]` — every supported file format.
  - `docs`, `test`, `style`: documentation, test, and lint/format tooling.
  - `dev`: `swxsoc[docs,test,style]`.
- The base install must stay importable and usable without any extra installed. CI enforces this with a core-only workflow (`.github/workflows/test_core.yml`, `.[test]`, Python 3.12) alongside a full workflow (`.github/workflows/testing.yml`, `.[all,test]`, Python 3.10–3.14).
- Guard optional imports at module load and fail late with an actionable message, matching the existing pattern:
  ```python
  try:
      from spacepy import pycdf

      HAS_SPACEPY = True
  except ImportError:
      HAS_SPACEPY = False

  ...

  if not HAS_SPACEPY:
      raise ImportError(
          "spacepy is required for CDF operations. "
          "Install it with: pip install swxsoc[cdf]"
      )
  ```
- See `swxsoc/io/cdf_handler.py`, `swxsoc/util/validation.py`, and `swxsoc/util/schema.py` (which degrades gracefully with a stub when `sammi-cdf` is missing) for reference implementations. `matplotlib` is instead imported lazily inside the plotting methods of `swxsoc/swxdata.py`.
- `swxsoc/db/` uses a variant of this pattern: `swxsoc/db/_optional.py` exposes `HAS_SQLALCHEMY`/`HAS_TENACITY` flags (via `find_spec`) plus a `require_tracker_dependencies(tenacity=False)` helper that raises an actionable `ImportError`.
  - `swxsoc/db/__init__.py` must stay importable in the core install, so it calls the helper inside each function body and defers `sqlalchemy` imports.
  - `swxsoc/db/tables/__init__.py` and `swxsoc/db/tracker.py` (with `tenacity=True`) call it once at module import time, so they are not importable without the `tracker` extra.
- Never import an optional dependency at the top level of `swxsoc/__init__.py` or in a module that base functionality imports unconditionally.
- Tests that need an extra must skip cleanly, using module-level `pytest.importorskip("spacepy.pycdf")` (or `"sqlalchemy"`/`"tenacity"` for tracker tests).
- Doctests are not skipped automatically:
  - RST docs that need an extra are listed by filename in `docs/conftest.py` (`cdf_doc_files`, `tracker_doc_files`); add new docs there.
  - Doctests that change the mission must set it explicitly (`os.environ["SWXSOC_MISSION"] = "hermes"` then `swxsoc.reconfigure()`); the autouse `restore_mission_config` fixture in `docs/conftest.py` restores it after each RST file.
  - New tracker modules under `swxsoc/db/` that fail at import time must be added to `collect_ignore_glob` in `swxsoc/conftest.py`, or `pytest swxsoc` fails in a core-only install.
- When adding a new optional dependency, add it to an extra in `pyproject.toml`, guard the import, and confirm the core-only test run still passes.

## Documentation Map
- Docs layout: `docs/user-guide/` (split into "Data Management" and "Pipeline Management" toctrees in `docs/user-guide/index.rst`), `docs/examples/` (runnable examples), `docs/dev-guide/` (contributor docs), and `docs/api.rst` (API reference).
- `docs/user-guide/quick_reference.rst` is a task-oriented Q&A index that links to every guide; add an entry there when you add a new user-facing guide.
- Every guide page starts with a `.. _label:` target, a 1-2 sentence summary, and a "Required install" note if it needs an extra.
- When changing code, check whether these docs need updating too:

  | Code area | Docs to check |
  | --- | --- |
  | `swxsoc/swxdata.py`, `swxsoc/io/` (CDF) | `docs/user-guide/reading_writing_data.rst`, `docs/examples/swxdata_examples.rst`, `docs/examples/tutorial1.rst`, `docs/user-guide/fillval_and_masks.rst`, `docs/user-guide/cdf_format_guide.rst`, `docs/user-guide/quick_reference.rst` |
  | `swxsoc/util/validation.py` | `docs/user-guide/cdf_validation.rst` |
  | `swxsoc/db/timeseries.py` | `docs/user-guide/recording_to_timeseries_database.rst` |
  | `swxsoc/db/tracker.py`, `swxsoc/db/tables/` | `docs/user-guide/metatracker_guide.rst` |
  | `swxsoc/util/config.py`, `config.yml`, mission selection | `docs/user-guide/customization.rst`, `docs/dev-guide/config.rst` |
  | `swxsoc/comm/` | `docs/user-guide/comms_clients.rst` |
  | `swxsoc/util/grafana.py` | `docs/user-guide/grafana_annotation_management.rst` |
  | `swxsoc/net/` | `docs/user-guide/retrieving_data.rst` |
  | `swxsoc/util/logger.py` | `docs/user-guide/logger.rst` |
  | `swxsoc/util/schema.py`, CDF attribute schemas | `docs/user-guide/schema_information_guide.rst` |
  | Any public API addition/removal | `docs/api.rst` (`automodapi` entries) |
  | `pyproject.toml` optional-dependencies changes | This file's "Optional Dependency Groups" section, `docs/user-guide/installation.rst`, `docs/dev-guide/dev_env.rst` |
  | Environment variables read by `swxsoc` | `docs/user-guide/customization.rst` ("Environment Variables" table) |
  | Any user-facing behavior change | `CHANGELOG.rst` at the repo root (pulled into the docs by `docs/whatsnew/changelog.rst`) |

## Build and Test
- Install development dependencies:
  - `pip install -e .[dev]` (equivalent to `.[docs,test,style]`)
  - Add file format support with `pip install -e .[all,test]` or `.[cdf,test]`.
- Run the full test suite as CI does (package tests plus RST doctests under `docs/`):
  - `pytest swxsoc docs --cov swxsoc`
  - CI sets `MPLBACKEND=Agg`; set it locally too if plotting tests open windows.
- Verify the core-only path (no extras installed) still works:
  - `pip install -e .[test] && pytest swxsoc docs --cov swxsoc`
- Run a focused test module:
  - `pytest swxsoc/util/tests/test_config.py`
- Build docs (needs `pip install -e .[docs,all]`, because API pages import optional modules; warnings are errors):
  - `sphinx-build docs docs/_build/html -W -b html`
  - The build also writes `llms.txt` and `llms-full.txt` (via `sphinx-llms-txt`) to `docs/_build/html`; `llms-full.txt` is a plain-text copy of all docs pages and is a quick way to search the rendered docs.
- Check reStructuredText:
  - `rstcheck -r docs`
- Run local hooks/checks:
  - `pre-commit run --all-files`

## Testing Conventions
- Place tests under package-local `tests/` directories (for example `swxsoc/util/tests/`). New test directories need an `__init__.py` and must be added to `testpaths` in `pyproject.toml`.
- Name test files `test_*.py`.
- Add regression tests with bug fixes.
- Keep doctest examples runnable and aligned with real behavior.
- Code examples that use the `swxsoc` package must use the `.. doctest::` directive and be executed by the CI tests to ensure reliability.
  Use `# doctest: +SKIP` only for calls that need external services or credentials.
  Shell, YAML, and other non-Python examples may remain in `.. code-block::`.
- Gate tests that require an optional dependency with module-level `pytest.importorskip`.

## Project Conventions and Pitfalls
- Configuration is mission-aware; `SWXSOC_MISSION` can override defaults, otherwise `selected_mission` in `config.yml` wins.
- If tests mutate mission/config environment state, call `swxsoc.reconfigure()` before assertions. The autouse fixture in `swxsoc/conftest.py` resets the mission to `HERMES` per test.
- `swxsoc.reconfigure()` only reloads `swxsoc.config`; it does **not** rebuild `swxsoc.db.tables`' ORM classes. Any code that changes the active mission and also uses the tracker (`swxsoc.db.tracker`/`swxsoc.db.tables`) must call `swxsoc.db.reconfigure()` immediately afterward (guarded on `swxsoc.db.HAS_SQLALCHEMY`), or the tracker's table names/columns will still reflect the previous mission. `swxsoc/conftest.py`'s `default_test_mission` and `use_mission` fixtures do this automatically and are the reference pattern.
- Doctests do not use pytest fixtures automatically; set required mission/config context directly in doctest examples.
- Keep documentation one sentence per line in RST files.
- For detailed guidance, reference:
  - [docs/dev-guide/code_standards.rst](docs/dev-guide/code_standards.rst)
  - [docs/dev-guide/tests.rst](docs/dev-guide/tests.rst)
  - [docs/dev-guide/docs.rst](docs/dev-guide/docs.rst)
  - [docs/user-guide/customization.rst](docs/user-guide/customization.rst) for config/mission behavior
  - [docs/user-guide/metatracker_guide.rst](docs/user-guide/metatracker_guide.rst) for the `swxsoc.db` tracker/reconfigure behavior
