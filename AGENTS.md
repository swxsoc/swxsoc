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
- `swxsoc/util/`: Shared utilities (config, logging, schema, validation, exceptions).
- `swxsoc/net/`: Data discovery/retrieval client logic.
- `swxsoc/db/`: Database writers and related integrations.
- `swxsoc/comm/`: Outbound notifications (for example Slack).
- `swxsoc/scripts/`: Command-line entry points.
- `swxsoc/data/`: Packaged config (`config.yml`) and CDF attribute schema YAML files.
- Keep cross-cutting helpers in `swxsoc/util/`; keep package-specific helpers close to their package.

## Optional Dependency Groups
- Extras are defined in `pyproject.toml` under `[project.optional-dependencies]`:
  - `cdf`: `spacepy`, `sammi-cdf`, `matplotlib` — required for all CDF read/write/validation and SWXData plotting.
  - `fits`: no additional packages; FITS support ships via `astropy` in the base dependencies.
  - `all`: `swxsoc[cdf,fits]` — every supported file format.
  - `docs`, `test`, `style`: documentation, test, and lint/format tooling.
  - `dev`: `swxsoc[docs,test,style]`.
- The base install must stay importable and usable without any extra installed. CI enforces this with a core-only job that installs `.[test]`, alongside a full job that installs `.[all,test]`.
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
- See `swxsoc/io/cdf_handler.py`, `swxsoc/util/validation.py`, and `swxsoc/util/schema.py` (which degrades gracefully with a stub when `sammi-cdf` is missing) for reference implementations.
- Never import an optional dependency at the top level of `swxsoc/__init__.py` or in a module that base functionality imports unconditionally.
- Tests that need an extra must skip cleanly, using module-level `pytest.importorskip("spacepy.pycdf")`.
- When adding a new optional dependency, add it to an extra in `pyproject.toml`, guard the import, and confirm the core-only test run still passes.

## Build and Test
- Install development dependencies:
  - `pip install -e .[dev]` (equivalent to `.[docs,test,style]`)
  - Add file format support with `pip install -e .[all,test]` or `.[cdf,test]`.
- Run test suite (includes doctest-rst via pytest config):
  - `pytest --pyargs swxsoc --cov swxsoc`
- Verify the core-only path (no extras installed) still works:
  - `pip install -e .[test] && pytest swxsoc docs --cov swxsoc`
- Run a focused test module:
  - `pytest swxsoc/util/tests/test_config.py`
- Build docs:
  - `sphinx-build docs docs/_build/html -W -b html`
- Check reStructuredText:
  - `rstcheck -r docs`
- Run local hooks/checks:
  - `pre-commit run --all-files`

## Testing Conventions
- Place tests under package-local `tests/` directories (for example `swxsoc/util/tests/`). New test directories must also be added to `testpaths` in `pyproject.toml`.
- Name test files `test_*.py`.
- Add regression tests with bug fixes.
- Keep doctest examples runnable and aligned with real behavior.
- Gate tests that require an optional dependency with module-level `pytest.importorskip`.

## Project Conventions and Pitfalls
- Configuration is mission-aware; `SWXSOC_MISSION` can override defaults, otherwise `selected_mission` in `config.yml` wins.
- If tests mutate mission/config environment state, call `swxsoc.reconfigure()` before assertions. The autouse fixture in `swxsoc/conftest.py` resets the mission to `HERMES` per test.
- Doctests do not use pytest fixtures automatically; set required mission/config context directly in doctest examples.
- Keep documentation one sentence per line in RST files.
- For detailed guidance, reference:
  - [docs/dev-guide/code_standards.rst](docs/dev-guide/code_standards.rst)
  - [docs/dev-guide/tests.rst](docs/dev-guide/tests.rst)
  - [docs/dev-guide/docs.rst](docs/dev-guide/docs.rst)
  - [docs/user-guide/customization.rst](docs/user-guide/customization.rst) for config/mission behavior
