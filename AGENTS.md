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
- `swxsoc/util/`: Shared utilities (config, logging, schema, validation, I/O helpers (CDF read/write) ).
- `swxsoc/net/`: Data discovery/retrieval client logic.
- `swxsoc/db/`: Database writers and related integrations.
- Keep cross-cutting helpers in `swxsoc/util/`; keep package-specific helpers close to their package.

## Build and Test
- Install development dependencies:
  - `pip install -e .[test,docs,style]`
- Run test suite (includes doctest-rst via pytest config):
  - `pytest --pyargs swxsoc --cov swxsoc`
- Run a focused test module:
  - `pytest swxsoc/util/tests/test_config.py`
- Build docs:
  - `sphinx-build docs docs/_build/html -W -b html`
- Check reStructuredText:
  - `rstcheck -r docs`
- Run local hooks/checks:
  - `pre-commit run --all-files`

## Testing Conventions
- Place tests under package-local `tests/` directories (for example `swxsoc/util/tests/`).
- Name test files `test_*.py`.
- Add regression tests with bug fixes.
- Keep doctest examples runnable and aligned with real behavior.

## Project Conventions and Pitfalls
- Configuration is mission-aware; `SWXSOC_MISSION` can override defaults.
- If tests mutate mission/config environment state, reinitialize config using project reconfiguration patterns before assertions.
- Doctests do not use pytest fixtures automatically; set required mission/config context directly in doctest examples.
- Keep documentation one sentence per line in RST files.
- For detailed guidance, reference:
  - `docs/dev-guide/code_standards.rst`
  - `docs/dev-guide/tests.rst`
  - `docs/dev-guide/docs.rst`
