# Plan: #32 — Package installation: pyproject.toml only

Issue: https://github.com/radical-cybertools/radical.orbit/issues/32
Status check (2026-08-25): **partially done**:
- `python_requires >= 3.10` — done (setup.py:205; CI tests 3.10–3.12).
- `radical.pilot` optional — de-facto done: it is not in requirements.txt at
  all; the lucid plugin is the only consumer (verify it imports lazily /
  `is_enabled`-gates on availability so a missing RP never breaks load).
- Plugin auto-load concern — done since: plugins are selected via
  `--plugins` / `plugins=` with role-based `default` sets
  (plugin_host_base.py, `DEFAULT_PLUGINS_BY_ROLE`), and missing-dependency
  plugins are skipped/isolated at load.
- **Remaining**: the actual conversion — metadata still lives in `setup.py`
  (+ setup.cfg); `pyproject.toml` only declares the build backend.

## Work

1. Move all static metadata into `pyproject.toml` `[project]`: name,
   description, readme, license (MIT — keep consistent with PR #112's fix),
   authors, urls, `requires-python = ">=3.10"`, classifiers,
   `dependencies` (from requirements.txt), `[project.scripts]` /
   script-files for `bin/` entries (note: `radical-orbit-endpoint-wrapper.sh`
   is a shell script — needs `[tool.setuptools]` `script-files`, or move to
   data + console shim; check how setup.py ships it today).
2. Dynamic version: keep the VERSION-file + git-tag scheme
   (`setup.py:get_version`) via
   `[tool.setuptools.dynamic]` + a tiny `setup.py` shim **or** migrate to
   `setuptools-scm`. Decision: keep the existing scheme with
   `dynamic = ["version"]` and a minimal remaining `setup.py` that only
   computes version — smallest diff, sdist/wheel behavior unchanged. (Full
   setup.py deletion only if the version logic ports cleanly to a
   `[tool.setuptools.dynamic].version = {attr = ...}` helper module.)
3. Package data: `find_namespace_packages` under `src/` →
   `[tool.setuptools.packages.find] where = ["src"]`, plus the data files
   (`data/*.html`, `data/plugins/*.js`, `data/*.json`, VERSION) —
   currently in setup.py `package_data`; also keep `docs/requirements.txt`
   in the sdist (PR #114 just added that — don't regress it).
4. Optional-dependency extras while at it:
   `[project.optional-dependencies]` e.g. `rp = [radical.pilot]`,
   `globus = [globus-sdk]`, `rhapsody = [rhapsody-py]`, `psij =
   [psij-python]` — and slim the hard `dependencies` down to the
   transport/core set (httpx, msgpack, websockets, fastapi, uvicorn,
   pydantic, cloudpickle, psutil, rich). Plugins already degrade gracefully
   when an import is missing (verify per plugin: rhapsody, globus, psij
   guard their imports). This delivers the issue's "explicit plugin
   requirements" in the modern form.
5. Delete setup.cfg (check what it still carries — likely flake8/metadata
   remnants; .flake8 exists separately).
6. Verify: `pip install .` in a clean venv; `python -m build` sdist+wheel;
   conda recipe (PR #111) still builds — its meta.yaml parses setup
   metadata, update if it pins on setup.py; CI matrix green.

## Risks

- The wrapper shell script install path and the VERSION/git-tag derivation
  are the two places pyproject conversions typically break — test both from
  an sdist install, not just a repo install.
- Slimming hard deps (step 4) changes default installs: `pip install
  radical.orbit` no longer pulls psij/rhapsody. Announce in the changelog;
  the role-default plugin sets already skip uninstalled plugins, so
  behavior degrades cleanly. If that is too aggressive for one PR, split
  step 4 into a follow-up.

## Effort

Small-medium, one PR (or two if extras split out). Mostly mechanical;
the test is the packaging matrix, not pytest.
