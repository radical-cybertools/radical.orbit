"""Generate the code reference pages and navigation.

Mirrors the sibling repos (rhapsody, radical.asyncflow): one mkdocstrings
page per module under ``src/radical/orbit``, plus a literate-nav SUMMARY.
Pages land flat under ``api/`` (the ``radical.orbit`` prefix is implied).
"""

from pathlib import Path

import mkdocs_gen_files

nav = mkdocs_gen_files.Nav()

root        = Path(__file__).parent.parent
src         = root / "src"
package_dir = src / "radical" / "orbit"

with mkdocs_gen_files.open("api/index.md", "w") as fd:
    fd.write(
        "# Module Reference\n\n"
        "Auto-generated API reference for every module in the\n"
        "`radical.orbit` package.  The curated views live in\n"
        "[Plugin API Reference](../plugin_api.md) and\n"
        "[Embedding the Participant Runtime](../runtime_embedding.md).\n")

for path in sorted(package_dir.rglob("*.py")):
    if path.name.startswith("_"):        # __init__/__main__/private modules
        continue
    if "__pycache__" in path.parts:
        continue

    parts = tuple(path.relative_to(package_dir).with_suffix("").parts)

    doc_path      = Path(*parts).with_suffix(".md")
    full_doc_path = Path("api", doc_path)

    nav[parts] = doc_path.as_posix()

    with mkdocs_gen_files.open(full_doc_path, "w") as fd:
        ident = ".".join(("radical", "orbit") + parts)
        fd.write(f"# {ident}\n\n::: {ident}")

    mkdocs_gen_files.set_edit_path(full_doc_path, path)

with mkdocs_gen_files.open("api/SUMMARY.md", "w") as nav_file:
    nav_file.write("- [Overview](index.md)\n")
    nav_file.writelines(nav.build_literate_nav())
