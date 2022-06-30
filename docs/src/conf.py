"""Sphinx documentation generation configuration."""

import types
import inspect
import pathlib
import datetime
import importlib.metadata

import swf_typed

project = "swf-typed"
copyright = f"{datetime.date.today().year}, Laurie O"
author = "Laurie O"
release = importlib.metadata.version("swf-typed")  # full version
version = ".".join(release.split(".")[:2])  # short X.Y version

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.viewcode",
    "sphinx.ext.napoleon",
    "sphinx.ext.autosummary",
    "sphinx.ext.intersphinx",
]

html_theme = "sphinx_rtd_theme"

autosummary_generate = False
intersphinx_mapping = {'python': ('https://docs.python.org/3', None)}


def _generate_api_docs() -> None:
    source_dir = pathlib.Path(__file__).parent
    swf_typed_exports = inspect.getmembers(
        swf_typed, lambda x: not isinstance(x, types.ModuleType)
    )
    swf_typed_modules = inspect.getmembers(
        swf_typed, lambda x: isinstance(x, types.ModuleType)
    )

    module_rst_references = []
    for module_name, module in swf_typed_modules:
        exports_for_module = []
        for name, export in swf_typed_exports:
            if getattr(export, "__module__", None) == module.__name__:
                exports_for_module.append((name, export))

        if not exports_for_module:
            continue

        title = " ".join(w.capitalize() for w in module_name.strip("_").split("_"))
        lines = [
            title,
            "=" * len(title),
            "",
            module.__doc__,
            "",
            ".. currentmodule:: swf_typed",
            "",
            ".. autosummary::",
            "   :nosignatures:",
            "",
        ]
        lines += [f"   {n}" for n, _ in exports_for_module]

        for name, export in exports_for_module:
            if isinstance(export, type):
                if issubclass(export, Exception):
                    type_name = "exception"
                else:
                    type_name = "class"
            elif isinstance(export, types.FunctionType):
                type_name = "function"
            else:
                type_name = "data"

            lines += ["", f".. auto{type_name}:: {name}"]
            if type_name == "class":
                lines += [
                    "   :inherited-members:",
                    "   :members:",
                    "   :undoc-members:",
                ]
            elif type_name == "exception":
                lines += ["   :members:"]
            lines += [""]

        module_rst_reference = f"swf_typed.{module_name}"
        module_rst_references.append(module_rst_reference)
        module_path = source_dir / f"{module_rst_reference}.rst"
        module_path.write_text("\n".join(lines))

    lines = [
        r"swf\_typed",
        "==========",
        "",
        ".. automodule:: swf_typed",
        "",
        ".. toctree::",
        "   :maxdepth: 1",
        "",
    ]
    lines += [f"   {n}" for n in module_rst_references]
    lines += [""]

    api_docs_path = source_dir / f"swf_typed.rst"
    api_docs_path.write_text("\n".join(lines))


_generate_api_docs()
