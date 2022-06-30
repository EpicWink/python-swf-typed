# Sphinx documentation generation

SPHINXOPTS    ?=
SPHINXBUILD   ?= sphinx-build
SOURCEDIR     = src
BUILDDIR      = build
TEMPLATESDIR  = templates

# "make" == "make html"
html: .deps
	$(SPHINXBUILD) -M html "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

.PHONY: html Makefile clean .deps

# Sphinx make-mode
%: Makefile
	$(SPHINXBUILD) -M $@ "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

.deps:
	pip install -r requirements.txt

clean:
	rm -rvf build src/swf_typed*.rst
