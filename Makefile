appname := $(shell pdm show --name)
appversion := $(shell pdm show --version)
sources := backup.py purgebackups.py

.PHONY: build
build: sdist wheel

.PHONY: sdist
sdist: dist/$(appname)-$(appversion).tar.gz

.PHONY: wheel
wheel: dist/$(appname)-$(appversion)-py3-none-any.whl

dist/$(appname)-$(appversion)-py3-none-any.whl: setup.py $(sources)
	python3 $< bdist_wheel

dist/$(appname)-$(appversion).tar.gz: setup.py $(sources)
	python3 $< sdist

.PHONY: lint
lint:
	pylint $(sources)

clean:
	rm -rf .venv dist build __pypackages__
