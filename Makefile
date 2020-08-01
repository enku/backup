export $(CURDIR)/.venv/bin:$(PATH)

appname := $(shell python3 setup.py --name)
appversion := $(shell python3 setup.py --version)
venv := .venv/.dirstate
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
lint: $(venv)
	pylint --rcfile=.pylintrc $(sources)

$(venv): dev_requirements.txt
	python3 -m venv .venv
	pip3 install -r $<
	touch $@

clean:
	rm -rf .venv dist build 
