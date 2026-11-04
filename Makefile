DRAWIO ?= drawio

DIAGRAMS := $(patsubst %.drawio,%.drawio.png,$(wildcard docs/img/*.drawio))

.PHONY: diagrams
diagrams: $(DIAGRAMS)

%.drawio.png: %.drawio
	$(DRAWIO) -x -f png -e -b 10 -o $@ $<
