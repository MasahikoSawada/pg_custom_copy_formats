MODULE_big = pg_custom_copy_formats
OBJS = \
	$(WIN32RES) \
	pg_custom_copy_formats.o \
	jsonlines.o

EXTENSION = pg_custom_copy_formats
PGFILEDESC = "custom copy format implementations"

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
