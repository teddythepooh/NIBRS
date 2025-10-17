#CURRENT_DIR := $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))

raw_data_dir := ./raw_data
output_dir := ./output

scripts := ./extract_and_load

core_logic := $(scripts)/core/nibrs.py
unzipper := $(scripts)/unzip.py
decoder := $(scripts)/decode.py

SEGMENTS := administrative \
	offense \
	arrestee \
	victim

# Dynamically extract years from file names of form nibrs-${year}.zip in $(raw_data_dir).
YEARS := $(patsubst $(raw_data_dir)/nibrs-%.zip,%,$(wildcard $(raw_data_dir)/nibrs-*.zip))

### TARGETS
ASCII_FILES := $(foreach year,$(YEARS),$(raw_data_dir)/nibrs-$(year).txt)

# These are dummy flags that signify whether the NIBRS segments on
# Amazon S3 are indeed up-to-date with their underlying dependencies.
FLAGS := $(foreach segment,$(SEGMENTS),\
			$(foreach file,$(ASCII_FILES),\
				$(output_dir)/$(segment)_segment_from_$(notdir $(file))))

### RULES
define NIBRS_UNZIP
$(raw_data_dir)/nibrs-$(1).txt: $(raw_data_dir)/nibrs-$(1).zip $(unzipper) $(core_logic)
	@echo Unzipping $$< ...
	python $(unzipper) -f $$<
endef

define NIBRS_DECODER
$(output_dir)/$(1)_segment_from_$(notdir $(2)): $(2) $(decoder) $(core_logic)
	@echo Decoding $(1) segment from file $(notdir $(2))...
	python $(decoder) \
		--output_dir=$(output_dir) \
		--config_file=configuration/col_specs.yml \
		--to_s3 \
		--nibrs_master_file=$(2) \
		--segment_name=$(1)_segment && echo "Done" > $$@
endef

$(foreach year,$(YEARS),\
	$(eval $(call NIBRS_UNZIP,$(year))))

$(foreach file,$(ASCII_FILES),\
	$(foreach segment,$(SEGMENTS),\
		$(eval $(call NIBRS_DECODER,$(segment),$(file)))))

.DEFAULT_GOAL = all
all: $(FLAGS)
