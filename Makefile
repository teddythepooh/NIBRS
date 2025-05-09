CURRENT_DIR := $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))

raw_data_dir := $(CURRENT_DIR)/raw_data
output_dir := $(CURRENT_DIR)/output

scripts := $(CURRENT_DIR)/src
core_logic := $(scripts)/utils/nibrs_decoder.py
unzipper := $(scripts)/unzip.py
decoder := $(scripts)/decode_segments.py

SEGMENTS := administrative \
	offense \
	arrestee \
	victim

YEARS := 2022 \
	2023

### TARGETS
# There are the NIBRS fixed-length, ASCII .txt files.
ASCII_FILES := $(foreach year,$(YEARS),$(raw_data_dir)/nibrs-$(year).txt)

# These are dummy flags that signify whether the decoded NIBRS segments in 
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
	@echo Decoding $(1) segment from $(notdir $(2))...
	python $(decoder) \
		--output_dir=$(output_dir) \
		--nibrs_master_file=$(2) \
		--config_file=configuration/col_specs.yaml \
		--segment_name=$(1)_segment \
		--to_s3 && echo "Done" > $$@
endef

$(foreach year,\
	$(YEARS),$(eval $(call NIBRS_UNZIP,$(year))))

$(foreach file,$(ASCII_FILES),\
	$(foreach segment,$(SEGMENTS),\
		$(eval $(call NIBRS_DECODER,$(segment),$(file)))))

.DEFAULT_GOAL = all
all: $(FLAGS)
