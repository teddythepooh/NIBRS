CURRENT_DIR := $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))

raw_data_dir := $(CURRENT_DIR)/raw_data
output_dir := $(CURRENT_DIR)/output

nibrs_2022 := $(raw_data_dir)/2022_NIBRS_NATIONAL_MASTER_FILE_ENC.txt
nibrs_2023 := $(raw_data_dir)/2023_NIBRS_NATIONAL_MASTER_FILE.txt

segments := administrative \
	offense \
	arrestee \
	victim

nibrs_ascii_files := $(nibrs_2022) \
	$(nibrs_2023)

flags = $(foreach segment,$(segments),$(foreach file,$(nibrs_ascii_files),$(output_dir)/$(segment)_from_$(notdir $(file))))

define NIBRS_DECODER
$(output_dir)/$(1)_from_$(notdir $(2)): $(2)
	@echo Decoding $(1) segment from $(notdir $(2))...
	python src/decode_segments.py \
		--output_dir=$(output_dir) \
		--nibrs_master_file=$(2) \
		--config_file=configuration/col_specs.yaml \
		--segment_name=$(1)_segment \
		--to_s3 && echo "Done" > $$@
endef

$(foreach file,$(nibrs_ascii_files),$(foreach segment,$(segments),$(eval $(call NIBRS_DECODER,$(segment),$(file)))))

.DEFAULT_GOAL = all
all: $(flags)
