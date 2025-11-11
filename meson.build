# Copyright (c) 2022-2025, PostgreSQL Global Development Group

copy_jsonlines_sources = files(
  'custom_copy_formats.c',
  'jsonlines.c',
)

if host_system == 'windows'
  custom_copy_formats_source += rc_lib_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'custom_copy_formats',
    '--FILEDESC', 'custom_copy_formats - Custom COPY format implementations',])
endif

custom_copy_formats = shared_module('custom_copy_formats',
  custom_copy_formats_source,
  c_pch: pch_postgres_h,
  kwargs: contrib_mod_args,
)
contrib_targets += custom_copy_formats_source

install_data(
  'custom_copy_formats.control',
  kwargs: contrib_data_args,
)

tests += {
  'name': 'jsonlines',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'jsonlines',
    ],
  },
}
