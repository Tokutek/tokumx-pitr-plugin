add_library(pitr_plugin SHARED
  pitr
  )
add_dependencies(pitr_plugin install_tdb_h generate_error_codes generate_action_types)

install(TARGETS pitr_plugin
  DESTINATION ${INSTALL_LIBDIR}/plugins
  COMPONENT tokumx_plugins
  )

if (TOKUMX_ENTERPRISE_CREATE_EXPORTS)
  file(RELATIVE_PATH _relative_source_dir "${CMAKE_SOURCE_DIR}" "${CMAKE_CURRENT_SOURCE_DIR}")
  install(TARGETS pitr_plugin DESTINATION "${_relative_source_dir}" COMPONENT tokumx_enterprise_exports EXPORT tokumx_enterprise_exports)
  exports_install(pitr_plugin "\${INSTALL_LIBDIR}/plugins" tokumx_plugins)
  unset(_relative_source_dir)
endif ()
