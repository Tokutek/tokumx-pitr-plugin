add_library(pitr_plugin SHARED
  pitr
  )
add_dependencies(pitr_plugin install_tdb_h generate_error_codes generate_action_types)

install(TARGETS pitr_plugin
  DESTINATION ${INSTALL_LIBDIR}/plugins
  COMPONENT tokumx_plugins
  )
