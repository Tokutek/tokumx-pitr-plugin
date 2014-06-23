add_library(pitr_plugin SHARED
  pitr
  )

install(TARGETS pitr_plugin
  DESTINATION ${INSTALL_LIBDIR}/plugins
  COMPONENT tokumx_plugins
  )
