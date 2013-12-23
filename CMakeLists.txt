add_library(exampleplugin SHARED
  example
  )

install(TARGETS backup_plugin
  DESTINATION ${INSTALL_LIBDIR}/plugins
  COMPONENT tokumx_plugins
  )