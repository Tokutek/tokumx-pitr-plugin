add_library(exampleplugin SHARED
  example
  )

install(TARGETS exampleplugin
  DESTINATION ${INSTALL_LIBDIR}/plugins
  COMPONENT tokumx_plugins
  )
