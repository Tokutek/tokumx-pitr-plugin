add_library(point_in_time_recovery SHARED
  pir
  )

install(TARGETS point_in_time_recovery
  DESTINATION ${INSTALL_LIBDIR}/plugins
  COMPONENT tokumx_plugins
  )
