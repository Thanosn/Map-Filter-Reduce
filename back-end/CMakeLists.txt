


set(BACKENDLIB)

file(GLOB SRC_CC "${CMAKE_CURRENT_SOURCE_DIR}/RDD.cpp")
file(GLOB SRC_H "${CMAKE_CURRENT_SOURCE_DIR}/*.h*")

foreach(_file ${SRC_CC})
    get_filename_component(CC_FILE ${_file} NAME)
    list(APPEND BACKENDLIB ${CC_FILE})
endforeach()

foreach(_file ${SRC_H})
    get_filename_component(H_FILE ${_file} NAME)
    list(APPEND BACKENDLIB ${H_FILE})
endforeach()


add_library(backEndLib STATIC ${BACKENDLIB})
