# Copyright (c) 2025-present, Arana/Kiwi Community.  All rights reserved.
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.


# define hooks source and target path
SET(HOOKS_SOURCE_DIR "${CMAKE_SOURCE_DIR}/etc/script")
set(GIT_HOOKS_DIR "${CMAKE_SOURCE_DIR}/.git/hooks")

# ensure pre-commit hook is installed
ADD_CUSTOM_TARGET(install_git_hooks
    COMMAND ${CMAKE_COMMAND} -E make_directory ${GIT_HOOKS_DIR}
    COMMAND ${CMAKE_COMMAND} -E copy_if_different
        ${HOOKS_SOURCE_DIR}/precommit.sh
        ${GIT_HOOKS_DIR}/pre-commit
    COMMAND ${CMAKE_COMMAND} -E chmod 755 ${GIT_HOOKS_DIR}/pre-commit
    COMMENT "Installing git hooks..."
)
