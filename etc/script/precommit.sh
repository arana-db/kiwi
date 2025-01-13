#!/bin/bash

# Path to the check_format.sh script
SCRIPT_PATH="etc/script/check_format.sh"

# Check if the script exists
if [ ! -f "$SCRIPT_PATH" ]; then
  echo "Error: $SCRIPT_PATH not found!"
  exit 1
fi

# Make sure the script is executable
chmod +x "$SCRIPT_PATH"

# Run the script on all staged files
#STAGED_FILES=$(git diff --cached --name-only --diff-filter=ACM | grep -E '\.(cpp|cc|h|hpp|c)$')

# if [ -n "$STAGED_FILES" ]; then
#  for file in $STAGED_FILES; do
#    "$SCRIPT_PATH" "$file"
#      if [ $? -ne 0 ]; then
#        echo "Commit aborted due to formatting issues."
#        exit 1
#      fi
#  done
# fi

sh $SCRIPT_PATH

exit 0   # Path to the check_format.sh script

