#!/bin/bash

# Check if clang-format is installed
if ! command -v clang-format &> /dev/null; then
    echo "Error: clang-format is not installed"
    echo "Please install clang-format first"
    exit 1
fi

# Set color output
RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color

# Counters
error_count=0
checked_count=0

# Check file
check_file() {
    local file=$1
    # Create a temporary file with formatted content
    clang-format $file > temp.txt
    
    # Compare the original file with the formatted file
    if ! diff -u "$file" temp.txt > /dev/null; then
        echo -e "${RED}Needs formatting: $file${NC}"
        # Show specific differences
        diff -u "$file" temp.txt | grep -E "^[\+\-]" | head -n 10
        echo "..."
        ((error_count++))
    else
        echo -e "${GREEN}Correctly formatted: $file${NC}"
    fi
    ((checked_count++))
    
    # Clean up temporary file
    rm temp.txt
}

# Main function
main() {
    echo "Starting code format check..."
    
    # Find all C/C++ source files
    while IFS= read -r -d '' file; do
        check_file "$file"
    done < <(find ./src -type f \( -name "*.cpp" -o -name "*.hpp" -o -name "*.c" -o -name "*.h" -o -name "*.cc" \) -print0)
    
    # Output summary
    echo "----------------------------------------"
    echo "Check completed!"
    echo "Total files checked: $checked_count"
    echo "Files needing formatting: $error_count"
    
    # Return non-zero if there are errors
    [ "$error_count" -gt 0 ] && exit 1 || exit 0
}

# Run the main function
main
