#!/bin/bash

# Script to copy custom Langflow components into the active virtual environment.

# --- Configuration ---
# Source directory for your custom component category
CATEGORY_DIR="Presto"
# Name of the virtual environment directory
VENV_DIR=".venv"
# --- End Configuration ---

set -e

# Check if the category directory exists
if [ ! -d "$CATEGORY_DIR" ]; then
    echo "Error: Source directory '$CATEGORY_DIR' not found."
    exit 1
fi

# Check if the virtual environment directory exists
if [ ! -d "$VENV_DIR" ]; then
    echo "Error: Virtual environment '$VENV_DIR' not found."
    exit 1
fi

# Find the python site-packages directory within the venv
SITE_PACKAGES=$(find "$VENV_DIR/lib/" -type d -name "site-packages" | head -n 1)

if [ -z "$SITE_PACKAGES" ]; then
    echo "Error: Could not find site-packages directory in '$VENV_DIR'."
    exit 1
fi

TARGET_COMPONENTS_DIR="$SITE_PACKAGES/langflow/components"
TARGET_CATEGORY_DIR="$TARGET_COMPONENTS_DIR/$CATEGORY_DIR"

# Clean up previous installations of our specific category
echo "Cleaning up old installation of the '$CATEGORY_DIR' category..."
rm -rf "$TARGET_CATEGORY_DIR"

# Copy the component category directory recursively
echo "Copying new component category: $CATEGORY_DIR"
cp -R "$CATEGORY_DIR" "$TARGET_COMPONENTS_DIR"/

echo ""
echo "✅ Custom components installed successfully into the '$CATEGORY_DIR' category."
