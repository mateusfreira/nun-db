#!/bin/bash
cd ./nun-db-data
# Loop through all .data.keys and .data.values files
shopt -s nullglob
for file in *-nun.data.keys *-nun.data.values; do
  # Extract the prefix by removing '-nun.data.keys' or '-nun.data.values'
  prefix="${file%-nun.data.*}"

  # Create a directory using the extracted prefix if it doesn't already exist
  mkdir -p "$prefix"

  # Determine the new filename based on the type of file
  if [[ "$file" == *-nun.data.keys ]]; then
    newfile="nun.keys"
  elif [[ "$file" == *-nun.data.values ]]; then
    newfile="nun.values"
  fi

  # Move and rename the file
  mv "$file" "$prefix/$newfile"
done

echo "Files have been organized and renamed."

