#!/bin/bash

# Script to extract private key content for Snowflake connector
# This removes headers and formats the key for the JSON config

echo "🔑 Extracting Private Key for Snowflake Connector"
echo "==============================================="

KEY_FILE="$HOME/.ssh/snowflake_rsa_key.p8"

if [ ! -f "$KEY_FILE" ]; then
  echo "❌ Private key file not found: $KEY_FILE"
  echo ""
  echo "Please ensure your Snowflake private key is located at:"
  echo "   $KEY_FILE"
  echo ""
  echo "If it's in a different location, update this script or create a symlink:"
  echo "   ln -s /path/to/your/key.p8 $KEY_FILE"
  exit 1
fi

echo "📋 Found private key file: $KEY_FILE"
echo ""

# Check if the key is encrypted (has a passphrase)
if grep -q "ENCRYPTED" "$KEY_FILE"; then
  echo "🔒 Your private key is encrypted (password protected)"
  echo ""
  echo "For the Snowflake connector, you have two options:"
  echo ""
  echo "1. Use the encrypted key with passphrase (recommended):"
  echo "   - Set both 'snowflake.private.key' and 'snowflake.private.key.passphrase'"
  echo ""
  echo "2. Create an unencrypted version (less secure):"
  echo "   openssl rsa -in $KEY_FILE -out ${KEY_FILE%.p8}_unencrypted.pem"
  echo ""
  
  # Extract encrypted key content
  echo "🔑 Encrypted private key content (copy this to your config):"
  echo "----------------------------------------"
  grep -v "BEGIN\|END" "$KEY_FILE" | tr -d '\n'
  echo ""
  echo "----------------------------------------"
  echo ""
  echo "⚠️  You'll also need to set 'snowflake.private.key.passphrase' in your config"
  
else
  echo "🔓 Your private key is unencrypted"
  echo ""
  echo "🔑 Private key content (copy this to your config):"
  echo "----------------------------------------"
  grep -v "BEGIN\|END" "$KEY_FILE" | tr -d '\n'
  echo ""
  echo "----------------------------------------"
fi

echo ""
echo "📝 To use this in your snowflake-connector-config.json:"
echo '   "snowflake.private.key": "PASTE_KEY_CONTENT_HERE",'
if grep -q "ENCRYPTED" "$KEY_FILE"; then
  echo '   "snowflake.private.key.passphrase": "YOUR_PASSPHRASE_HERE",'
fi
echo ""
echo "✅ Key extraction complete!"