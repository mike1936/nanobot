#!/usr/bin/env node
/**
 * WeChat login helper.
 *
 * Run this once to authenticate with WeChat by scanning the QR code.
 * The session is persisted to ~/.openclaw/ and reused on subsequent starts.
 *
 * Usage:
 *   npm run login
 */

import { login } from 'weixin-agent-sdk';

console.log('🐈 nanobot WeChat Login');
console.log('=======================\n');
console.log('Scan the QR code below with your WeChat app (Me → Settings → Devices → Desktop).\n');

login()
  .then(() => {
    console.log('\n✅ Login successful! You can now start the bridge with: npm start');
    process.exit(0);
  })
  .catch((err) => {
    console.error('Login failed:', err);
    process.exit(1);
  });
