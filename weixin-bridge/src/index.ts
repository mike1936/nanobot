#!/usr/bin/env node
/**
 * nanobot WeChat (Weixin) Bridge
 *
 * This bridge connects WeChat to nanobot's Python backend via a local WebSocket
 * server. It uses weixin-agent-sdk's long-polling mechanism — no public server
 * or webhook is required.
 *
 * Usage:
 *   # First-time login (scan QR code with WeChat):
 *   npm run login
 *
 *   # Start the bridge (after login):
 *   npm start
 *
 *   # Custom settings:
 *   BRIDGE_PORT=3002 BRIDGE_TOKEN=secret npm start
 */

import { start } from 'weixin-agent-sdk';
import { BridgeServer } from './server.js';

const PORT = parseInt(process.env.BRIDGE_PORT || '3002', 10);
const TOKEN = process.env.BRIDGE_TOKEN || undefined;

console.log('🐈 nanobot WeChat Bridge');
console.log('========================\n');

const server = new BridgeServer(PORT, TOKEN);
const agent = server.start();

// Handle graceful shutdown
async function shutdown(): Promise<void> {
  console.log('\nShutting down...');
  await server.stop();
  process.exit(0);
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

// Start the weixin-agent-sdk message loop (blocks until process exits)
start(agent).catch((err) => {
  console.error('Weixin agent error:', err);
  process.exit(1);
});
