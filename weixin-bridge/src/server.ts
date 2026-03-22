/**
 * WeChat (Weixin) bridge server for nanobot.
 *
 * Connects the weixin-agent-sdk (long-polling) to the nanobot Python backend
 * via a local WebSocket server. The bridge implements the Agent interface from
 * weixin-agent-sdk and forwards each chat request to the connected Python client,
 * then waits for the response before returning it to the SDK.
 *
 * Security: the WebSocket server binds to 127.0.0.1 only.
 * An optional BRIDGE_TOKEN can be set to authenticate clients.
 */

import { WebSocketServer, WebSocket } from 'ws';
import { type Agent, type ChatRequest, type ChatResponse } from 'weixin-agent-sdk';
import { randomUUID } from 'crypto';

interface PendingRequest {
  resolve: (response: ChatResponse) => void;
  reject: (error: Error) => void;
  timer: ReturnType<typeof setTimeout>;
}

interface InboundPayload {
  type: 'message';
  id: string;
  conversationId: string;
  text: string;
  media?: {
    type: 'image' | 'audio' | 'video' | 'file';
    filePath: string;
    mimeType: string;
    fileName?: string;
  };
}

interface ResponsePayload {
  type: 'response';
  id: string;
  text?: string;
  media?: {
    type: 'image' | 'video' | 'file';
    url: string;
    fileName?: string;
  };
}

/** Timeout after which an unanswered chat request is rejected (5 minutes). */
const RESPONSE_TIMEOUT_MS = 5 * 60 * 1000;

export class BridgeServer {
  private wss: WebSocketServer | null = null;
  private clients: Set<WebSocket> = new Set();
  private pending: Map<string, PendingRequest> = new Map();

  constructor(private port: number, private token?: string) {}

  /**
   * Start the WebSocket server and return an Agent adapter that forwards
   * chat requests to connected Python clients.
   */
  start(): Agent {
    // Bind to localhost only — never expose to external network
    this.wss = new WebSocketServer({ host: '127.0.0.1', port: this.port });
    console.log(`🌉 Weixin bridge server listening on ws://127.0.0.1:${this.port}`);
    if (this.token) console.log('🔒 Token authentication enabled');

    this.wss.on('connection', (ws) => {
      if (this.token) {
        // Require auth handshake as first message within 5 seconds
        const timeout = setTimeout(() => ws.close(4001, 'Auth timeout'), 5000);
        ws.once('message', (data) => {
          clearTimeout(timeout);
          try {
            const msg = JSON.parse(data.toString());
            if (msg.type === 'auth' && msg.token === this.token) {
              console.log('🔗 Python client authenticated');
              this.setupClient(ws);
            } else {
              ws.close(4003, 'Invalid token');
            }
          } catch {
            ws.close(4003, 'Invalid auth message');
          }
        });
      } else {
        console.log('🔗 Python client connected');
        this.setupClient(ws);
      }
    });

    const self = this;
    return {
      chat(request: ChatRequest): Promise<ChatResponse> {
        return self.handleChatRequest(request);
      },
    };
  }

  private setupClient(ws: WebSocket): void {
    this.clients.add(ws);

    // Inform the Python client that the bridge is ready
    ws.send(JSON.stringify({ type: 'status', status: 'ready' }));

    ws.on('message', (data) => {
      try {
        const payload = JSON.parse(data.toString()) as ResponsePayload;
        if (payload.type === 'response') {
          this.handleResponse(payload);
        }
      } catch (err) {
        console.error('Error parsing response from Python client:', err);
      }
    });

    ws.on('close', () => {
      console.log('🔌 Python client disconnected');
      this.clients.delete(ws);
    });

    ws.on('error', (err) => {
      console.error('WebSocket client error:', err);
      this.clients.delete(ws);
    });
  }

  private handleResponse(payload: ResponsePayload): void {
    const pending = this.pending.get(payload.id);
    if (!pending) {
      console.warn('Received response for unknown request id:', payload.id);
      return;
    }

    clearTimeout(pending.timer);
    this.pending.delete(payload.id);

    const response: ChatResponse = {};
    if (payload.text !== undefined) response.text = payload.text;
    if (payload.media !== undefined) response.media = payload.media;

    pending.resolve(response);
  }

  private handleChatRequest(request: ChatRequest): Promise<ChatResponse> {
    return new Promise((resolve, reject) => {
      const id = randomUUID();

      const timer = setTimeout(() => {
        this.pending.delete(id);
        reject(new Error(`Weixin bridge: request ${id} timed out after ${RESPONSE_TIMEOUT_MS / 1000}s`));
      }, RESPONSE_TIMEOUT_MS);

      this.pending.set(id, { resolve, reject, timer });

      const payload: InboundPayload = {
        type: 'message',
        id,
        conversationId: request.conversationId,
        text: request.text,
      };

      if (request.media) {
        payload.media = request.media;
      }

      const data = JSON.stringify(payload);
      let sent = false;

      for (const client of this.clients) {
        if (client.readyState === WebSocket.OPEN) {
          client.send(data);
          sent = true;
          break; // send to first available Python client
        }
      }

      if (!sent) {
        clearTimeout(timer);
        this.pending.delete(id);
        reject(new Error('Weixin bridge: no Python client connected'));
      }
    });
  }

  async stop(): Promise<void> {
    // Reject all pending requests
    for (const [, pending] of this.pending.entries()) {
      clearTimeout(pending.timer);
      pending.reject(new Error('Weixin bridge shutting down'));
    }
    this.pending.clear();

    // Close all client connections
    for (const client of this.clients) {
      client.close();
    }
    this.clients.clear();

    // Close the WebSocket server
    if (this.wss) {
      this.wss.close();
      this.wss = null;
    }
  }
}
