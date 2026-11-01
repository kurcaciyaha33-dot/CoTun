
const http = require("http");
const net = require("net");
const crypto = require("crypto");
const WebSocket = require("ws");

const PORT = Number(process.env.PORT || 8080);
const TCP_PORT = Number(process.env.TCP_PORT || 3389);
const TOKEN = process.env.CLIENT_TOKEN;
const DURATION = 6 * 60 * 60 * 1000;

if (!TOKEN || TOKEN.length < 32) {
  throw new Error("CLIENT_TOKEN must be at least 32 characters");
}

const httpServer = http.createServer((req, res) => {
  res.writeHead(200, {
    "Content-Type": "application/json"
  });

  res.end(JSON.stringify({
    service: "CoCoTunnel Lite",
    status: "online"
  }));
});

const wss = new WebSocket.Server({
  noServer: true,
  maxPayload: 1024 * 1024
});

let client = null;
let expiry = 0;
const connections = new Map();

function sendJSON(data) {
  if (client?.readyState === WebSocket.OPEN) {
    client.send(JSON.stringify(data));
  }
}

function closeSession() {
  if (client) {
    client.close();
    client = null;
  }

  for (const socket of connections.values()) {
    socket.destroy();
  }

  connections.clear();
}

httpServer.on("upgrade", (req, socket, head) => {
  if (req.url !== "/control") {
    socket.destroy();
    return;
  }

  wss.handleUpgrade(req, socket, head, ws => {
    wss.emit("connection", ws, req);
  });
});

wss.on("connection", ws => {
  let authenticated = false;

  ws.on("message", (data, binary) => {
    if (binary) {
      if (!authenticated || data.length < 16) return;

      const frame = Buffer.from(data);
      const id = frame.subarray(0, 16).toString("hex");
      const target = connections.get(id);

      if (target && !target.destroyed) {
        target.write(frame.subarray(16));
      }

      return;
    }

    let msg;

    try {
      msg = JSON.parse(data.toString());
    } catch {
      ws.close();
      return;
    }

    if (!authenticated) {
      const supplied = Buffer.from(String(msg.token || ""));
      const expected = Buffer.from(TOKEN);

      if (
        msg.type !== "auth" ||
        supplied.length !== expected.length ||
        !crypto.timingSafeEqual(supplied, expected)
      ) {
        ws.close(1008, "Unauthorized");
        return;
      }

      if (client && client.readyState === WebSocket.OPEN) {
        ws.close(1013, "Tunnel already connected");
        return;
      }

      authenticated = true;
      client = ws;
      expiry = Date.now() + DURATION;

      sendJSON({
        type: "ready",
        expires: new Date(expiry).toISOString(),
        port: TCP_PORT
      });
    }
  });

  ws.on("close", () => {
    if (client === ws) {
      client = null;

      for (const socket of connections.values()) {
        socket.destroy();
      }

      connections.clear();
    }
  });
});

const tcpServer = net.createServer(socket => {
  if (
    !client ||
    client.readyState !== WebSocket.OPEN ||
    Date.now() >= expiry
  ) {
    socket.destroy();
    return;
  }

  const id = crypto.randomBytes(16).toString("hex");
  connections.set(id, socket);

  sendJSON({
    type: "connect",
    id
  });

  socket.on("data", chunk => {
    if (client?.readyState !== WebSocket.OPEN) {
      socket.destroy();
      return;
    }

    client.send(
      Buffer.concat([
        Buffer.from(id, "hex"),
        chunk
      ]),
      { binary: true }
    );
  });

  socket.on("close", () => {
    connections.delete(id);
    sendJSON({ type: "disconnect", id });
  });

  socket.on("error", () => {
    connections.delete(id);
  });
});

setInterval(() => {
  if (client && Date.now() >= expiry) {
    console.log("Six-hour session expired");
    closeSession();
  }
}, 1000);

httpServer.listen(PORT, "0.0.0.0", () => {
  console.log("CoCoTunnel control server:", PORT);
});

tcpServer.listen(TCP_PORT, "0.0.0.0", () => {
  console.log("CoCoTunnel TCP relay:", TCP_PORT);
});
