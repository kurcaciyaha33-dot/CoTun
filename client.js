
const fs = require("fs");
const net = require("net");
const WebSocket = require("ws");

const relays = JSON.parse(
  fs.readFileSync("./relays.json", "utf8")
);

const config = JSON.parse(
  fs.readFileSync("./config.json", "utf8")
);

const args = process.argv.slice(2);

if (args[0] === "#relays") {
  console.log("\nCoCoTunnel Relay List\n");

  for (const [name, relay] of Object.entries(relays)) {
    console.log(
      `${name}: ${relay.url} | ${relay.host}:${relay.port}`
    );
  }

  process.exit(0);
}

const name = args[0] || "railway";
const relay = relays[name];

if (!relay) {
  console.error("Relay not found:", name);
  process.exit(1);
}

if (!config.token || config.token.length < 32) {
  throw new Error("Configure a valid client token");
}

const ws = new WebSocket(relay.url);
const sockets = new Map();

function sendData(id, chunk) {
  if (ws.readyState !== WebSocket.OPEN) return;

  ws.send(Buffer.concat([
    Buffer.from(id, "hex"),
    chunk
  ]), { binary: true });
}

ws.on("open", () => {
  console.log("Connecting to", name);

  ws.send(JSON.stringify({
    type: "auth",
    token: config.token
  }));
});

ws.on("message", (data, binary) => {
  if (binary) {
    const frame = Buffer.from(data);

    if (frame.length < 16) return;

    const id = frame.subarray(0, 16).toString("hex");
    const socket = sockets.get(id);

    if (socket && !socket.destroyed) {
      socket.write(frame.subarray(16));
    }

    return;
  }

  let msg;

  try {
    msg = JSON.parse(data.toString());
  } catch {
    return;
  }

  if (msg.type === "ready") {
    console.log("\nCoCoTunnel Lite v1.0");
    console.log("====================");
    console.log("Status: CONNECTED");
    console.log("Protocol: TCP");
    console.log("Local: 127.0.0.1:3389");
    console.log("Public:", relay.host + ":" + relay.port);
    console.log("Expires:", msg.expires);
    console.log("====================\n");
  }

  if (msg.type === "connect") {
    const id = msg.id;

    const socket = net.createConnection({
      host: "127.0.0.1",
      port: 3389
    });

    sockets.set(id, socket);

    socket.on("data", chunk => {
      sendData(id, chunk);
    });

    socket.on("close", () => {
      sockets.delete(id);
    });

    socket.on("error", err => {
      console.error("RDP:", err.message);
      sockets.delete(id);
    });
  }
});

ws.on("close", () => {
  for (const socket of sockets.values()) {
    socket.destroy();
  }

  sockets.clear();

  console.log("Tunnel disconnected");
});

ws.on("error", err => {
  console.error("Connection error:", err.message);
});

process.on("SIGINT", () => {
  ws.close();
  process.exit(0);
});
