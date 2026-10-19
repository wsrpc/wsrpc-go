import assert from "node:assert/strict";
import { createServer } from "node:http";
import { createRequire } from "node:module";
import { readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import WebSocket from "ws";
import { WSRPCClient } from "@wsrpc/client/node";
import { WebSocketAsync } from "@wsrpc/server";
const require = createRequire(import.meta.url);
const [mode, url] = process.argv.slice(2);
const value = { value: 21 };

async function check(client) {
  assert.deepEqual(await client.call("echo", { text: "Привет 🐍", value: 0 }), {
    text: "Привет 🐍",
    value: 0,
  });
  assert.equal(await client.call("reverse", value), 42);
  assert.equal(await client.call("counter", { value: 10 }), 10);
  assert.equal(await client.call("counter.add", { value: 2 }), 12);
  await assert.rejects(
    client.call("fail", {}),
    (error) => error.message === "legacy failure",
  );
}

if (mode === "server") {
  class RPC extends WebSocketAsync {}
  RPC.addRoute("echo", (_socket, params) => params);
  RPC.addRoute("reverse", (socket, params) => socket.call("double", params));
  RPC.addRoute("fail", () => {
    throw new Error("TypeScript failure");
  });
  const server = createServer();
  const rpc = RPC.attach(server);
  server.listen(0, "127.0.0.1", () =>
    console.log(`ws://127.0.0.1:${server.address().port}/ws/`),
  );
  process.once("SIGTERM", () => void rpc.close().then(() => server.close()));
} else if (mode === "clients") {
  const client = await WSRPCClient.connect(url, {
    routes: new Map([["double", (_socket, { value }) => value * 2]]),
  });
  try {
    await check(client);
  } finally {
    await client.close();
  }
  console.log("TypeScript client -> Go server: passed");
  globalThis.WebSocket = WebSocket;
  const Legacy = require("legacy-wsrpc");
  Legacy.DEBUG = false;
  Legacy.TRACE = false;
  const legacy = new Legacy(url, 25);
  legacy.addRoute("double", ({ value }) => value * 2);
  const queued = legacy.proxy.echo({ queued: true });
  legacy.connect();
  assert.deepEqual(await queued, { queued: true });
  await check(legacy);
  console.log("Unmodified npm 4.4.1 client -> Go server: passed");
  legacy.destroy();
  // Legacy destroy intentionally schedules reconnect; terminate this test process.
  process.exit(0);
} else if (mode === "assets") {
  writeFileSync(
    join(url, "wsrpc.js"),
    readFileSync(require.resolve("legacy-wsrpc")),
  );
  writeFileSync(
    join(url, "index.html"),
    readFileSync(new URL("legacy-application.html", import.meta.url)),
  );
} else if (mode === "browser") {
  const { chromium } = await import("playwright");
  const browser = await chromium.launch();
  try {
    const page = await browser.newPage();
    const errors = [];
    page.on("pageerror", (error) => errors.push(String(error)));
    await page.goto(url.replace(/^ws/, "http").replace(/ws\/$/, ""));
    const result = await page.evaluate(() =>
      Promise.race([
        window.applicationDone,
        new Promise((_, reject) =>
          setTimeout(() => reject(new Error("Browser timeout")), 8000),
        ),
      ]),
    );
    assert.deepEqual(errors, []);
    assert.deepEqual(result, {
      echo: { text: "old application", value: 0 },
      callback: 42,
      deferred: 9,
      nested: 3,
      error: "legacy failure",
      bytes: [0, 1, 255],
      removed: 1,
      connections: 2,
      afterReconnect: { again: true },
      state: "OPEN",
      stateCode: 1,
    });
    console.log(
      "Chromium: unchanged application + npm 4.4.1 -> Go server: passed",
    );
  } finally {
    await browser.close();
  }
} else {
  throw new Error("Unknown interop command");
}
