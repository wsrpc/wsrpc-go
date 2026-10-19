"""Real Go/Python/TS interoperability, including the published browser client 4.4.1."""

import asyncio
import socket
import tempfile
from contextlib import asynccontextmanager
from pathlib import Path

import uvicorn
from aiohttp import web
from aiohttp.test_utils import TestServer
from wsrpc_aiohttp import WSRPCClient as AioClient, WebSocketAsync as AioServer
from wsrpc_asgi import WSRPCClient as ASGIClient, WebSocketAsync as ASGIServer

ROOT = Path(__file__).resolve().parents[1]


async def run(*args):
    process = await asyncio.create_subprocess_exec(
        *map(str, args),
        cwd=ROOT,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        output, errors = await asyncio.wait_for(process.communicate(), 30)
        if process.returncode:
            raise RuntimeError(f"{args}: {errors.decode()}\n{output.decode()}")
        return output.decode()
    finally:
        if process.returncode is None:
            process.kill()
            await process.wait()


@asynccontextmanager
async def background(*args):
    process = await asyncio.create_subprocess_exec(
        *map(str, args), cwd=ROOT, stdout=asyncio.subprocess.PIPE
    )
    try:
        url = (await asyncio.wait_for(process.stdout.readline(), 10)).decode().strip()
        assert url.startswith("ws://"), url
        yield url
    finally:
        if process.returncode is None:
            process.terminate()
            try:
                await asyncio.wait_for(process.wait(), 5)
            except asyncio.TimeoutError:
                process.kill()
                await process.wait()


@asynccontextmanager
async def asgi_server(app):
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(128)
    port = listener.getsockname()[1]
    server = uvicorn.Server(
        uvicorn.Config(app, log_level="error", ws="websockets-sansio")
    )
    task = asyncio.create_task(server.serve(sockets=[listener]))
    try:
        for _ in range(500):
            if server.started:
                break
            if task.done():
                await task
            await asyncio.sleep(0.01)
        else:
            raise TimeoutError("ASGI startup")
        yield f"ws://127.0.0.1:{port}/ws/"
    finally:
        server.should_exit = True
        try:
            await asyncio.wait_for(task, 5)
        finally:
            listener.close()


async def python_clients(url):
    for base in [AioClient, ASGIClient]:

        class Client(base):
            pass

        Client.add_route("double", lambda socket, value: value * 2)
        async with Client(url, timeout=3) as client:
            assert await client.proxy.echo(text="Привет 🐍", value=0) == {
                "text": "Привет 🐍",
                "value": 0,
            }
            assert await client.proxy.reverse(value=21) == 42
            assert await client.proxy.counter(value=10) == 10
            assert await client.proxy.counter.add(value=2) == 12
            try:
                await client.proxy.fail()
            except Exception as exc:
                assert getattr(exc, "message", None) == "legacy failure"
            else:
                raise AssertionError("missing remote error")
        print(f"{base.__module__.split('.')[0]} client -> Go server: passed")


async def echo(socket, **params):
    return params


async def reverse(socket, value):
    return await socket.proxy.double(value=value)


async def main():
    with tempfile.TemporaryDirectory(prefix="wsrpc-go-interop-") as directory:
        tmp = Path(directory)
        server_binary = tmp / "server"
        client_binary = tmp / "client"
        await run("go", "build", "-o", server_binary, "./examples/server")
        await run("go", "build", "-o", client_binary, "./examples/client")
        await run("node", "interop/js.mjs", "assets", tmp)
        async with background(
            server_binary, "-listen", "127.0.0.1:0", "-assets", tmp
        ) as url:
            await python_clients(url)
            print(await run("node", "interop/js.mjs", "clients", url), end="")
            print(await run("node", "interop/js.mjs", "browser", url), end="")
        for base in [AioServer, ASGIServer]:

            class Handler(base):
                pass

            Handler.add_route("echo", echo)
            Handler.add_route("reverse", reverse)
            if base is AioServer:
                app = web.Application()
                app.router.add_get("/ws/", Handler)
                async with TestServer(app) as server:
                    await run(client_binary, "-url", str(server.make_url("/ws/")))
            else:
                async with asgi_server(Handler.as_asgi(path="/ws/")) as url:
                    await run(client_binary, "-url", url)
            print(f"Go client -> {base.__module__.split('.')[0]} server: passed")
        async with background("node", "interop/js.mjs", "server") as url:
            await run(client_binary, "-url", url)
            print("Go client -> TypeScript server: passed")


if __name__ == "__main__":
    asyncio.run(asyncio.wait_for(main(), 90))
