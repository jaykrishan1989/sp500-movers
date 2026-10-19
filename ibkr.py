"""Connect to the local Interactive Brokers Trader Workstation or IB Gateway."""

from __future__ import annotations


def connectToIBKR(
    host: str = "127.0.0.1",
    port: int = 4001,
    client_id: int = 1,
    readonly: bool = False,
    timeout: float = 10,
):
    """Open a TWS/Gateway session and return it.

    Defaults match this workstation: socket port 4001, read-only API off.
    Call disconnect() on the returned session when the app is finished.
    Each app that stays connected needs its own client_id.
    """
    try:
        from ib_async import IB
    except ImportError:
        from ib_insync import IB

    ib = IB()
    try:
        ib.connect(
            host,
            port,
            clientId=client_id,
            readonly=readonly,
            timeout=timeout,
        )
    except Exception as exc:
        if ib.isConnected():
            ib.disconnect()
        raise ConnectionError(
            f"Could not connect to IBKR at {host}:{port} with client id {client_id}. "
            "Start Trader Workstation or IB Gateway, enable socket clients, "
            "and use a client id that no other app is using."
        ) from exc

    if not ib.isConnected():
        raise ConnectionError(
            f"IBKR at {host}:{port} did not stay connected (client id {client_id})."
        )
    return ib
