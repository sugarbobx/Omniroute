"""
mt5_worker.py — OmniRoute MT5 terminal worker (subprocess)

The MetaTrader5 Python SDK keeps a SINGLE global connection per process:
mt5.initialize() silently rebinds it, so one process can only ever talk to one
terminal at a time. To copy across multiple accounts we run ONE worker process
per terminal and drive each over a stdin/stdout pipe.

Protocol: newline-delimited JSON.
  Request : {"id": <int>, "cmd": <str>, "args": {...}}
  Response: {"id": <int>, "ok": <bool>, "result": <any>} | {"id","ok":false,"error"}

Commands: initialize, account_info, symbol_info, symbol_info_tick, copy_rates,
          order_send, positions_get, shutdown, ping.

This module is only ever launched as a child process by mt5_client.py. It must
have the MetaTrader5 package importable (Windows + terminal installed).
"""

import json
import sys

try:
    import MetaTrader5 as mt5
except ImportError:  # pragma: no cover - worker only runs where SDK exists
    mt5 = None


_TIMEFRAMES = {}


def _tf(name):
    if not _TIMEFRAMES:
        _TIMEFRAMES.update({
            "M1": mt5.TIMEFRAME_M1, "M5": mt5.TIMEFRAME_M5,
            "M15": mt5.TIMEFRAME_M15, "M30": mt5.TIMEFRAME_M30,
            "H1": mt5.TIMEFRAME_H1, "H4": mt5.TIMEFRAME_H4, "D1": mt5.TIMEFRAME_D1,
        })
    return _TIMEFRAMES.get(name, mt5.TIMEFRAME_M5)


def _cmd_initialize(args):
    kwargs = {"timeout": args.get("timeout", 10000)}
    if args.get("path"):
        kwargs["path"] = args["path"]
    if args.get("login"):
        kwargs["login"] = int(args["login"])
    if args.get("password"):
        kwargs["password"] = args["password"]
    if args.get("server"):
        kwargs["server"] = args["server"]
    ok = mt5.initialize(**kwargs)
    if not ok:
        return {"ok_init": False, "error": str(mt5.last_error())}
    info = mt5.account_info()
    return {
        "ok_init": True,
        "equity": float(info.equity) if info else 0.0,
        "balance": float(info.balance) if info else 0.0,
    }


def _cmd_account_info(_args):
    info = mt5.account_info()
    if not info:
        return None
    return {"equity": float(info.equity), "balance": float(info.balance),
            "currency": info.currency, "login": info.login}


def _cmd_symbol_info(args):
    si = mt5.symbol_info(args["symbol"])
    if not si:
        return None
    return {"point": float(si.point), "digits": int(si.digits),
            "trade_contract_size": float(si.trade_contract_size),
            "volume_min": float(si.volume_min), "volume_max": float(si.volume_max),
            "volume_step": float(si.volume_step)}


def _cmd_symbol_info_tick(args):
    t = mt5.symbol_info_tick(args["symbol"])
    if not t:
        return None
    return {"bid": float(t.bid), "ask": float(t.ask), "time": int(t.time)}


def _cmd_copy_rates(args):
    rates = mt5.copy_rates_from_pos(args["symbol"], _tf(args["timeframe"]), 0,
                                    int(args["count"]))
    if rates is None or len(rates) == 0:
        return None
    return {
        "open":   [float(r["open"]) for r in rates],
        "high":   [float(r["high"]) for r in rates],
        "low":    [float(r["low"]) for r in rates],
        "close":  [float(r["close"]) for r in rates],
        "volume": [int(r["tick_volume"]) for r in rates],
        "time":   [int(r["time"]) for r in rates],
    }


def _cmd_order_send(args):
    req = dict(args["request"])
    # Map string action/type/filling tokens to SDK constants
    _consts = {
        "TRADE_ACTION_DEAL": mt5.TRADE_ACTION_DEAL,
        "TRADE_ACTION_PENDING": mt5.TRADE_ACTION_PENDING,
        "TRADE_ACTION_SLTP": mt5.TRADE_ACTION_SLTP,
        "ORDER_TYPE_BUY": mt5.ORDER_TYPE_BUY, "ORDER_TYPE_SELL": mt5.ORDER_TYPE_SELL,
        "ORDER_TYPE_BUY_LIMIT": mt5.ORDER_TYPE_BUY_LIMIT,
        "ORDER_TYPE_SELL_LIMIT": mt5.ORDER_TYPE_SELL_LIMIT,
        "ORDER_TYPE_BUY_STOP": mt5.ORDER_TYPE_BUY_STOP,
        "ORDER_TYPE_SELL_STOP": mt5.ORDER_TYPE_SELL_STOP,
        "ORDER_TIME_GTC": mt5.ORDER_TIME_GTC,
        "ORDER_FILLING_IOC": mt5.ORDER_FILLING_IOC,
        "ORDER_FILLING_FOK": mt5.ORDER_FILLING_FOK,
    }
    for key in ("action", "type", "type_time", "type_filling"):
        if isinstance(req.get(key), str) and req[key] in _consts:
            req[key] = _consts[req[key]]
    r = mt5.order_send(req)
    if r is None:
        return {"retcode": -1, "error": str(mt5.last_error())}
    return {"retcode": int(r.retcode), "order": int(r.order),
            "price": float(r.price), "comment": r.comment}


def _cmd_positions_get(args):
    symbol = args.get("symbol")
    positions = mt5.positions_get(symbol=symbol) if symbol else mt5.positions_get()
    magic = args.get("magic")
    out = []
    for p in (positions or []):
        if magic is not None and p.magic != magic:
            continue
        out.append({"ticket": int(p.ticket), "symbol": p.symbol,
                    "type": int(p.type), "volume": float(p.volume),
                    "price_open": float(p.price_open), "magic": int(p.magic),
                    "sl": float(p.sl), "tp": float(p.tp),
                    "profit": float(p.profit)})
    return out


_HANDLERS = {
    "initialize": _cmd_initialize,
    "account_info": _cmd_account_info,
    "symbol_info": _cmd_symbol_info,
    "symbol_info_tick": _cmd_symbol_info_tick,
    "copy_rates": _cmd_copy_rates,
    "order_send": _cmd_order_send,
    "positions_get": _cmd_positions_get,
    "ping": lambda _a: "pong",
}


def main():
    if mt5 is None:
        sys.stdout.write(json.dumps({"id": 0, "ok": False,
                                     "error": "MetaTrader5 not installed"}) + "\n")
        sys.stdout.flush()
        return
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            msg = json.loads(line)
        except json.JSONDecodeError:
            continue
        rid, cmd, args = msg.get("id"), msg.get("cmd"), msg.get("args") or {}
        if cmd == "shutdown":
            try:
                mt5.shutdown()
            finally:
                sys.stdout.write(json.dumps({"id": rid, "ok": True, "result": None}) + "\n")
                sys.stdout.flush()
            return
        try:
            result = _HANDLERS[cmd](args)
            resp = {"id": rid, "ok": True, "result": result}
        except Exception as exc:  # never crash the worker on one bad command
            resp = {"id": rid, "ok": False, "error": f"{type(exc).__name__}: {exc}"}
        sys.stdout.write(json.dumps(resp) + "\n")
        sys.stdout.flush()


if __name__ == "__main__":
    main()
