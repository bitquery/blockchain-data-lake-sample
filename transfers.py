"""
Extract transfers from a decoded EVM block.

This is the "write your own parser" example: given a decoded BlockMessage, it is
a short pass over the transactions and their receipt logs. It covers:

  - native transfers   (TransactionHeader.Value, from -> to)
  - ERC-20 transfers   (Transfer event, amount in log data)
  - ERC-721 transfers  (Transfer event, token id in the 4th topic)

It has no S3 or CLI dependencies, so it stays importable and testable on its own.
stream.py calls transfers_from_block() behind its --transfers flag.
"""

# Transfer(address,address,uint256) event signature, shared by ERC-20 and ERC-721
TRANSFER = bytes.fromhex(
    "ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
)


def topic_address(topic):
    """An indexed address is the last 20 bytes of a 32-byte topic."""
    return "0x" + topic[-20:].hex()


def transfers_from_block(block):
    """Return native, ERC-20, and ERC-721 transfers found in a decoded block."""
    out = []
    for tx in block.Transactions:
        th = tx.TransactionHeader
        tx_hash = "0x" + th.Hash.hex()

        # 1. native transfer carried by the transaction itself
        value = int.from_bytes(th.Value, "big")
        if value > 0:
            out.append({
                "type": "native", "tx": tx_hash, "token": None,
                "from": "0x" + th.From.hex(), "to": "0x" + th.To.hex(),
                "amount": value,
            })

        # 2. token transfers emitted as Transfer events in the logs
        if not tx.HasField("Receipt"):
            continue
        for log in tx.Receipt.Logs:
            t = log.Topics
            if len(t) < 3 or t[0].Hash != TRANSFER:
                continue
            token = "0x" + log.LogHeader.Address.hex()
            frm, to = topic_address(t[1].Hash), topic_address(t[2].Hash)
            if len(t) == 3:                       # ERC-20: amount is in data
                out.append({
                    "type": "erc20", "tx": tx_hash, "token": token,
                    "from": frm, "to": to,
                    "amount": int.from_bytes(log.LogHeader.Data, "big"),
                })
            elif len(t) == 4:                     # ERC-721: token id is topic 3
                out.append({
                    "type": "erc721", "tx": tx_hash, "token": token,
                    "from": frm, "to": to,
                    "token_id": int.from_bytes(t[3].Hash, "big"),
                })
    return out
