"""
Extract transfers from a decoded Solana block.

Solana has no Transfer events. A transfer is an instruction, and the decoded
BlockMessage lists every instruction, top-level and inner (CPI), so swap legs
are included. It covers:

  - native SOL transfers  (System Program Transfer, u32 index 2 + u64 lamports)
  - SPL token transfers   (Token Program and Token-2022, Transfer and TransferChecked)

Token accounts carry their mint, owner wallet and decimals in the block, so the
"from" and "to" are wallets and no extra lookups are needed.

It has no S3 or CLI dependencies, so it stays importable and testable on its own.
stream.py calls transfers_from_block() behind its --transfers flag with --chain solana.
"""

B58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"

def b58(raw):
    # Solana addresses and signatures are base58 strings of raw bytes
    n, s = int.from_bytes(raw, "big"), ""
    while n:
        n, r = divmod(n, 58)
        s = B58[r] + s
    return "1" * (len(raw) - len(raw.lstrip(b"\0"))) + s

SYSTEM_PROGRAM = "11111111111111111111111111111111"
TOKEN_PROGRAMS = {
    "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",   # SPL Token
    "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb",   # Token-2022
}

def u64(data, offset):
    return int.from_bytes(data[offset:offset + 8], "little")

def transfers_from_block(block):
    out = []
    for tx in block.Transactions:
        if not tx.Status.Success:          # failed transactions move nothing
            continue
        sig = b58(tx.Signature)
        accounts = tx.Header.Accounts
        for ix in tx.Instructions:         # top-level and inner (CPI) instructions
            program = b58(accounts[ix.ProgramAccountIndex].Address)
            data, idx = ix.Data, ix.AccountIndexes

            # 1. native SOL: System Program Transfer, u32 index 2 + u64 lamports
            if program == SYSTEM_PROGRAM:
                if len(data) >= 12 and int.from_bytes(data[:4], "little") == 2:
                    out.append({
                        "type": "sol", "tx": sig, "mint": None,
                        "from": b58(accounts[idx[0]].Address),
                        "to": b58(accounts[idx[1]].Address),
                        "amount": u64(data, 4), "decimals": 9,
                    })

            # 2. SPL tokens: Transfer (3) and TransferChecked (12)
            elif program in TOKEN_PROGRAMS and data:
                if data[0] == 3 and len(data) >= 9:        # source, dest, authority
                    src, dst = accounts[idx[0]], accounts[idx[1]]
                elif data[0] == 12 and len(data) >= 10:    # source, mint, dest, authority
                    src, dst = accounts[idx[0]], accounts[idx[2]]
                else:
                    continue
                token = src.Token if src.Token.Mint else dst.Token
                out.append({
                    "type": "spl", "tx": sig, "mint": b58(token.Mint),
                    # token accounts hold the balance; Token.Owner is the wallet
                    "from": b58(src.Token.Owner or src.Address),
                    "to": b58(dst.Token.Owner or dst.Address),
                    "amount": u64(data, 1), "decimals": token.Decimals,
                })
    return out
