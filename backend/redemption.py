"""Redemption of resolved Polymarket positions back to USDC."""

import logging

import httpx

from backend.config import settings

log = logging.getLogger(__name__)

DATA_API_URL = "https://data-api.polymarket.com"
USDC_POLYGON = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"

# Polymarket order-book exchange contracts (used for OrderFilled event monitoring only)
CTF_EXCHANGE = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"
NEG_RISK_CTF_EXCHANGE = "0xC5d563A36AE78145C45a50134d48A1215220f80a"

# Gnosis Conditional Tokens Framework — the ERC-1155 contract that holds conditional
# tokens and exposes redeemPositions().  This is a different contract from the
# Polymarket Exchange (CTF_EXCHANGE above).
GNOSIS_CTF = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
NEG_RISK_GNOSIS_CTF = "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296"

ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"
ZERO_BYTES32 = b"\x00" * 32

ORDER_FILLED_ABI = [
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "name": "orderHash", "type": "bytes32"},
            {"indexed": True, "name": "maker", "type": "address"},
            {"indexed": True, "name": "taker", "type": "address"},
            {"indexed": False, "name": "makerAssetId", "type": "uint256"},
            {"indexed": False, "name": "takerAssetId", "type": "uint256"},
            {"indexed": False, "name": "makerAmountFilled", "type": "uint256"},
            {"indexed": False, "name": "takerAmountFilled", "type": "uint256"},
            {"indexed": False, "name": "fee", "type": "uint256"},
        ],
        "name": "OrderFilled",
        "type": "event",
    }
]

REDEEM_ABI = [
    {
        "inputs": [
            {"name": "collateralToken", "type": "address"},
            {"name": "parentCollectionId", "type": "bytes32"},
            {"name": "conditionId", "type": "bytes32"},
            {"name": "indexSets", "type": "uint256[]"},
        ],
        "name": "redeemPositions",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function",
    }
]

# Gnosis Safe ABI — only the functions we need
GNOSIS_SAFE_ABI = [
    {
        "name": "execTransaction",
        "type": "function",
        "stateMutability": "payable",
        "inputs": [
            {"name": "to", "type": "address"},
            {"name": "value", "type": "uint256"},
            {"name": "data", "type": "bytes"},
            {"name": "operation", "type": "uint8"},
            {"name": "safeTxGas", "type": "uint256"},
            {"name": "baseGas", "type": "uint256"},
            {"name": "gasPrice", "type": "uint256"},
            {"name": "gasToken", "type": "address"},
            {"name": "refundReceiver", "type": "address"},
            {"name": "signatures", "type": "bytes"},
        ],
        "outputs": [{"name": "success", "type": "bool"}],
    },
    {
        "name": "nonce",
        "type": "function",
        "stateMutability": "view",
        "inputs": [],
        "outputs": [{"name": "", "type": "uint256"}],
    },
]

# EIP-712 type hashes for Gnosis Safe
_DOMAIN_SEPARATOR_TYPEHASH = None
_SAFE_TX_TYPEHASH = None


def _get_type_hashes():
    global _DOMAIN_SEPARATOR_TYPEHASH, _SAFE_TX_TYPEHASH
    if _DOMAIN_SEPARATOR_TYPEHASH is None:
        from eth_utils import keccak

        _DOMAIN_SEPARATOR_TYPEHASH = keccak(
            b"EIP712Domain(uint256 chainId,address verifyingContract)"
        )
        _SAFE_TX_TYPEHASH = keccak(
            b"SafeTx(address to,uint256 value,bytes data,uint8 operation,"
            b"uint256 safeTxGas,uint256 baseGas,uint256 gasPrice,"
            b"address gasToken,address refundReceiver,uint256 nonce)"
        )
    return _DOMAIN_SEPARATOR_TYPEHASH, _SAFE_TX_TYPEHASH


def _build_safe_signature(
    private_key: str,
    safe_address: str,
    to: str,
    calldata: bytes,
    nonce: int,
    chain_id: int = 137,
) -> bytes:
    """
    Build a valid Gnosis Safe EIP-712 signature for an execTransaction call.
    Uses operation=0 (CALL) and zero gas/refund params (caller pays gas in MATIC).
    Returns the 65-byte signature expected by execTransaction.
    """
    from eth_abi import encode as abi_encode
    from eth_account import Account
    from eth_utils import keccak
    from web3 import Web3

    domain_typehash, safe_tx_typehash = _get_type_hashes()

    safe_addr = Web3.to_checksum_address(safe_address)
    to_addr = Web3.to_checksum_address(to)
    zero_addr = Web3.to_checksum_address(ZERO_ADDRESS)

    # Domain separator: EIP712Domain(chainId, verifyingContract)
    domain_separator = keccak(
        abi_encode(
            ["bytes32", "uint256", "address"],
            [domain_typehash, chain_id, safe_addr],
        )
    )

    # SafeTx struct hash
    safe_tx_hash = keccak(
        abi_encode(
            [
                "bytes32",  # SAFE_TX_TYPEHASH
                "address",  # to
                "uint256",  # value
                "bytes32",  # keccak(data)
                "uint8",  # operation (0 = CALL)
                "uint256",  # safeTxGas
                "uint256",  # baseGas
                "uint256",  # gasPrice
                "address",  # gasToken
                "address",  # refundReceiver
                "uint256",  # nonce
            ],
            [
                safe_tx_typehash,
                to_addr,
                0,  # value (no ETH/MATIC sent)
                keccak(calldata),
                0,  # operation = CALL
                0,  # safeTxGas (caller covers gas)
                0,  # baseGas
                0,  # gasPrice (no refund)
                zero_addr,  # gasToken
                zero_addr,  # refundReceiver
                nonce,
            ],
        )
    )

    # Final EIP-712 hash
    final_hash = keccak(b"\x19\x01" + domain_separator + safe_tx_hash)

    # Sign with EOA private key
    signed = Account._sign_hash(final_hash, private_key)
    # v must be 27 or 28 for Gnosis Safe EOA signature validation
    v = signed.v if signed.v >= 27 else signed.v + 27
    return signed.r.to_bytes(32, "big") + signed.s.to_bytes(32, "big") + bytes([v])


async def get_redeemable_positions(wallet_address: str) -> list[dict]:
    """Fetch positions that are resolved and redeemable.

    ``wallet_address`` must be the Polymarket proxy/funder wallet address
    (POLY_FUNDER_ADDRESS), NOT the EOA derived from the private key.
    The data API indexes positions by the proxy wallet, not the signing EOA.
    """
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(
                f"{DATA_API_URL}/positions",
                params={"user": wallet_address},
            )
            resp.raise_for_status()
            positions = resp.json()

        return [p for p in positions if p.get("redeemable") and float(p.get("size", 0)) > 0]
    except Exception as e:
        log.error("Failed to fetch redeemable positions: %s", e)
        return []


async def redeem_position(position: dict) -> dict:
    """Redeem a single resolved position via the Gnosis Safe proxy wallet.

    Polymarket's trading wallet is a Gnosis Safe contract controlled by the
    EOA derived from POLY_PRIVATE_KEY.  The Safe holds the ERC-1155 conditional
    tokens, so ``redeemPositions`` must be called through the Safe's
    ``execTransaction`` function, signed by the EOA.
    """
    from web3 import Web3

    try:
        condition_id = position.get("conditionId")
        if not condition_id:
            return {"error": "No conditionId in position"}

        # API returns "negativeRisk", not "negRisk".
        # Use the Gnosis CTF contract for redeemPositions — NOT the Polymarket Exchange.
        neg_risk = position.get("negativeRisk", False)
        ctf_addr = NEG_RISK_GNOSIS_CTF if neg_risk else GNOSIS_CTF

        # Index set for the specific outcome we hold (not both [1, 2])
        # outcomeIndex 0 → indexSet 1 (0b01)
        # outcomeIndex 1 → indexSet 2 (0b10)
        outcome_index = int(position.get("outcomeIndex", 0))
        index_sets = [1 << outcome_index]

        w3 = Web3(Web3.HTTPProvider(settings.POLYGON_RPC_URL))
        private_key = settings.POLY_PRIVATE_KEY
        eoa = w3.eth.account.from_key(private_key).address
        safe_address = settings.POLY_FUNDER_ADDRESS  # Gnosis Safe holds the tokens

        # Build the CTF redeemPositions calldata
        ctf = w3.eth.contract(
            address=Web3.to_checksum_address(ctf_addr),
            abi=REDEEM_ABI,
        )
        calldata = ctf.encode_abi(
            "redeemPositions",
            args=[
                Web3.to_checksum_address(USDC_POLYGON),
                ZERO_BYTES32,
                bytes.fromhex(condition_id.lstrip("0x")),
                index_sets,
            ],
        )

        # Read current Safe nonce
        safe = w3.eth.contract(
            address=Web3.to_checksum_address(safe_address),
            abi=GNOSIS_SAFE_ABI,
        )
        nonce = safe.functions.nonce().call()

        # Build EIP-712 signature
        sig = _build_safe_signature(
            private_key=private_key,
            safe_address=safe_address,
            to=ctf_addr,
            calldata=bytes.fromhex(calldata[2:]),  # strip 0x
            nonce=nonce,
            chain_id=137,
        )

        gas_price = w3.eth.gas_price
        eoa_nonce = w3.eth.get_transaction_count(eoa)

        tx = safe.functions.execTransaction(
            Web3.to_checksum_address(ctf_addr),  # to
            0,  # value
            bytes.fromhex(calldata[2:]),  # data
            0,  # operation = CALL
            0,  # safeTxGas
            0,  # baseGas
            0,  # gasPrice (caller pays)
            Web3.to_checksum_address(ZERO_ADDRESS),  # gasToken
            Web3.to_checksum_address(ZERO_ADDRESS),  # refundReceiver
            sig,  # signatures
        ).build_transaction(
            {
                "from": eoa,
                "nonce": eoa_nonce,
                "gas": 300_000,
                "gasPrice": int(gas_price * 1.1),
                "chainId": 137,
            }
        )

        signed = w3.eth.account.sign_transaction(tx, private_key)
        tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
        receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)

        if receipt["status"] == 1:
            value = float(position.get("currentValue", 0))
            gas_matic = receipt["gasUsed"] * int(gas_price * 1.1) / 1e18
            log.info(
                "Redeemed conditionId=%s outcome=%s tx=%s value=$%.2f gas=%.6f MATIC",
                condition_id[:16],
                position.get("outcome", "?"),
                tx_hash.hex()[:16],
                value,
                gas_matic,
            )
            return {
                "success": True,
                "tx_hash": tx_hash.hex(),
                "value": value,
                "gas_matic": gas_matic,
            }
        else:
            return {"error": "Transaction reverted", "tx_hash": tx_hash.hex()}

    except Exception as e:
        log.error("Redemption failed for conditionId=%s: %s", position.get("conditionId", "?"), e)
        return {"error": str(e)}


async def check_and_redeem() -> dict:
    """Check for redeemable positions and redeem them. Safe to call periodically."""
    if not settings.credentials_valid():
        return {"skipped": True, "reason": "no_credentials"}

    try:
        # Use the Gnosis Safe / funder address — this is where positions are held.
        # Do NOT derive from POLY_PRIVATE_KEY; that gives the signing EOA, not the Safe.
        wallet_address = settings.POLY_FUNDER_ADDRESS
        positions = await get_redeemable_positions(wallet_address)
        if not positions:
            return {"redeemed": 0, "failed": 0, "total_value": 0.0}

        log.info("Found %d redeemable position(s)", len(positions))

        redeemed = 0
        failed = 0
        total_value = 0.0
        total_gas_matic = 0.0
        redeemed_positions: list[dict] = []

        for position in positions:
            result = await redeem_position(position)
            if result.get("success"):
                redeemed += 1
                total_value += result.get("value", 0.0)
                gas_matic = result.get("gas_matic", 0.0)
                total_gas_matic += gas_matic
                redeemed_positions.append(
                    {
                        "condition_id": position.get("conditionId", ""),
                        "gas_matic": gas_matic,
                    }
                )
            else:
                failed += 1
                log.error("Failed to redeem position: %s", result.get("error"))

        return {
            "redeemed": redeemed,
            "failed": failed,
            "total_value": total_value,
            "total_gas_matic": total_gas_matic,
            "redeemed_positions": redeemed_positions,
        }

    except Exception as e:
        log.error("check_and_redeem error: %s", e)
        return {"error": str(e)}
