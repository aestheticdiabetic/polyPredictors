"""Redemption of resolved Polymarket positions back to USDC."""

import logging

import httpx

from backend.config import settings

log = logging.getLogger(__name__)

DATA_API_URL = "https://data-api.polymarket.com"
USDC_POLYGON = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
CTF_EXCHANGE = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"
NEG_RISK_CTF_EXCHANGE = "0xC5d563A36AE78145C45a50134d48A1215220f80a"

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


async def get_redeemable_positions(wallet_address: str) -> list[dict]:
    """Fetch positions that are resolved and redeemable."""
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(
                f"{DATA_API_URL}/positions",
                params={"user": wallet_address},
            )
            resp.raise_for_status()
            positions = resp.json()

        return [
            p for p in positions
            if p.get("redeemable") and float(p.get("size", 0)) > 0
        ]
    except Exception as e:
        log.error("Failed to fetch redeemable positions: %s", e)
        return []


async def redeem_position(position: dict) -> dict:
    """Redeem a single resolved position using web3."""
    from web3 import Web3

    try:
        condition_id = position.get("conditionId")
        if not condition_id:
            return {"error": "No conditionId in position"}

        neg_risk = position.get("negRisk", False)
        exchange_addr = NEG_RISK_CTF_EXCHANGE if neg_risk else CTF_EXCHANGE

        w3 = Web3(Web3.HTTPProvider(settings.POLYGON_RPC_URL))
        wallet = Web3.to_checksum_address(settings.POLY_FUNDER_ADDRESS)
        private_key = settings.POLY_PRIVATE_KEY

        exchange = w3.eth.contract(
            address=Web3.to_checksum_address(exchange_addr),
            abi=REDEEM_ABI,
        )

        nonce = w3.eth.get_transaction_count(wallet)
        gas_price = w3.eth.gas_price

        tx = exchange.functions.redeemPositions(
            Web3.to_checksum_address(USDC_POLYGON),
            b"\x00" * 32,  # parentCollectionId = zero
            bytes.fromhex(condition_id.lstrip("0x")),
            [1, 2],  # YES and NO index sets
        ).build_transaction({
            "from": wallet,
            "nonce": nonce,
            "gas": 200000,
            "gasPrice": int(gas_price * 1.1),
            "chainId": 137,
        })

        signed = w3.eth.account.sign_transaction(tx, private_key)
        tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
        receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)

        if receipt["status"] == 1:
            value = float(position.get("currentValue", 0))
            log.info(
                "Redeemed position conditionId=%s tx=%s value=$%.2f",
                condition_id[:16], tx_hash.hex()[:16], value,
            )
            return {"success": True, "tx_hash": tx_hash.hex(), "value": value}
        else:
            return {"error": "Transaction failed", "tx_hash": tx_hash.hex()}

    except Exception as e:
        log.error("Redemption failed for conditionId=%s: %s", position.get("conditionId", "?"), e)
        return {"error": str(e)}


async def check_and_redeem() -> dict:
    """Check for redeemable positions and redeem them. Safe to call periodically."""
    if not settings.credentials_valid():
        return {"skipped": True, "reason": "no_credentials"}

    try:
        positions = await get_redeemable_positions(settings.POLY_FUNDER_ADDRESS)
        if not positions:
            return {"redeemed": 0, "failed": 0, "total_value": 0.0}

        log.info("Found %d redeemable position(s)", len(positions))

        redeemed = 0
        failed = 0
        total_value = 0.0

        for position in positions:
            result = await redeem_position(position)
            if result.get("success"):
                redeemed += 1
                total_value += result.get("value", 0.0)
            else:
                failed += 1
                log.error("Failed to redeem position: %s", result.get("error"))

        return {"redeemed": redeemed, "failed": failed, "total_value": total_value}

    except Exception as e:
        log.error("check_and_redeem error: %s", e)
        return {"error": str(e)}
