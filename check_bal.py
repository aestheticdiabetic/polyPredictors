import sys
sys.path.insert(0, '/app')
from backend.polymarket_client import PolymarketClient
from py_clob_client.clob_types import AssetType, BalanceAllowanceParams
token = '2271039595838938'
client = PolymarketClient()
clob = client._get_clob_client()
bal_resp = clob.get_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=token, signature_type=2))
print('resp:', bal_resp)
print('balance shares:', int(bal_resp.get('balance', 0)) / 1_000_000)
print('allowance shares:', int(bal_resp.get('allowance', 0)) / 1_000_000)
