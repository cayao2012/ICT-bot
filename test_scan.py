"""Quick test to time scan() and verify virtual bar works."""
import sys, os, time, logging
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "tsxapi4py", "src"))
os.environ["PYTHONPATH"] = os.path.join(os.path.dirname(__file__), "tsxapi4py", "src")

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(name)-6s | %(message)s', datefmt='%H:%M:%S')
logging.getLogger('tsxapipy').setLevel(logging.WARNING)
logging.getLogger('signalrcore').setLevel(logging.WARNING)
logging.getLogger('SignalRCoreClient').setLevel(logging.ERROR)

import ptnut_bot
from tsxapipy import authenticate, APIClient

token, tt = authenticate()
api = APIClient(initial_token=token, token_acquired_at=tt)
ptnut_bot.NQ_CONTRACT = "CON.F.US.ENQ.M26"
ptnut_bot.NQ["contract_id"] = "CON.F.US.ENQ.M26"

scanner = ptnut_bot.V106Scanner(api, "CON.F.US.ENQ.M26")
scanner.initial_load()

print(f"\nBars: 5m={len(scanner._b5_cache)} 1m={len(scanner._base_1m)} 15m={len(scanner._b15_cache)}")
print(f"Virtual bar: {scanner._build_virtual_5m()}")

t0 = time.time()
sigs = scanner.scan(is_5m_boundary=False)
print(f"\nNon-5m scan: {time.time()-t0:.2f}s | {len(sigs)} signals")
for s in sigs:
    print(f"  {s['side']} {s['zone_type']} @ {s['entry']:.2f} risk={s['risk']:.1f} sc={s['score']}")

t0 = time.time()
sigs = scanner.scan(is_5m_boundary=True)
print(f"\n5m scan: {time.time()-t0:.2f}s | {len(sigs)} signals")
for s in sigs:
    print(f"  {s['side']} {s['zone_type']} @ {s['entry']:.2f} risk={s['risk']:.1f} sc={s['score']}")
